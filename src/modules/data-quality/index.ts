import { env } from "../../config/env.js";
import { query, withTransaction } from "../../db/pool.js";
import { logError, logInfo, logWarning } from "../../observability/index.js";

type DataQualityMetricRow = {
	metric_date: string;
	visits: string | number;
	orders: string | number;
	revenue: string | number;
	spend: string | number;
};

type PersistedCheckRow = {
	run_date: string;
	check_key: string;
	status: "healthy" | "warning" | "failed";
	severity: "info" | "warning" | "critical";
	discrepancy_count: number;
	summary: string;
	details: Record<string, unknown>;
	checked_at: Date;
	alert_emitted_at: Date | null;
};

type DataQualityCheckResult = {
	checkKey: string;
	status: PersistedCheckRow["status"];
	severity: PersistedCheckRow["severity"];
	discrepancyCount: number;
	summary: string;
	details: Record<string, unknown>;
	threshold: number;
	alertTriggered: boolean;
};

type AnomalyFlag = {
	metric: "visits" | "orders" | "revenue" | "spend";
	currentValue: number;
	baselineValue: number;
	absoluteDelta: number;
	relativeDelta: number | null;
};

type DuplicateAssignmentRow = {
	entity_key: string;
	canonical_count: number;
	journey_ids: string[];
	source_tables: string[];
};

type ShopifyConflictRow = {
	shopify_customer_id: string;
	canonical_count: number;
	journey_ids: string[];
	source_tables: string[];
};

type HashAnomalyRow = {
	source_name: string;
	field_name: string;
	invalid_count: number;
	sample_values: string[];
};

type RateCheckRow = {
	total_count: string | number;
	discrepancy_count: string | number;
};

type DataQualityCheckSeverity = Extract<
	PersistedCheckRow["severity"],
	"warning" | "critical"
>;

const HASH_FORMAT_REGEX = "^[0-9a-f]{64}$";

function toDateString(value: Date): string {
	return value.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
	const next = new Date(date);
	next.setUTCDate(next.getUTCDate() + days);
	return next;
}

function toRunDateEnd(runDate: string): string {
	return `${toDateString(addUtcDays(new Date(`${runDate}T00:00:00.000Z`), 1))}T00:00:00.000Z`;
}

function resolveReadinessWindow(runDate: string): {
	startDate: string;
	endDate: string;
	startAt: string;
	endAt: string;
	windowDays: number;
} {
	const windowDays = Math.max(1, env.MMM_READINESS_WINDOW_DAYS);
	const end = new Date(`${runDate}T00:00:00.000Z`);
	const startDate = toDateString(addUtcDays(end, -(windowDays - 1)));
	const endDate = runDate;

	return {
		startDate,
		endDate,
		startAt: `${startDate}T00:00:00.000Z`,
		endAt: toRunDateEnd(runDate),
		windowDays,
	};
}

function toNumber(value: string | number): number {
	return typeof value === "number" ? value : Number(value);
}

function pluralize(label: string, count: number): string {
	return count === 1 ? label : `${label}s`;
}

function evaluateDiscrepancyCount(input: {
	discrepancyCount: number;
	threshold: number;
	severityOnAlert: DataQualityCheckSeverity;
	healthySummary: string;
	warningSummary: string;
	alertSummary: string;
	details: Record<string, unknown>;
}): DataQualityCheckResult {
	const discrepancyCount = Math.max(0, input.discrepancyCount);
	const threshold = Math.max(0, input.threshold);
	const alertTriggered = discrepancyCount > threshold;

	if (discrepancyCount === 0) {
		return {
			checkKey: "",
			status: "healthy",
			severity: "info",
			discrepancyCount,
			summary: input.healthySummary,
			details: input.details,
			threshold,
			alertTriggered: false,
		};
	}

	if (alertTriggered) {
		return {
			checkKey: "",
			status: input.severityOnAlert === "critical" ? "failed" : "warning",
			severity: input.severityOnAlert,
			discrepancyCount,
			summary: input.alertSummary,
			details: input.details,
			threshold,
			alertTriggered: true,
		};
	}

	return {
		checkKey: "",
		status: "warning",
		severity: "warning",
		discrepancyCount,
		summary: input.warningSummary,
		details: input.details,
		threshold,
		alertTriggered: false,
	};
}

function evaluateMaximumRate(input: {
	discrepancyCount: number;
	totalCount: number;
	maxRate: number;
	severityOnAlert: DataQualityCheckSeverity;
	healthySummary: string;
	warningSummary: string;
	alertSummary: string;
	details: Record<string, unknown>;
}): DataQualityCheckResult {
	const discrepancyCount = Math.max(0, input.discrepancyCount);
	const totalCount = Math.max(0, input.totalCount);
	const maxRate = Math.max(0, input.maxRate);
	const observedRate = totalCount > 0 ? discrepancyCount / totalCount : 0;
	const alertTriggered = observedRate > maxRate;

	if (discrepancyCount === 0 || totalCount === 0) {
		return {
			checkKey: "",
			status: "healthy",
			severity: "info",
			discrepancyCount,
			summary: input.healthySummary,
			details: {
				...input.details,
				totalCount,
				observedRate,
			},
			threshold: maxRate,
			alertTriggered: false,
		};
	}

	if (alertTriggered) {
		return {
			checkKey: "",
			status: input.severityOnAlert === "critical" ? "failed" : "warning",
			severity: input.severityOnAlert,
			discrepancyCount,
			summary: input.alertSummary,
			details: {
				...input.details,
				totalCount,
				observedRate,
			},
			threshold: maxRate,
			alertTriggered: true,
		};
	}

	return {
		checkKey: "",
		status: "warning",
		severity: "warning",
		discrepancyCount,
		summary: input.warningSummary,
		details: {
			...input.details,
			totalCount,
			observedRate,
		},
		threshold: maxRate,
		alertTriggered: false,
	};
}

async function buildReportingAnomalyCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const lookbackDates = buildLookbackDates(
		runDate,
		env.DATA_QUALITY_ANOMALY_LOOKBACK_DAYS + 1,
	);
	const metricsResult = await query<DataQualityMetricRow>(
		`
      SELECT
        metric_date::text,
        COALESCE(SUM(visits), 0)::text AS visits,
        COALESCE(SUM(attributed_orders), 0)::text AS orders,
        COALESCE(SUM(attributed_revenue), 0)::text AS revenue,
        COALESCE(SUM(spend), 0)::text AS spend
      FROM daily_reporting_metrics
      WHERE metric_date = ANY($1::date[])
      GROUP BY metric_date
      ORDER BY metric_date ASC
    `,
		[lookbackDates],
	);
	const anomalyFlags = detectAnomalyFlags(metricsResult.rows, runDate);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount: anomalyFlags.length,
		threshold: env.DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD,
		severityOnAlert: "warning",
		healthySummary: "No reporting anomalies detected for the run date.",
		warningSummary: `${anomalyFlags.length} reporting ${pluralize("metric", anomalyFlags.length)} deviated from the trailing baseline but remained within the configured alert threshold.`,
		alertSummary: `${anomalyFlags.length} reporting ${pluralize("metric", anomalyFlags.length)} exceeded the trailing-baseline alert threshold.`,
		details: {
			anomalyFlags,
			lookbackDates,
		},
	});

	return {
		...evaluated,
		checkKey: "reporting_anomaly_check",
	};
}

async function buildOrphanSessionCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const result = await query<{
		discrepancy_count: string;
		sample_session_ids: string[];
	}>(
		`
      WITH orphan_sessions AS (
        SELECT
          s.id::text AS session_id,
          GREATEST(
            COALESCE(s.last_seen_at, s.first_seen_at),
            COALESCE((
              SELECT MAX(e.occurred_at)
              FROM tracking_events e
              WHERE e.session_id = s.id
            ), '-infinity'::timestamptz),
            COALESCE((
              SELECT MAX(sai.last_captured_at)
              FROM session_attribution_identities sai
              WHERE sai.roas_radar_session_id = s.id
            ), '-infinity'::timestamptz),
            COALESCE((
              SELECT MAX(o.ingested_at)
              FROM shopify_orders o
              WHERE o.landing_session_id = s.id
            ), '-infinity'::timestamptz)
          ) AS last_observed_at
        FROM tracking_sessions s
        WHERE s.identity_journey_id IS NULL
          AND s.first_seen_at < $1::timestamptz
          AND (
            EXISTS (
              SELECT 1
              FROM tracking_events e
              WHERE e.session_id = s.id
            )
            OR EXISTS (
              SELECT 1
              FROM session_attribution_identities sai
              WHERE sai.roas_radar_session_id = s.id
            )
            OR EXISTS (
              SELECT 1
              FROM shopify_orders o
              WHERE o.landing_session_id = s.id
            )
          )
      ),
      sampled AS (
        SELECT session_id
        FROM orphan_sessions
        ORDER BY last_observed_at DESC, session_id ASC
        LIMIT $2::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM orphan_sessions) AS discrepancy_count,
        COALESCE((SELECT array_agg(session_id ORDER BY session_id ASC) FROM sampled), ARRAY[]::text[]) AS sample_session_ids
    `,
		[toRunDateEnd(runDate), env.DATA_QUALITY_SAMPLE_LIMIT],
	);

	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: env.DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD,
		severityOnAlert: "critical",
		healthySummary:
			"No orphan sessions were detected in the identity graph snapshot.",
		warningSummary: `${discrepancyCount} orphan ${pluralize("session", discrepancyCount)} remain unresolved but did not breach the configured alert threshold.`,
		alertSummary: `${discrepancyCount} orphan ${pluralize("session", discrepancyCount)} breached the configured alert threshold.`,
		details: {
			sampleSessionIds: row?.sample_session_ids ?? [],
		},
	});

	return {
		...evaluated,
		checkKey: "identity_graph_orphan_sessions",
	};
}

async function buildDuplicateCanonicalAssignmentCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const result = await query<{
		discrepancy_count: string;
		samples: DuplicateAssignmentRow[];
	}>(
		`
      WITH session_assignments AS (
        SELECT
          s.id::text AS entity_key,
          s.identity_journey_id::text AS journey_id,
          'tracking_sessions'::text AS source_table
        FROM tracking_sessions s
        WHERE s.identity_journey_id IS NOT NULL
          AND s.first_seen_at < $1::timestamptz

        UNION ALL

        SELECT
          e.session_id::text AS entity_key,
          e.identity_journey_id::text AS journey_id,
          'tracking_events'::text AS source_table
        FROM tracking_events e
        WHERE e.identity_journey_id IS NOT NULL
          AND e.occurred_at < $1::timestamptz

        UNION ALL

        SELECT
          sai.roas_radar_session_id::text AS entity_key,
          sai.identity_journey_id::text AS journey_id,
          'session_attribution_identities'::text AS source_table
        FROM session_attribution_identities sai
        WHERE sai.identity_journey_id IS NOT NULL
          AND sai.first_captured_at < $1::timestamptz

        UNION ALL

        SELECT
          cj.session_id::text AS entity_key,
          cj.identity_journey_id::text AS journey_id,
          'customer_journey'::text AS source_table
        FROM customer_journey cj
        WHERE cj.refreshed_at < $1::timestamptz
      ),
      conflicts AS (
        SELECT
          entity_key,
          COUNT(DISTINCT journey_id)::int AS canonical_count,
          ARRAY_AGG(DISTINCT journey_id ORDER BY journey_id) AS journey_ids,
          ARRAY_AGG(DISTINCT source_table ORDER BY source_table) AS source_tables
        FROM session_assignments
        GROUP BY entity_key
        HAVING COUNT(DISTINCT journey_id) > 1
      ),
      sampled AS (
        SELECT *
        FROM conflicts
        ORDER BY canonical_count DESC, entity_key ASC
        LIMIT $2::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM conflicts) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'entity_key', entity_key,
                'canonical_count', canonical_count,
                'journey_ids', journey_ids,
                'source_tables', source_tables
              )
              ORDER BY canonical_count DESC, entity_key ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[toRunDateEnd(runDate), env.DATA_QUALITY_SAMPLE_LIMIT],
	);

	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: env.DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD,
		severityOnAlert: "critical",
		healthySummary: "No duplicate canonical session assignments were detected.",
		warningSummary: `${discrepancyCount} session ${pluralize("assignment", discrepancyCount)} disagreed across canonical surfaces but remained within the configured alert threshold.`,
		alertSummary: `${discrepancyCount} session ${pluralize("assignment", discrepancyCount)} disagreed across canonical surfaces and breached the alert threshold.`,
		details: {
			sampleConflicts: row?.samples ?? [],
		},
	});

	return {
		...evaluated,
		checkKey: "identity_graph_duplicate_canonical_assignments",
	};
}

async function buildConflictingShopifyMappingCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const result = await query<{
		discrepancy_count: string;
		samples: ShopifyConflictRow[];
	}>(
		`
      WITH shopify_assignments AS (
        SELECT
          j.authoritative_shopify_customer_id AS shopify_customer_id,
          j.id::text AS journey_id,
          'identity_journeys'::text AS source_table
        FROM identity_journeys j
        WHERE j.authoritative_shopify_customer_id IS NOT NULL
          AND j.created_at < $1::timestamptz

        UNION ALL

        SELECT
          sc.shopify_customer_id,
          sc.identity_journey_id::text AS journey_id,
          'shopify_customers'::text AS source_table
        FROM shopify_customers sc
        WHERE sc.shopify_customer_id IS NOT NULL
          AND sc.identity_journey_id IS NOT NULL
          AND sc.created_at < $1::timestamptz

        UNION ALL

        SELECT
          so.shopify_customer_id,
          so.identity_journey_id::text AS journey_id,
          'shopify_orders'::text AS source_table
        FROM shopify_orders so
        WHERE so.shopify_customer_id IS NOT NULL
          AND so.identity_journey_id IS NOT NULL
          AND so.ingested_at < $1::timestamptz
      ),
      conflicts AS (
        SELECT
          shopify_customer_id,
          COUNT(DISTINCT journey_id)::int AS canonical_count,
          ARRAY_AGG(DISTINCT journey_id ORDER BY journey_id) AS journey_ids,
          ARRAY_AGG(DISTINCT source_table ORDER BY source_table) AS source_tables
        FROM shopify_assignments
        GROUP BY shopify_customer_id
        HAVING COUNT(DISTINCT journey_id) > 1
      ),
      sampled AS (
        SELECT *
        FROM conflicts
        ORDER BY canonical_count DESC, shopify_customer_id ASC
        LIMIT $2::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM conflicts) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'shopify_customer_id', shopify_customer_id,
                'canonical_count', canonical_count,
                'journey_ids', journey_ids,
                'source_tables', source_tables
              )
              ORDER BY canonical_count DESC, shopify_customer_id ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[toRunDateEnd(runDate), env.DATA_QUALITY_SAMPLE_LIMIT],
	);

	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: env.DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD,
		severityOnAlert: "critical",
		healthySummary: "No conflicting Shopify customer mappings were detected.",
		warningSummary: `${discrepancyCount} Shopify customer ${pluralize("mapping", discrepancyCount)} disagreed across canonical surfaces but remained within the configured alert threshold.`,
		alertSummary: `${discrepancyCount} Shopify customer ${pluralize("mapping", discrepancyCount)} disagreed across canonical surfaces and breached the alert threshold.`,
		details: {
			sampleConflicts: row?.samples ?? [],
		},
	});

	return {
		...evaluated,
		checkKey: "identity_graph_conflicting_shopify_mappings",
	};
}

async function buildHashFormatAnomalyCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const result = await query<{
		discrepancy_count: string;
		samples: HashAnomalyRow[];
	}>(
		`
      WITH anomalies AS (
        SELECT
          'identity_nodes'::text AS source_name,
          node_type AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.node_key
            FROM identity_nodes sample
            WHERE sample.node_type = n.node_type
              AND sample.created_at < $1::timestamptz
              AND sample.node_key !~ $2
            ORDER BY sample.node_key ASC
            LIMIT 3
          ) AS sample_values
        FROM identity_nodes n
        WHERE n.node_type IN ('hashed_email', 'phone_hash')
          AND n.created_at < $1::timestamptz
          AND n.node_key !~ $2
        GROUP BY node_type

        UNION ALL

        SELECT
          'identity_journeys'::text AS source_name,
          'primary_email_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.primary_email_hash
            FROM identity_journeys sample
            WHERE sample.created_at < $1::timestamptz
              AND sample.primary_email_hash IS NOT NULL
              AND sample.primary_email_hash !~ $2
            ORDER BY sample.primary_email_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM identity_journeys j
        WHERE j.created_at < $1::timestamptz
          AND j.primary_email_hash IS NOT NULL
          AND j.primary_email_hash !~ $2

        UNION ALL

        SELECT
          'identity_journeys'::text AS source_name,
          'primary_phone_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.primary_phone_hash
            FROM identity_journeys sample
            WHERE sample.created_at < $1::timestamptz
              AND sample.primary_phone_hash IS NOT NULL
              AND sample.primary_phone_hash !~ $2
            ORDER BY sample.primary_phone_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM identity_journeys j
        WHERE j.created_at < $1::timestamptz
          AND j.primary_phone_hash IS NOT NULL
          AND j.primary_phone_hash !~ $2

        UNION ALL

        SELECT
          'shopify_customers'::text AS source_name,
          'email_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.email_hash
            FROM shopify_customers sample
            WHERE sample.created_at < $1::timestamptz
              AND sample.email_hash IS NOT NULL
              AND sample.email_hash !~ $2
            ORDER BY sample.email_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM shopify_customers sc
        WHERE sc.created_at < $1::timestamptz
          AND sc.email_hash IS NOT NULL
          AND sc.email_hash !~ $2

        UNION ALL

        SELECT
          'shopify_customers'::text AS source_name,
          'phone_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.phone_hash
            FROM shopify_customers sample
            WHERE sample.created_at < $1::timestamptz
              AND sample.phone_hash IS NOT NULL
              AND sample.phone_hash !~ $2
            ORDER BY sample.phone_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM shopify_customers sc
        WHERE sc.created_at < $1::timestamptz
          AND sc.phone_hash IS NOT NULL
          AND sc.phone_hash !~ $2

        UNION ALL

        SELECT
          'shopify_orders'::text AS source_name,
          'email_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.email_hash
            FROM shopify_orders sample
            WHERE sample.ingested_at < $1::timestamptz
              AND sample.email_hash IS NOT NULL
              AND sample.email_hash !~ $2
            ORDER BY sample.email_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM shopify_orders so
        WHERE so.ingested_at < $1::timestamptz
          AND so.email_hash IS NOT NULL
          AND so.email_hash !~ $2

        UNION ALL

        SELECT
          'shopify_orders'::text AS source_name,
          'phone_hash'::text AS field_name,
          COUNT(*)::int AS invalid_count,
          ARRAY(
            SELECT DISTINCT sample.phone_hash
            FROM shopify_orders sample
            WHERE sample.ingested_at < $1::timestamptz
              AND sample.phone_hash IS NOT NULL
              AND sample.phone_hash !~ $2
            ORDER BY sample.phone_hash ASC
            LIMIT 3
          ) AS sample_values
        FROM shopify_orders so
        WHERE so.ingested_at < $1::timestamptz
          AND so.phone_hash IS NOT NULL
          AND so.phone_hash !~ $2
      ),
      filtered AS (
        SELECT *
        FROM anomalies
        WHERE invalid_count > 0
      ),
      sampled AS (
        SELECT *
        FROM filtered
        ORDER BY invalid_count DESC, source_name ASC, field_name ASC
        LIMIT $3::int
      )
      SELECT
        COALESCE((SELECT SUM(invalid_count)::text FROM filtered), '0') AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'source_name', source_name,
                'field_name', field_name,
                'invalid_count', invalid_count,
                'sample_values', sample_values
              )
              ORDER BY invalid_count DESC, source_name ASC, field_name ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[toRunDateEnd(runDate), HASH_FORMAT_REGEX, env.DATA_QUALITY_SAMPLE_LIMIT],
	);

	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: env.DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD,
		severityOnAlert: "warning",
		healthySummary:
			"No hash-format anomalies were detected across identity and Shopify surfaces.",
		warningSummary: `${discrepancyCount} hash-format ${pluralize("anomaly", discrepancyCount)} were detected but remained within the configured alert threshold.`,
		alertSummary: `${discrepancyCount} hash-format ${pluralize("anomaly", discrepancyCount)} breached the configured alert threshold.`,
		details: {
			samples: row?.samples ?? [],
			expectedPattern: HASH_FORMAT_REGEX,
		},
	});

	return {
		...evaluated,
		checkKey: "identity_graph_hash_format_anomalies",
	};
}

async function buildMmmCaptureCompletenessCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<
		RateCheckRow & {
			sample_session_ids: string[];
		}
	>(
		`
      WITH sessions AS (
        SELECT s.id
        FROM tracking_sessions s
        WHERE s.first_seen_at >= $1::timestamptz
          AND s.first_seen_at < $2::timestamptz
      ),
      missing AS (
        SELECT sessions.id::text AS session_id
        FROM sessions
        LEFT JOIN session_attribution_identities sai
          ON sai.roas_radar_session_id = sessions.id
        WHERE sai.roas_radar_session_id IS NULL
      ),
      sampled AS (
        SELECT session_id
        FROM missing
        ORDER BY session_id ASC
        LIMIT $3::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM sessions) AS total_count,
        (SELECT COUNT(*)::text FROM missing) AS discrepancy_count,
        COALESCE((SELECT array_agg(session_id ORDER BY session_id ASC) FROM sampled), ARRAY[]::text[]) AS sample_session_ids
    `,
		[window.startAt, window.endAt, env.DATA_QUALITY_SAMPLE_LIMIT],
	);
	const row = result.rows[0];
	const totalCount = toNumber(row?.total_count ?? 0);
	const discrepancyCount = toNumber(row?.discrepancy_count ?? 0);
	const minRate = env.MMM_READINESS_CAPTURE_COMPLETENESS_MIN_RATE;
	const evaluated = evaluateMaximumRate({
		discrepancyCount,
		totalCount,
		maxRate: 1 - minRate,
		severityOnAlert: "critical",
		healthySummary: "MMM capture completeness passed for the readiness window.",
		warningSummary: `${discrepancyCount} captured ${pluralize("session", discrepancyCount)} lacked a session-attribution identity row but remained within the MMM readiness threshold.`,
		alertSummary: `MMM capture completeness fell below ${(minRate * 100).toFixed(2)}% for the readiness window.`,
		details: {
			window,
			minCompletenessRate: minRate,
			sampleSessionIds: row?.sample_session_ids ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_capture_completeness" };
}

async function buildMmmMissingSessionIdRateCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<
		RateCheckRow & {
			sample_order_ids: string[];
		}
	>(
		`
      WITH orders AS (
        SELECT shopify_order_id, landing_session_id
        FROM shopify_orders
        WHERE COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz
          AND COALESCE(processed_at, created_at_shopify, ingested_at) < $2::timestamptz
          AND COALESCE(source_name, '') IN ('web', 'shopify_draft_order', '')
      ),
      missing AS (
        SELECT shopify_order_id
        FROM orders
        WHERE landing_session_id IS NULL
      ),
      sampled AS (
        SELECT shopify_order_id
        FROM missing
        ORDER BY shopify_order_id ASC
        LIMIT $3::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM orders) AS total_count,
        (SELECT COUNT(*)::text FROM missing) AS discrepancy_count,
        COALESCE((SELECT array_agg(shopify_order_id ORDER BY shopify_order_id ASC) FROM sampled), ARRAY[]::text[]) AS sample_order_ids
    `,
		[window.startAt, window.endAt, env.DATA_QUALITY_SAMPLE_LIMIT],
	);
	const row = result.rows[0];
	const discrepancyCount = toNumber(row?.discrepancy_count ?? 0);
	const evaluated = evaluateMaximumRate({
		discrepancyCount,
		totalCount: toNumber(row?.total_count ?? 0),
		maxRate: env.MMM_READINESS_MISSING_SESSION_ID_MAX_RATE,
		severityOnAlert: "critical",
		healthySummary: "MMM missing session-id rate passed for Shopify web orders.",
		warningSummary: `${discrepancyCount} Shopify web ${pluralize("order", discrepancyCount)} lacked landing session ids but remained within the MMM readiness threshold.`,
		alertSummary: "MMM missing session-id rate breached the readiness threshold.",
		details: {
			window,
			sampleOrderIds: row?.sample_order_ids ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_missing_session_id_rate" };
}

async function buildMmmDualWriteMismatchCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<
		RateCheckRow & {
			sample_session_ids: string[];
		}
	>(
		`
      WITH paired AS (
        SELECT s.id
        FROM tracking_sessions s
        JOIN session_attribution_identities sai
          ON sai.roas_radar_session_id = s.id
        WHERE s.first_seen_at >= $1::timestamptz
          AND s.first_seen_at < $2::timestamptz
      ),
      mismatched AS (
        SELECT s.id::text AS session_id
        FROM tracking_sessions s
        JOIN session_attribution_identities sai
          ON sai.roas_radar_session_id = s.id
        WHERE s.first_seen_at >= $1::timestamptz
          AND s.first_seen_at < $2::timestamptz
          AND (
            s.landing_page IS DISTINCT FROM sai.landing_url
            OR s.referrer_url IS DISTINCT FROM sai.referrer_url
            OR s.initial_utm_source IS DISTINCT FROM sai.initial_utm_source
            OR s.initial_utm_medium IS DISTINCT FROM sai.initial_utm_medium
            OR s.initial_utm_campaign IS DISTINCT FROM sai.initial_utm_campaign
            OR s.initial_utm_content IS DISTINCT FROM sai.initial_utm_content
            OR s.initial_utm_term IS DISTINCT FROM sai.initial_utm_term
            OR s.initial_gclid IS DISTINCT FROM sai.initial_gclid
            OR s.initial_gbraid IS DISTINCT FROM sai.initial_gbraid
            OR s.initial_wbraid IS DISTINCT FROM sai.initial_wbraid
            OR s.initial_fbclid IS DISTINCT FROM sai.initial_fbclid
            OR s.initial_ttclid IS DISTINCT FROM sai.initial_ttclid
            OR s.initial_msclkid IS DISTINCT FROM sai.initial_msclkid
          )
      ),
      sampled AS (
        SELECT session_id
        FROM mismatched
        ORDER BY session_id ASC
        LIMIT $3::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM paired) AS total_count,
        (SELECT COUNT(*)::text FROM mismatched) AS discrepancy_count,
        COALESCE((SELECT array_agg(session_id ORDER BY session_id ASC) FROM sampled), ARRAY[]::text[]) AS sample_session_ids
    `,
		[window.startAt, window.endAt, env.DATA_QUALITY_SAMPLE_LIMIT],
	);
	const row = result.rows[0];
	const discrepancyCount = toNumber(row?.discrepancy_count ?? 0);
	const evaluated = evaluateMaximumRate({
		discrepancyCount,
		totalCount: toNumber(row?.total_count ?? 0),
		maxRate: env.MMM_READINESS_DUAL_WRITE_MISMATCH_MAX_RATE,
		severityOnAlert: "critical",
		healthySummary: "MMM dual-write parity passed for session attribution capture.",
		warningSummary: `${discrepancyCount} dual-write ${pluralize("mismatch", discrepancyCount)} remained within the MMM readiness threshold.`,
		alertSummary: "MMM dual-write mismatch rate breached the readiness threshold.",
		details: {
			window,
			sampleSessionIds: row?.sample_session_ids ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_dual_write_mismatch" };
}

async function buildMmmResolverUnattributedRateCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<
		RateCheckRow & {
			sample_order_ids: string[];
		}
	>(
		`
      WITH resolved_orders AS (
        SELECT ar.shopify_order_id, ar.match_source, ar.attribution_reason
        FROM attribution_results ar
        JOIN shopify_orders so
          ON so.shopify_order_id = ar.shopify_order_id
        WHERE COALESCE(so.processed_at, so.created_at_shopify, so.ingested_at) >= $1::timestamptz
          AND COALESCE(so.processed_at, so.created_at_shopify, so.ingested_at) < $2::timestamptz
      ),
      unattributed AS (
        SELECT shopify_order_id
        FROM resolved_orders
        WHERE match_source = 'unattributed'
           OR attribution_reason = 'unattributed'
      ),
      sampled AS (
        SELECT shopify_order_id
        FROM unattributed
        ORDER BY shopify_order_id ASC
        LIMIT $3::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM resolved_orders) AS total_count,
        (SELECT COUNT(*)::text FROM unattributed) AS discrepancy_count,
        COALESCE((SELECT array_agg(shopify_order_id ORDER BY shopify_order_id ASC) FROM sampled), ARRAY[]::text[]) AS sample_order_ids
    `,
		[window.startAt, window.endAt, env.DATA_QUALITY_SAMPLE_LIMIT],
	);
	const row = result.rows[0];
	const discrepancyCount = toNumber(row?.discrepancy_count ?? 0);
	const evaluated = evaluateMaximumRate({
		discrepancyCount,
		totalCount: toNumber(row?.total_count ?? 0),
		maxRate: env.MMM_READINESS_RESOLVER_UNATTRIBUTED_MAX_RATE,
		severityOnAlert: "critical",
		healthySummary: "MMM resolver unattributed rate passed for the readiness window.",
		warningSummary: `${discrepancyCount} resolved ${pluralize("order", discrepancyCount)} fell through to unattributed but remained within the MMM readiness threshold.`,
		alertSummary: "MMM resolver unattributed rate breached the readiness threshold.",
		details: {
			window,
			sampleOrderIds: row?.sample_order_ids ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_resolver_unattributed_rate" };
}

async function buildMmmSpendFreshnessCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<{
		platform: string;
		latest_report_date: string | null;
		latest_synced_at: Date | null;
		is_stale: boolean;
	}>(
		`
      WITH active_platforms AS (
        SELECT 'meta_ads'::text AS platform
        WHERE EXISTS (SELECT 1 FROM meta_ads_connections WHERE status = 'active')
        UNION ALL
        SELECT 'google_ads'::text AS platform
        WHERE EXISTS (SELECT 1 FROM google_ads_connections WHERE status = 'active')
      ),
      spend AS (
        SELECT 'meta_ads'::text AS platform, MAX(report_date)::text AS latest_report_date, MAX(updated_at) AS latest_synced_at
        FROM meta_ads_daily_spend
        WHERE report_date <= $1::date
        UNION ALL
        SELECT 'google_ads'::text AS platform, MAX(report_date)::text AS latest_report_date, MAX(updated_at) AS latest_synced_at
        FROM google_ads_daily_spend
        WHERE report_date <= $1::date
      )
      SELECT
        active_platforms.platform,
        spend.latest_report_date,
        spend.latest_synced_at,
        (
          spend.latest_report_date IS NULL
          OR spend.latest_report_date::date < $1::date
          OR spend.latest_synced_at < now() - ($2::int * interval '1 hour')
        ) AS is_stale
      FROM active_platforms
      LEFT JOIN spend
        ON spend.platform = active_platforms.platform
      ORDER BY active_platforms.platform ASC
    `,
		[window.endDate, env.MMM_READINESS_SPEND_FRESHNESS_MAX_LAG_HOURS],
	);
	const stalePlatforms = result.rows.filter((row) => row.is_stale);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount: stalePlatforms.length,
		threshold: 0,
		severityOnAlert: "critical",
		healthySummary: "MMM spend freshness passed for active ad platforms.",
		warningSummary: "MMM spend freshness had stale platforms within threshold.",
		alertSummary: `${stalePlatforms.length} active ad ${pluralize("platform", stalePlatforms.length)} breached spend freshness readiness.`,
		details: {
			window,
			maxLagHours: env.MMM_READINESS_SPEND_FRESHNESS_MAX_LAG_HOURS,
			platforms: result.rows.map((row) => ({
				platform: row.platform,
				latestReportDate: row.latest_report_date,
				latestSyncedAt: row.latest_synced_at?.toISOString() ?? null,
				isStale: row.is_stale,
			})),
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_spend_freshness" };
}

async function buildMmmCampaignMetadataFreshnessCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<{
		discrepancy_count: string;
		samples: Array<{
			platform: string;
			account_id: string;
			entity_id: string;
			latest_name: string;
			last_seen_at: string;
		}>;
	}>(
		`
      WITH stale AS (
        SELECT platform, account_id, entity_id, latest_name, last_seen_at
        FROM ad_platform_entity_metadata
        WHERE entity_type = 'campaign'
          AND last_seen_at < now() - ($1::int * interval '1 hour')
      ),
      sampled AS (
        SELECT *
        FROM stale
        ORDER BY last_seen_at ASC, platform ASC, entity_id ASC
        LIMIT $2::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM stale) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'platform', platform,
                'account_id', account_id,
                'entity_id', entity_id,
                'latest_name', latest_name,
                'last_seen_at', last_seen_at
              )
              ORDER BY last_seen_at ASC, platform ASC, entity_id ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[
			env.MMM_READINESS_CAMPAIGN_METADATA_MAX_LAG_HOURS,
			env.DATA_QUALITY_SAMPLE_LIMIT,
		],
	);
	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: 0,
		severityOnAlert: "critical",
		healthySummary: "MMM campaign metadata freshness passed.",
		warningSummary: "MMM campaign metadata freshness had stale entities within threshold.",
		alertSummary: `${discrepancyCount} campaign metadata ${pluralize("entity", discrepancyCount)} breached freshness readiness.`,
		details: {
			window,
			maxLagHours: env.MMM_READINESS_CAMPAIGN_METADATA_MAX_LAG_HOURS,
			samples: row?.samples ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_campaign_metadata_freshness" };
}

async function buildMmmReportingAggregateFreshnessCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);
	const result = await query<{
		discrepancy_count: string;
		samples: Array<{ metric_date: string; last_computed_at: string | null }>;
	}>(
		`
      WITH days AS (
        SELECT generate_series($1::date, $2::date, interval '1 day')::date AS metric_date
      ),
      aggregate_freshness AS (
        SELECT metric_date, MAX(last_computed_at) AS last_computed_at
        FROM daily_reporting_metrics
        WHERE metric_date BETWEEN $1::date AND $2::date
        GROUP BY metric_date
      ),
      stale AS (
        SELECT days.metric_date::text, aggregate_freshness.last_computed_at
        FROM days
        LEFT JOIN aggregate_freshness
          ON aggregate_freshness.metric_date = days.metric_date
        WHERE aggregate_freshness.last_computed_at IS NULL
           OR aggregate_freshness.last_computed_at < now() - ($3::int * interval '1 hour')
      ),
      sampled AS (
        SELECT *
        FROM stale
        ORDER BY metric_date ASC
        LIMIT $4::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM stale) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'metric_date', metric_date,
                'last_computed_at', last_computed_at
              )
              ORDER BY metric_date ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[
			window.startDate,
			window.endDate,
			env.MMM_READINESS_REPORTING_AGGREGATE_MAX_LAG_HOURS,
			env.DATA_QUALITY_SAMPLE_LIMIT,
		],
	);
	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: 0,
		severityOnAlert: "critical",
		healthySummary: "MMM reporting aggregate freshness passed.",
		warningSummary: "MMM reporting aggregate freshness had stale dates within threshold.",
		alertSummary: `${discrepancyCount} reporting aggregate ${pluralize("date", discrepancyCount)} breached freshness readiness.`,
		details: {
			window,
			maxLagHours: env.MMM_READINESS_REPORTING_AGGREGATE_MAX_LAG_HOURS,
			samples: row?.samples ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_reporting_aggregate_freshness" };
}

async function buildMmmDataQualityBlockersCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const lookbackStart = toDateString(
		addUtcDays(
			new Date(`${runDate}T00:00:00.000Z`),
			-(Math.max(1, env.MMM_READINESS_DATA_QUALITY_BLOCKER_LOOKBACK_DAYS) - 1),
		),
	);
	const result = await query<{
		discrepancy_count: string;
		samples: Array<{
			run_date: string;
			check_key: string;
			status: string;
			severity: string;
			summary: string;
		}>;
	}>(
		`
      WITH blockers AS (
        SELECT run_date::text, check_key, status, severity, summary
        FROM data_quality_check_runs
        WHERE run_date BETWEEN $1::date AND $2::date
          AND check_key NOT LIKE 'mmm_readiness_%'
          AND (status = 'failed' OR severity = 'critical')
      ),
      sampled AS (
        SELECT *
        FROM blockers
        ORDER BY run_date DESC, check_key ASC
        LIMIT $3::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM blockers) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'run_date', run_date,
                'check_key', check_key,
                'status', status,
                'severity', severity,
                'summary', summary
              )
              ORDER BY run_date DESC, check_key ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[lookbackStart, runDate, env.DATA_QUALITY_SAMPLE_LIMIT],
	);
	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: 0,
		severityOnAlert: "critical",
		healthySummary: "MMM data-quality blocker gate passed.",
		warningSummary: "MMM data-quality blockers remained within threshold.",
		alertSummary: `${discrepancyCount} blocking data-quality ${pluralize("check", discrepancyCount)} must be resolved before MMM readiness approval.`,
		details: {
			lookbackStart,
			lookbackEnd: runDate,
			lookbackDays: env.MMM_READINESS_DATA_QUALITY_BLOCKER_LOOKBACK_DAYS,
			samples: row?.samples ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_data_quality_blockers" };
}

async function buildMmmGa4FallbackStatusCheck(
	runDate: string,
): Promise<DataQualityCheckResult> {
	const window = resolveReadinessWindow(runDate);

	if (!env.GA4_BIGQUERY_ENABLED) {
		const discrepancyCount = env.MMM_READINESS_GA4_FALLBACK_REQUIRED ? 1 : 0;
		const evaluated = evaluateDiscrepancyCount({
			discrepancyCount,
			threshold: 0,
			severityOnAlert: "critical",
			healthySummary: "MMM GA4 fallback status is not required and GA4 ingestion is disabled.",
			warningSummary: "MMM GA4 fallback status is disabled within threshold.",
			alertSummary:
				"MMM GA4 fallback is required for readiness but GA4 BigQuery ingestion is disabled.",
			details: {
				window,
				ga4BigQueryEnabled: false,
				required: env.MMM_READINESS_GA4_FALLBACK_REQUIRED,
			},
		});

		return { ...evaluated, checkKey: "mmm_readiness_ga4_fallback_status" };
	}

	const result = await query<{
		discrepancy_count: string;
		samples: Array<{
			hour_start: string;
			status: string | null;
			last_run_completed_at: string | null;
		}>;
	}>(
		`
      WITH hours AS (
        SELECT generate_series(
          date_trunc('hour', $1::timestamptz),
          date_trunc('hour', $2::timestamptz) - interval '1 hour',
          interval '1 hour'
        ) AS hour_start
      ),
      latest AS (
        SELECT DISTINCT ON (hour_start)
          hour_start,
          status,
          last_run_completed_at
        FROM ga4_bigquery_hourly_jobs
        WHERE hour_start >= $1::timestamptz
          AND hour_start < $2::timestamptz
        ORDER BY hour_start, updated_at DESC
      ),
      stale AS (
        SELECT
          hours.hour_start,
          latest.status,
          latest.last_run_completed_at
        FROM hours
        LEFT JOIN latest
          ON latest.hour_start = hours.hour_start
        WHERE latest.status IS NULL
           OR latest.status <> 'completed'
           OR latest.last_run_completed_at < now() - ($3::int * interval '1 hour')
      ),
      sampled AS (
        SELECT *
        FROM stale
        ORDER BY hour_start ASC
        LIMIT $4::int
      )
      SELECT
        (SELECT COUNT(*)::text FROM stale) AS discrepancy_count,
        COALESCE(
          (
            SELECT jsonb_agg(
              jsonb_build_object(
                'hour_start', hour_start,
                'status', status,
                'last_run_completed_at', last_run_completed_at
              )
              ORDER BY hour_start ASC
            )
            FROM sampled
          ),
          '[]'::jsonb
        ) AS samples
    `,
		[
			window.startAt,
			window.endAt,
			env.MMM_READINESS_GA4_FALLBACK_MAX_LAG_HOURS,
			env.DATA_QUALITY_SAMPLE_LIMIT,
		],
	);
	const row = result.rows[0];
	const discrepancyCount = Number(row?.discrepancy_count ?? 0);
	const evaluated = evaluateDiscrepancyCount({
		discrepancyCount,
		threshold: 0,
		severityOnAlert: "critical",
		healthySummary: "MMM GA4 fallback status passed.",
		warningSummary: "MMM GA4 fallback status had incomplete hours within threshold.",
		alertSummary: `${discrepancyCount} GA4 fallback ingestion ${pluralize("hour", discrepancyCount)} breached readiness.`,
		details: {
			window,
			maxLagHours: env.MMM_READINESS_GA4_FALLBACK_MAX_LAG_HOURS,
			samples: row?.samples ?? [],
		},
	});

	return { ...evaluated, checkKey: "mmm_readiness_ga4_fallback_status" };
}

function emitCheckLog(runDate: string, check: DataQualityCheckResult): void {
	const fields = {
		service: process.env.K_SERVICE ?? "roas-radar-data-quality",
		runDate,
		checkKey: check.checkKey,
		status: check.status,
		severity: check.severity,
		discrepancyCount: check.discrepancyCount,
		threshold: check.threshold,
		alertTriggered: check.alertTriggered,
		details: check.details,
	};

	if (check.alertTriggered && check.severity === "critical") {
		logError("data_quality_alert_triggered", new Error(check.summary), fields);
		return;
	}

	if (check.alertTriggered || check.status === "warning") {
		logWarning(
			check.alertTriggered
				? "data_quality_alert_triggered"
				: "data_quality_check_evaluated",
			fields,
		);
		return;
	}

	logInfo("data_quality_check_evaluated", fields);
}

export function resolveRunDate(now = new Date()): string {
	const target = addUtcDays(
		new Date(
			Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
		),
		-env.DATA_QUALITY_TARGET_LAG_DAYS,
	);
	return toDateString(target);
}

export function buildLookbackDates(
	runDate: string,
	lookbackDays: number,
): string[] {
	const end = new Date(`${runDate}T00:00:00.000Z`);
	const days = Math.max(lookbackDays, 1);
	const dates: string[] = [];

	for (let index = days - 1; index >= 0; index -= 1) {
		dates.push(toDateString(addUtcDays(end, -index)));
	}

	return dates;
}

export function detectAnomalyFlags(
	rows: DataQualityMetricRow[],
	runDate: string,
): AnomalyFlag[] {
	const current = rows.find((row) => row.metric_date === runDate);

	if (!current) {
		return [];
	}

	const baselineRows = rows.filter((row) => row.metric_date !== runDate);
	if (baselineRows.length === 0) {
		return [];
	}

	const metrics: Array<AnomalyFlag["metric"]> = [
		"visits",
		"orders",
		"revenue",
		"spend",
	];

	return metrics
		.map((metric) => {
			const baselineValue =
				baselineRows.reduce((sum, row) => sum + toNumber(row[metric]), 0) /
				baselineRows.length;
			const currentValue = toNumber(current[metric]);
			const absoluteDelta = baselineValue - currentValue;
			const relativeDelta =
				baselineValue <= 0 ? null : absoluteDelta / baselineValue;

			return {
				metric,
				currentValue,
				baselineValue,
				absoluteDelta,
				relativeDelta,
			};
		})
		.filter(
			(flag) =>
				flag.baselineValue >= env.DATA_QUALITY_ANOMALY_MIN_BASELINE &&
				flag.absoluteDelta > 0 &&
				(flag.relativeDelta ?? 0) >= env.DATA_QUALITY_ANOMALY_THRESHOLD_RATIO,
		);
}

export async function fetchDataQualityReport(runDate: string): Promise<{
	runDate: string;
	totals: {
		totalChecks: number;
		failedChecks: number;
		warningChecks: number;
		totalDiscrepancies: number;
	};
	checks: Array<{
		checkKey: string;
		status: PersistedCheckRow["status"];
		severity: PersistedCheckRow["severity"];
		discrepancyCount: number;
		summary: string;
		details: Record<string, unknown>;
		checkedAt: string;
		alertEmittedAt: string | null;
	}>;
}> {
	const result = await query<PersistedCheckRow>(
		`
      SELECT
        run_date::text,
        check_key,
        status,
        severity,
        discrepancy_count,
        summary,
        details,
        checked_at,
        alert_emitted_at
      FROM data_quality_check_runs
      WHERE run_date = $1::date
      ORDER BY
        CASE status
          WHEN 'failed' THEN 0
          WHEN 'warning' THEN 1
          ELSE 2
        END,
        check_key ASC
    `,
		[runDate],
	);

	const checks = result.rows.map((row) => ({
		checkKey: row.check_key,
		status: row.status,
		severity: row.severity,
		discrepancyCount: row.discrepancy_count,
		summary: row.summary,
		details: row.details,
		checkedAt: row.checked_at.toISOString(),
		alertEmittedAt: row.alert_emitted_at?.toISOString() ?? null,
	}));

	return {
		runDate,
		totals: {
			totalChecks: checks.length,
			failedChecks: checks.filter((check) => check.status === "failed").length,
			warningChecks: checks.filter((check) => check.status === "warning")
				.length,
			totalDiscrepancies: checks.reduce(
				(sum, check) => sum + check.discrepancyCount,
				0,
			),
		},
		checks,
	};
}

export async function runDailyDataQualityChecks(
	runDate = resolveRunDate(),
): Promise<{
	runDate: string;
	totals: {
		totalChecks: number;
		failedChecks: number;
		warningChecks: number;
		totalDiscrepancies: number;
	};
}> {
	const checks = await Promise.all([
		buildReportingAnomalyCheck(runDate),
		buildOrphanSessionCheck(runDate),
		buildDuplicateCanonicalAssignmentCheck(runDate),
		buildConflictingShopifyMappingCheck(runDate),
		buildHashFormatAnomalyCheck(runDate),
		buildMmmCaptureCompletenessCheck(runDate),
		buildMmmMissingSessionIdRateCheck(runDate),
		buildMmmDualWriteMismatchCheck(runDate),
		buildMmmResolverUnattributedRateCheck(runDate),
		buildMmmSpendFreshnessCheck(runDate),
		buildMmmCampaignMetadataFreshnessCheck(runDate),
		buildMmmReportingAggregateFreshnessCheck(runDate),
		buildMmmDataQualityBlockersCheck(runDate),
		buildMmmGa4FallbackStatusCheck(runDate),
	]);

	await withTransaction(async (client) => {
		for (const check of checks) {
			await client.query(
				`
          INSERT INTO data_quality_check_runs (
            run_date,
            check_key,
            status,
            severity,
            discrepancy_count,
            summary,
            details,
            checked_at,
            alert_emitted_at,
            updated_at
          )
          VALUES (
            $1::date,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::jsonb,
            now(),
            CASE WHEN $8 THEN now() ELSE NULL END,
            now()
          )
          ON CONFLICT (run_date, check_key)
          DO UPDATE SET
            status = EXCLUDED.status,
            severity = EXCLUDED.severity,
            discrepancy_count = EXCLUDED.discrepancy_count,
            summary = EXCLUDED.summary,
            details = EXCLUDED.details,
            checked_at = now(),
            alert_emitted_at = CASE
              WHEN EXCLUDED.alert_emitted_at IS NOT NULL THEN now()
              ELSE NULL
            END,
            updated_at = now()
        `,
				[
					runDate,
					check.checkKey,
					check.status,
					check.severity,
					check.discrepancyCount,
					check.summary,
					JSON.stringify(check.details),
					check.alertTriggered,
				],
			);
		}
	});

	for (const check of checks) {
		emitCheckLog(runDate, check);
	}

	const report = await fetchDataQualityReport(runDate);
	logInfo("data_quality_run_completed", {
		service: process.env.K_SERVICE ?? "roas-radar-data-quality",
		runDate,
		totals: report.totals,
	});

	return {
		runDate: report.runDate,
		totals: report.totals,
	};
}

export const __dataQualityTestUtils = {
	resolveRunDate,
	resolveReadinessWindow,
	buildLookbackDates,
	detectAnomalyFlags,
	evaluateDiscrepancyCount,
	evaluateMaximumRate,
};
