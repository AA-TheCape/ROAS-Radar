"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.__dataQualityTestUtils = void 0;
exports.resolveRunDate = resolveRunDate;
exports.buildLookbackDates = buildLookbackDates;
exports.detectAnomalyFlags = detectAnomalyFlags;
exports.fetchDataQualityReport = fetchDataQualityReport;
exports.runDailyDataQualityChecks = runDailyDataQualityChecks;
const env_js_1 = require("../../config/env.js");
const pool_js_1 = require("../../db/pool.js");
const index_js_1 = require("../../observability/index.js");
const HASH_FORMAT_REGEX = '^[0-9a-f]{64}$';
function toDateString(value) {
    return value.toISOString().slice(0, 10);
}
function addUtcDays(date, days) {
    const next = new Date(date);
    next.setUTCDate(next.getUTCDate() + days);
    return next;
}
function toRunDateEnd(runDate) {
    return `${toDateString(addUtcDays(new Date(`${runDate}T00:00:00.000Z`), 1))}T00:00:00.000Z`;
}
function toNumber(value) {
    return typeof value === 'number' ? value : Number(value);
}
function pluralize(label, count) {
    return count === 1 ? label : `${label}s`;
}
function evaluateDiscrepancyCount(input) {
    const discrepancyCount = Math.max(0, input.discrepancyCount);
    const threshold = Math.max(0, input.threshold);
    const alertTriggered = discrepancyCount > threshold;
    if (discrepancyCount === 0) {
        return {
            checkKey: '',
            status: 'healthy',
            severity: 'info',
            discrepancyCount,
            summary: input.healthySummary,
            details: input.details,
            threshold,
            alertTriggered: false
        };
    }
    if (alertTriggered) {
        return {
            checkKey: '',
            status: input.severityOnAlert === 'critical' ? 'failed' : 'warning',
            severity: input.severityOnAlert,
            discrepancyCount,
            summary: input.alertSummary,
            details: input.details,
            threshold,
            alertTriggered: true
        };
    }
    return {
        checkKey: '',
        status: 'warning',
        severity: 'warning',
        discrepancyCount,
        summary: input.warningSummary,
        details: input.details,
        threshold,
        alertTriggered: false
    };
}
async function buildReportingAnomalyCheck(runDate) {
    const lookbackDates = buildLookbackDates(runDate, env_js_1.env.DATA_QUALITY_ANOMALY_LOOKBACK_DAYS + 1);
    const metricsResult = await (0, pool_js_1.query)(`
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
    `, [lookbackDates]);
    const anomalyFlags = detectAnomalyFlags(metricsResult.rows, runDate);
    const evaluated = evaluateDiscrepancyCount({
        discrepancyCount: anomalyFlags.length,
        threshold: env_js_1.env.DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD,
        severityOnAlert: 'warning',
        healthySummary: 'No reporting anomalies detected for the run date.',
        warningSummary: `${anomalyFlags.length} reporting ${pluralize('metric', anomalyFlags.length)} deviated from the trailing baseline but remained within the configured alert threshold.`,
        alertSummary: `${anomalyFlags.length} reporting ${pluralize('metric', anomalyFlags.length)} exceeded the trailing-baseline alert threshold.`,
        details: {
            anomalyFlags,
            lookbackDates
        }
    });
    return {
        ...evaluated,
        checkKey: 'reporting_anomaly_check'
    };
}
async function buildOrphanSessionCheck(runDate) {
    const result = await (0, pool_js_1.query)(`
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
    `, [toRunDateEnd(runDate), env_js_1.env.DATA_QUALITY_SAMPLE_LIMIT]);
    const row = result.rows[0];
    const discrepancyCount = Number(row?.discrepancy_count ?? 0);
    const evaluated = evaluateDiscrepancyCount({
        discrepancyCount,
        threshold: env_js_1.env.DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD,
        severityOnAlert: 'critical',
        healthySummary: 'No orphan sessions were detected in the identity graph snapshot.',
        warningSummary: `${discrepancyCount} orphan ${pluralize('session', discrepancyCount)} remain unresolved but did not breach the configured alert threshold.`,
        alertSummary: `${discrepancyCount} orphan ${pluralize('session', discrepancyCount)} breached the configured alert threshold.`,
        details: {
            sampleSessionIds: row?.sample_session_ids ?? []
        }
    });
    return {
        ...evaluated,
        checkKey: 'identity_graph_orphan_sessions'
    };
}
async function buildDuplicateCanonicalAssignmentCheck(runDate) {
    const result = await (0, pool_js_1.query)(`
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
    `, [toRunDateEnd(runDate), env_js_1.env.DATA_QUALITY_SAMPLE_LIMIT]);
    const row = result.rows[0];
    const discrepancyCount = Number(row?.discrepancy_count ?? 0);
    const evaluated = evaluateDiscrepancyCount({
        discrepancyCount,
        threshold: env_js_1.env.DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD,
        severityOnAlert: 'critical',
        healthySummary: 'No duplicate canonical session assignments were detected.',
        warningSummary: `${discrepancyCount} session ${pluralize('assignment', discrepancyCount)} disagreed across canonical surfaces but remained within the configured alert threshold.`,
        alertSummary: `${discrepancyCount} session ${pluralize('assignment', discrepancyCount)} disagreed across canonical surfaces and breached the alert threshold.`,
        details: {
            sampleConflicts: row?.samples ?? []
        }
    });
    return {
        ...evaluated,
        checkKey: 'identity_graph_duplicate_canonical_assignments'
    };
}
async function buildConflictingShopifyMappingCheck(runDate) {
    const result = await (0, pool_js_1.query)(`
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
    `, [toRunDateEnd(runDate), env_js_1.env.DATA_QUALITY_SAMPLE_LIMIT]);
    const row = result.rows[0];
    const discrepancyCount = Number(row?.discrepancy_count ?? 0);
    const evaluated = evaluateDiscrepancyCount({
        discrepancyCount,
        threshold: env_js_1.env.DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD,
        severityOnAlert: 'critical',
        healthySummary: 'No conflicting Shopify customer mappings were detected.',
        warningSummary: `${discrepancyCount} Shopify customer ${pluralize('mapping', discrepancyCount)} disagreed across canonical surfaces but remained within the configured alert threshold.`,
        alertSummary: `${discrepancyCount} Shopify customer ${pluralize('mapping', discrepancyCount)} disagreed across canonical surfaces and breached the alert threshold.`,
        details: {
            sampleConflicts: row?.samples ?? []
        }
    });
    return {
        ...evaluated,
        checkKey: 'identity_graph_conflicting_shopify_mappings'
    };
}
async function buildHashFormatAnomalyCheck(runDate) {
    const result = await (0, pool_js_1.query)(`
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
    `, [toRunDateEnd(runDate), HASH_FORMAT_REGEX, env_js_1.env.DATA_QUALITY_SAMPLE_LIMIT]);
    const row = result.rows[0];
    const discrepancyCount = Number(row?.discrepancy_count ?? 0);
    const evaluated = evaluateDiscrepancyCount({
        discrepancyCount,
        threshold: env_js_1.env.DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD,
        severityOnAlert: 'warning',
        healthySummary: 'No hash-format anomalies were detected across identity and Shopify surfaces.',
        warningSummary: `${discrepancyCount} hash-format ${pluralize('anomaly', discrepancyCount)} were detected but remained within the configured alert threshold.`,
        alertSummary: `${discrepancyCount} hash-format ${pluralize('anomaly', discrepancyCount)} breached the configured alert threshold.`,
        details: {
            samples: row?.samples ?? [],
            expectedPattern: HASH_FORMAT_REGEX
        }
    });
    return {
        ...evaluated,
        checkKey: 'identity_graph_hash_format_anomalies'
    };
}
function emitCheckLog(runDate, check) {
    const fields = {
        service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
        runDate,
        checkKey: check.checkKey,
        status: check.status,
        severity: check.severity,
        discrepancyCount: check.discrepancyCount,
        threshold: check.threshold,
        alertTriggered: check.alertTriggered,
        details: check.details
    };
    if (check.alertTriggered && check.severity === 'critical') {
        (0, index_js_1.logError)('data_quality_alert_triggered', new Error(check.summary), fields);
        return;
    }
    if (check.alertTriggered || check.status === 'warning') {
        (0, index_js_1.logWarning)(check.alertTriggered ? 'data_quality_alert_triggered' : 'data_quality_check_evaluated', fields);
        return;
    }
    (0, index_js_1.logInfo)('data_quality_check_evaluated', fields);
}
function resolveRunDate(now = new Date()) {
    const target = addUtcDays(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())), -env_js_1.env.DATA_QUALITY_TARGET_LAG_DAYS);
    return toDateString(target);
}
function buildLookbackDates(runDate, lookbackDays) {
    const end = new Date(`${runDate}T00:00:00.000Z`);
    const days = Math.max(lookbackDays, 1);
    const dates = [];
    for (let index = days - 1; index >= 0; index -= 1) {
        dates.push(toDateString(addUtcDays(end, -index)));
    }
    return dates;
}
function detectAnomalyFlags(rows, runDate) {
    const current = rows.find((row) => row.metric_date === runDate);
    if (!current) {
        return [];
    }
    const baselineRows = rows.filter((row) => row.metric_date !== runDate);
    if (baselineRows.length === 0) {
        return [];
    }
    const metrics = ['visits', 'orders', 'revenue', 'spend'];
    return metrics
        .map((metric) => {
        const baselineValue = baselineRows.reduce((sum, row) => sum + toNumber(row[metric]), 0) / baselineRows.length;
        const currentValue = toNumber(current[metric]);
        const absoluteDelta = baselineValue - currentValue;
        const relativeDelta = baselineValue <= 0 ? null : absoluteDelta / baselineValue;
        return {
            metric,
            currentValue,
            baselineValue,
            absoluteDelta,
            relativeDelta
        };
    })
        .filter((flag) => flag.baselineValue >= env_js_1.env.DATA_QUALITY_ANOMALY_MIN_BASELINE &&
        flag.absoluteDelta > 0 &&
        (flag.relativeDelta ?? 0) >= env_js_1.env.DATA_QUALITY_ANOMALY_THRESHOLD_RATIO);
}
async function fetchDataQualityReport(runDate) {
    const result = await (0, pool_js_1.query)(`
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
    `, [runDate]);
    const checks = result.rows.map((row) => ({
        checkKey: row.check_key,
        status: row.status,
        severity: row.severity,
        discrepancyCount: row.discrepancy_count,
        summary: row.summary,
        details: row.details,
        checkedAt: row.checked_at.toISOString(),
        alertEmittedAt: row.alert_emitted_at?.toISOString() ?? null
    }));
    return {
        runDate,
        totals: {
            totalChecks: checks.length,
            failedChecks: checks.filter((check) => check.status === 'failed').length,
            warningChecks: checks.filter((check) => check.status === 'warning').length,
            totalDiscrepancies: checks.reduce((sum, check) => sum + check.discrepancyCount, 0)
        },
        checks
    };
}
async function runDailyDataQualityChecks(runDate = resolveRunDate()) {
    const checks = await Promise.all([
        buildReportingAnomalyCheck(runDate),
        buildOrphanSessionCheck(runDate),
        buildDuplicateCanonicalAssignmentCheck(runDate),
        buildConflictingShopifyMappingCheck(runDate),
        buildHashFormatAnomalyCheck(runDate)
    ]);
    await (0, pool_js_1.withTransaction)(async (client) => {
        for (const check of checks) {
            await client.query(`
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
        `, [
                runDate,
                check.checkKey,
                check.status,
                check.severity,
                check.discrepancyCount,
                check.summary,
                JSON.stringify(check.details),
                check.alertTriggered
            ]);
        }
    });
    for (const check of checks) {
        emitCheckLog(runDate, check);
    }
    const report = await fetchDataQualityReport(runDate);
    (0, index_js_1.logInfo)('data_quality_run_completed', {
        service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
        runDate,
        totals: report.totals
    });
    return {
        runDate: report.runDate,
        totals: report.totals
    };
}
exports.__dataQualityTestUtils = {
    resolveRunDate,
    buildLookbackDates,
    detectAnomalyFlags,
    evaluateDiscrepancyCount
};
