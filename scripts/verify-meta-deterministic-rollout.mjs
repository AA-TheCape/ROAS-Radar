import pg from "pg";

const { Client } = pg;

const REQUIRED_CONSTRAINTS = [
	"deterministic_event_sources_meta_api_provenance_chk",
	"raw_deterministic_events_api_verified_chk",
	"deterministic_event_facts_api_verified_chk",
	"deterministic_event_verification_statuses_api_verified_chk",
	"deterministic_model_outputs_api_verified_chk",
	"meta_ads_deterministic_attribution_aggregates_evidence_origin_chk",
	"meta_ads_deterministic_attribution_aggregates_family_event_chk",
	"meta_ads_deterministic_attribution_aggregates_raw_traceability_chk",
	"meta_ads_deterministic_attribution_aggregates_verified_shape_chk",
	"meta_ads_deterministic_attribution_aggregates_window_chk",
];

function requireDatabaseUrl() {
	if (!process.env.DATABASE_URL) {
		throw new Error(
			"DATABASE_URL is required for Meta deterministic rollout verification",
		);
	}

	return process.env.DATABASE_URL;
}

function numberFromEnv(name, fallback) {
	const rawValue = process.env[name];
	if (!rawValue) {
		return fallback;
	}

	const parsed = Number(rawValue);
	if (!Number.isFinite(parsed) || parsed < 0) {
		throw new Error(`${name} must be a non-negative number`);
	}

	return parsed;
}

function printResult(level, label, detail) {
	process.stdout.write(`${level.toUpperCase()}: ${label}`);
	if (detail) {
		process.stdout.write(` - ${detail}`);
	}
	process.stdout.write("\n");
}

function countRows(result) {
	return Number(result.rows[0]?.count ?? 0);
}

async function main() {
	const thresholds = {
		freshnessHours: numberFromEnv("META_DETERMINISTIC_FRESHNESS_HOURS", 30),
		windowHours: numberFromEnv("META_DETERMINISTIC_ROLLOUT_WINDOW_HOURS", 48),
		maxQuarantineRate: numberFromEnv(
			"META_DETERMINISTIC_MAX_QUARANTINE_RATE",
			0.25,
		),
		staleProcessingHours: numberFromEnv(
			"META_DETERMINISTIC_STALE_PROCESSING_HOURS",
			2,
		),
	};
	const failures = [];
	const warnings = [];
	const client = new Client({ connectionString: requireDatabaseUrl() });

	await client.connect();

	try {
		const constraintResult = await client.query(
			`
        SELECT conname
        FROM pg_constraint
        WHERE conname = ANY($1::text[])
          AND convalidated = true
      `,
			[REQUIRED_CONSTRAINTS],
		);
		const presentConstraints = new Set(
			constraintResult.rows.map((row) => row.conname),
		);
		const missingConstraints = REQUIRED_CONSTRAINTS.filter(
			(name) => !presentConstraints.has(name),
		);
		if (missingConstraints.length > 0) {
			failures.push(
				`missing deterministic DB constraints: ${missingConstraints.join(", ")}`,
			);
		} else {
			printResult("pass", "database constraints", "all required checks exist");
		}

		const enabledConnections = countRows(
			await client.query(`
        SELECT COUNT(*)::int AS count
        FROM meta_ads_connections
        WHERE deterministic_view_impression_sync_enabled = true
          AND status = 'active'
      `),
		);
		if (enabledConnections === 0) {
			warnings.push("no active Meta connections have deterministic sync enabled");
		} else {
			printResult(
				"pass",
				"enabled connections",
				`${enabledConnections} active connection(s) enabled`,
			);
		}

		const staleConnections = await client.query(
			`
        SELECT
          connections.id,
          connections.ad_account_id,
          MAX(jobs.completed_at) AS latest_completed_at
        FROM meta_ads_connections connections
        LEFT JOIN meta_ads_deterministic_sync_jobs jobs
          ON jobs.connection_id = connections.id
          AND jobs.status = 'completed'
          AND jobs.completed_at >= now() - ($1::text || ' hours')::interval
        WHERE connections.deterministic_view_impression_sync_enabled = true
          AND connections.status = 'active'
        GROUP BY connections.id, connections.ad_account_id
        HAVING MAX(jobs.completed_at) IS NULL
        ORDER BY connections.id
      `,
			[thresholds.freshnessHours],
		);
		if (staleConnections.rowCount > 0) {
			failures.push(
				`enabled connection(s) missing fresh completed jobs: ${staleConnections.rows
					.map((row) => `${row.id}/${row.ad_account_id}`)
					.join(", ")}`,
			);
		} else if (enabledConnections > 0) {
			printResult(
				"pass",
				"per-connection freshness",
				"all enabled active connections have a fresh completed job",
			);
		}

		const latestCompleted = await client.query(`
      SELECT
        id,
        connection_id,
        sync_date,
        completed_at,
        EXTRACT(EPOCH FROM (now() - completed_at)) / 3600 AS age_hours
      FROM meta_ads_deterministic_sync_jobs
      WHERE status = 'completed'
        AND completed_at IS NOT NULL
      ORDER BY completed_at DESC
      LIMIT 1
    `);
		if (latestCompleted.rowCount === 0) {
			if (enabledConnections > 0) {
				failures.push("no completed deterministic sync job found");
			} else {
				warnings.push("no completed deterministic sync job found");
			}
		} else {
			const latest = latestCompleted.rows[0];
			const ageHours = Number(latest.age_hours);
			if (ageHours > thresholds.freshnessHours) {
				failures.push(
					`latest completed sync job is ${ageHours.toFixed(1)} hours old; threshold is ${thresholds.freshnessHours}`,
				);
			} else {
				printResult(
					"pass",
					"sync freshness",
					`job ${latest.id} completed ${ageHours.toFixed(1)} hours ago for ${latest.sync_date}`,
				);
			}
		}

		const unhealthyJobs = await client.query(
			`
        SELECT status, COUNT(*)::int AS count
        FROM meta_ads_deterministic_sync_jobs
        WHERE (
          status IN ('failed', 'retry')
          AND updated_at >= now() - ($1::text || ' hours')::interval
        )
        OR (
          status = 'processing'
          AND locked_at < now() - ($2::text || ' hours')::interval
        )
        GROUP BY status
        ORDER BY status
      `,
			[thresholds.windowHours, thresholds.staleProcessingHours],
		);
		if (unhealthyJobs.rowCount > 0) {
			failures.push(
				`unhealthy sync jobs: ${unhealthyJobs.rows
					.map((row) => `${row.status}=${row.count}`)
					.join(", ")}`,
			);
		} else {
			printResult("pass", "sync job queue", "no recent failed/retry/stale jobs");
		}

		const rowCounts = await client.query(
			`
        SELECT 'raw' AS surface, COUNT(*)::bigint AS count
        FROM raw_deterministic_events
        WHERE platform = 'meta_ads'
          AND ingested_at_utc >= now() - ($1::text || ' hours')::interval
        UNION ALL
        SELECT 'facts' AS surface, COUNT(*)::bigint AS count
        FROM deterministic_event_facts
        WHERE platform = 'meta_ads'
          AND normalized_at_utc >= now() - ($1::text || ' hours')::interval
        UNION ALL
        SELECT 'aggregates' AS surface, COUNT(*)::bigint AS count
        FROM meta_ads_deterministic_attribution_aggregates
        WHERE created_at >= now() - ($1::text || ' hours')::interval
        UNION ALL
        SELECT 'model_outputs' AS surface, COUNT(*)::bigint AS count
        FROM deterministic_model_outputs
        WHERE platform = 'meta_ads'
          AND generated_at_utc >= now() - ($1::text || ' hours')::interval
      `,
			[thresholds.windowHours],
		);
		const countsBySurface = Object.fromEntries(
			rowCounts.rows.map((row) => [row.surface, Number(row.count)]),
		);
		for (const [surface, count] of Object.entries(countsBySurface)) {
			printResult("info", `${surface} rows`, `${count} in ${thresholds.windowHours}h`);
		}
		if (
			enabledConnections > 0 &&
			(countsBySurface.raw ?? 0) === 0 &&
			(countsBySurface.facts ?? 0) === 0 &&
			(countsBySurface.aggregates ?? 0) === 0
		) {
			failures.push(
				`no deterministic raw/fact/aggregate rows persisted in ${thresholds.windowHours}h`,
			);
		}

		const quarantineResult = await client.query(
			`
        WITH recent_quarantine AS (
          SELECT COUNT(*)::numeric AS quarantined
          FROM deterministic_event_evidence_quarantine
          WHERE platform = 'meta_ads'
            AND quarantined_at_utc >= now() - ($1::text || ' hours')::interval
        ),
        recent_raw AS (
          SELECT COUNT(*)::numeric AS accepted
          FROM raw_deterministic_events
          WHERE platform = 'meta_ads'
            AND ingested_at_utc >= now() - ($1::text || ' hours')::interval
        )
        SELECT
          COALESCE(recent_quarantine.quarantined, 0)::int AS quarantined,
          COALESCE(recent_raw.accepted, 0)::int AS accepted,
          CASE
            WHEN COALESCE(recent_quarantine.quarantined, 0) + COALESCE(recent_raw.accepted, 0) = 0 THEN 0
            ELSE COALESCE(recent_quarantine.quarantined, 0)
              / (COALESCE(recent_quarantine.quarantined, 0) + COALESCE(recent_raw.accepted, 0))
          END::float8 AS quarantine_rate
        FROM recent_quarantine, recent_raw
      `,
			[thresholds.windowHours],
		);
		const quarantine = quarantineResult.rows[0];
		const quarantineRate = Number(quarantine.quarantine_rate ?? 0);
		if (quarantineRate > thresholds.maxQuarantineRate) {
			failures.push(
				`quarantine rate ${(quarantineRate * 100).toFixed(1)}% exceeds ${(thresholds.maxQuarantineRate * 100).toFixed(1)}%`,
			);
		} else {
			printResult(
				"pass",
				"quarantine rate",
				`${(quarantineRate * 100).toFixed(1)}% (${quarantine.quarantined} quarantined, ${quarantine.accepted} accepted)`,
			);
		}

		const topQuarantineReasons = await client.query(
			`
        SELECT reason_code, COUNT(*)::int AS count
        FROM deterministic_event_evidence_quarantine
        WHERE platform = 'meta_ads'
          AND quarantined_at_utc >= now() - ($1::text || ' hours')::interval
        GROUP BY reason_code
        ORDER BY count DESC, reason_code
        LIMIT 5
      `,
			[thresholds.windowHours],
		);
		for (const row of topQuarantineReasons.rows) {
			printResult("info", "quarantine reason", `${row.reason_code}=${row.count}`);
		}

		const contractViolations = await client.query(`
      SELECT 'verified_raw_not_meta_api' AS violation, COUNT(*)::int AS count
      FROM raw_deterministic_events
      WHERE platform_verified = true
        AND (platform <> 'meta_ads' OR evidence_origin <> 'api')
      UNION ALL
      SELECT 'verified_facts_not_meta_api' AS violation, COUNT(*)::int AS count
      FROM deterministic_event_facts
      WHERE platform_verified = true
        AND (platform <> 'meta_ads' OR evidence_origin <> 'api')
      UNION ALL
      SELECT 'verified_outputs_not_meta_api' AS violation, COUNT(*)::int AS count
      FROM deterministic_model_outputs
      WHERE platform_verified = true
        AND (platform <> 'meta_ads' OR evidence_origin <> 'api')
      UNION ALL
      SELECT 'bad_model_event_pair' AS violation, COUNT(*)::int AS count
      FROM deterministic_model_outputs
      WHERE (model_key = 'deterministic_views' AND event_type <> 'view')
         OR (model_key = 'deterministic_impressions' AND event_type <> 'impression')
      UNION ALL
      SELECT 'bad_aggregate_family_event_pair' AS violation, COUNT(*)::int AS count
      FROM meta_ads_deterministic_attribution_aggregates
      WHERE (attribution_family = 'deterministic_views' AND event_type <> 'view')
         OR (attribution_family = 'deterministic_impressions' AND event_type <> 'impression')
      UNION ALL
      SELECT 'bad_aggregate_window' AS violation, COUNT(*)::int AS count
      FROM meta_ads_deterministic_attribution_aggregates
      WHERE attribution_window <> '7d_view'
         OR attribution_window_days <> 7
    `);
		const nonZeroViolations = contractViolations.rows.filter(
			(row) => Number(row.count) > 0,
		);
		if (nonZeroViolations.length > 0) {
			failures.push(
				`contract violations: ${nonZeroViolations
					.map((row) => `${row.violation}=${row.count}`)
					.join(", ")}`,
			);
		} else {
			printResult("pass", "contract shape", "no persisted separation violations");
		}

		const reconciliation = await client.query(`
      SELECT run_date, status, mismatch_count, checked_at
      FROM meta_ads_deterministic_reconciliation_runs
      ORDER BY checked_at DESC
      LIMIT 1
    `);
		if (reconciliation.rowCount === 0) {
			warnings.push("no Meta deterministic reconciliation run has been recorded");
		} else {
			const latest = reconciliation.rows[0];
			if (latest.status !== "passed") {
				failures.push(
					`latest reconciliation for ${latest.run_date} is ${latest.status} with ${latest.mismatch_count} mismatch(es)`,
				);
			} else {
				printResult(
					"pass",
					"reconciliation",
					`${latest.run_date} passed with ${latest.mismatch_count} mismatch(es)`,
				);
			}
		}
	} finally {
		await client.end();
	}

	for (const warning of warnings) {
		printResult("warn", warning);
	}

	if (failures.length > 0) {
		for (const failure of failures) {
			printResult("fail", failure);
		}
		process.exit(1);
	}

	printResult("pass", "Meta deterministic rollout verification complete");
}

main().catch((error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	process.exit(1);
});
