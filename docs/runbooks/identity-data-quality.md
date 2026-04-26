# Identity Data Quality Runbook

Use this runbook when the scheduled data-quality job emits `data_quality_alert_triggered` or `/api/reporting/reconciliation` shows failed identity checks.

## Checks

- `identity_graph_orphan_sessions`: sessions have attribution or Shopify evidence but no `identity_journey_id`.
- `identity_graph_duplicate_canonical_assignments`: the same session resolves to multiple canonical journeys across `tracking_sessions`, `tracking_events`, `session_attribution_identities`, or `customer_journey`.
- `identity_graph_conflicting_shopify_mappings`: a single `shopify_customer_id` resolves to multiple journeys across Shopify and identity surfaces.
- `identity_graph_hash_format_anomalies`: hashed identifiers are present but do not match the expected 64-character lowercase SHA-256 format.
- `reporting_anomaly_check`: reporting metrics materially dropped versus the trailing baseline.

## First Response

1. Open `GET /api/reporting/reconciliation?runDate=YYYY-MM-DD` for the run date in the alert payload.
2. Confirm which `checkKey` breached and review the `details` samples.
3. Check the `roas-radar-data-quality` Cloud Run Job execution logs for `data_quality_alert_triggered`.

## Investigation Queries

### Orphan sessions

```sql
SELECT s.id, s.first_seen_at, s.last_seen_at
FROM tracking_sessions s
WHERE s.identity_journey_id IS NULL
  AND (
    EXISTS (SELECT 1 FROM tracking_events e WHERE e.session_id = s.id)
    OR EXISTS (SELECT 1 FROM session_attribution_identities sai WHERE sai.roas_radar_session_id = s.id)
    OR EXISTS (SELECT 1 FROM shopify_orders o WHERE o.landing_session_id = s.id)
  )
ORDER BY s.last_seen_at DESC
LIMIT 50;
```

### Duplicate canonical assignments

```sql
WITH session_assignments AS (
  SELECT id::text AS session_id, identity_journey_id::text AS journey_id, 'tracking_sessions' AS source_table
  FROM tracking_sessions
  WHERE identity_journey_id IS NOT NULL
  UNION ALL
  SELECT session_id::text, identity_journey_id::text, 'tracking_events'
  FROM tracking_events
  WHERE identity_journey_id IS NOT NULL
  UNION ALL
  SELECT roas_radar_session_id::text, identity_journey_id::text, 'session_attribution_identities'
  FROM session_attribution_identities
  WHERE identity_journey_id IS NOT NULL
  UNION ALL
  SELECT session_id::text, identity_journey_id::text, 'customer_journey'
  FROM customer_journey
)
SELECT session_id, array_agg(DISTINCT journey_id ORDER BY journey_id) AS journey_ids
FROM session_assignments
GROUP BY session_id
HAVING COUNT(DISTINCT journey_id) > 1
ORDER BY session_id
LIMIT 50;
```

### Conflicting Shopify mappings

```sql
WITH shopify_assignments AS (
  SELECT authoritative_shopify_customer_id AS shopify_customer_id, id::text AS journey_id, 'identity_journeys' AS source_table
  FROM identity_journeys
  WHERE authoritative_shopify_customer_id IS NOT NULL
  UNION ALL
  SELECT shopify_customer_id, identity_journey_id::text, 'shopify_customers'
  FROM shopify_customers
  WHERE shopify_customer_id IS NOT NULL AND identity_journey_id IS NOT NULL
  UNION ALL
  SELECT shopify_customer_id, identity_journey_id::text, 'shopify_orders'
  FROM shopify_orders
  WHERE shopify_customer_id IS NOT NULL AND identity_journey_id IS NOT NULL
)
SELECT shopify_customer_id, array_agg(DISTINCT journey_id ORDER BY journey_id) AS journey_ids
FROM shopify_assignments
GROUP BY shopify_customer_id
HAVING COUNT(DISTINCT journey_id) > 1
ORDER BY shopify_customer_id
LIMIT 50;
```

### Hash anomalies

```sql
SELECT *
FROM data_quality_check_runs
WHERE check_key = 'identity_graph_hash_format_anomalies'
ORDER BY run_date DESC
LIMIT 7;
```

## Remediation

1. Re-run the graph backfill for affected surfaces with `npm run identity:backfill-graph` if the issue is stale assignment data.
2. Repair or quarantine conflicting identifiers before replaying ingestion when the breach is caused by authoritative Shopify conflicts.
3. Fix the upstream hashing or normalization path before replaying records if the breach is a hash-format anomaly.
4. Re-run `npm run data-quality:check` locally or re-execute the Cloud Run Job after repair to confirm the alert clears.
