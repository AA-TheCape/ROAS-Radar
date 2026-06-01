# Attribution Confidence Scoring Runbook

Use this runbook when confidence scores, lookup-backed attribution metadata, fallback attribution, or order/result consistency drift from the `v1` contract in `docs/confidence-scoring-contract-v1.md`.

## Triggers

- Dashboard or API rows show `confidenceScore = null`, out-of-range values, or unexpected label/source combinations.
- `ROAS Radar * Resolver Unattributed Rate` rises while deterministic capture metrics look healthy.
- A migration, resolver change, Shopify hint recovery, GA4 fallback rollout, or identity stitching backfill changes attribution winners.
- `order_attribution_confidence_backfill_*` logs report failures or dry-run updates that exceed the expected rollout size.
- Reporting order metadata disagrees with attribution results for the same `shopify_order_id`.

## Model Summary

Confidence scoring describes match strength for the winning order attribution path. It does not change resolver precedence, attribution tier, revenue, campaign performance, or model credit math.

| Winner | Source code | Matching method | Score | Label |
| --- | --- | --- | ---: | --- |
| Landing session evidence | `landing_session_id` | `matched_by_landing_session` | `1.00` | `high` |
| Checkout token evidence | `checkout_token` | `matched_by_checkout_token` | `1.00` | `high` |
| Cart token evidence | `cart_token` | `matched_by_cart_token` | `0.90` | `high` |
| Customer identity fallback | `customer_identity` | `matched_by_customer_identity` | `0.60` | `medium` |
| Shopify hint fallback with supported click ID | `shopify_hint_fallback` | `shopify_hint_derived` | `0.55` | `low` |
| Shopify hint fallback with canonical UTMs only | `shopify_hint_fallback` | `shopify_hint_derived` | `0.40` | `low` |
| GA4 fallback with supported click ID | `ga4_fallback` | `ga4_fallback_derived` | `0.35` | `low` |
| GA4 fallback with canonical UTMs only | `ga4_fallback` | `ga4_fallback_derived` | `0.25` | `low` |
| No eligible match | `unattributed` | `unattributed` or `unknown` | `0.00` | `none` |

Resolver precedence remains landing session, checkout token, cart token, customer identity, Shopify hint fallback, GA4 fallback, then unattributed. A lower-confidence fallback must not replace a higher-precedence winner.

## Lookup Table Maintenance Rules

- Maintain `attribution_sources` and `matching_methods` through migrations only. Do not patch IDs manually in production.
- Treat numeric IDs as stable contract values. Existing rows and snapshots reference these IDs.
- Keep `code` lowercase snake case and under 64 characters. Labels may change for display clarity, but codes and IDs should not change without a new contract.
- Keep deprecated rows inactive instead of deleting them. Foreign keys use `ON DELETE RESTRICT`.
- Every new active matching method must point to the attribution source that owns it.
- Any new source, matching method, score, label, or precedence change requires a new versioned confidence contract and a backfill plan.

Inspect lookup health:

```sql
SELECT
  sources.id AS source_id,
  sources.code AS source_code,
  sources.display_label AS source_label,
  sources.is_active AS source_active,
  methods.id AS method_id,
  methods.code AS method_code,
  methods.display_label AS method_label,
  methods.is_active AS method_active
FROM attribution_sources sources
LEFT JOIN matching_methods methods
  ON methods.attribution_source_id = sources.id
ORDER BY sources.id, methods.id;
```

Find lookup-backed metadata that points to inactive or missing codes:

```sql
SELECT
  orders.shopify_order_id,
  orders.attribution_source_id,
  sources.code AS source_code,
  orders.matching_method_id,
  methods.code AS method_code,
  orders.last_attribution_run_at
FROM shopify_orders orders
LEFT JOIN attribution_sources sources
  ON sources.id = orders.attribution_source_id
LEFT JOIN matching_methods methods
  ON methods.id = orders.matching_method_id
WHERE sources.id IS NULL
   OR methods.id IS NULL
   OR sources.is_active IS NOT TRUE
   OR methods.is_active IS NOT TRUE
ORDER BY orders.last_attribution_run_at DESC NULLS LAST
LIMIT 100;
```

## Recompute Triggers

Recompute confidence metadata when any of these change for an already-ingested order:

- `attribution_results.match_source`, `attribution_reason`, `session_id`, or winning dimensions.
- `shopify_orders.attribution_source`, `attribution_reason`, `attribution_snapshot`, or recovered Shopify hint fields.
- GA4 fallback candidate eligibility, click ID detection, or canonical UTM normalization.
- Identity stitching output that can move a row into or out of `customer_identity`.
- Lookup table IDs, confidence score mapping, confidence contract version, or resolver precedence.
- A stale or failed order attribution backfill later writes a corrected winner.

Run a dry confidence backfill before write-enabled repair:

```bash
npm run attribution:backfill-confidence -- --dry-run --batch-size 1000
```

Then run the write-enabled pass with the same batch size. If the dry run stops partway through, resume with the last reported cursor:

```bash
npm run attribution:backfill-confidence -- --batch-size 1000 --resume-after-order-row-id 123456
```

## Troubleshooting Flow

1. Confirm whether the issue is score distribution, lookup metadata, fallback overuse, stale runs, or order/result disagreement.
2. Check whether the affected rows are recent live orders, imported Shopify orders, recovered hint orders, GA4 fallback rows, or an explicit backfill window.
3. Inspect source and method lookup tables before changing application code.
4. Compare `shopify_orders` against `attribution_results`; the same winner should drive source, method, score, contract version, and run timestamp.
5. If rows are stale but otherwise valid, run confidence backfill. If winners are wrong, queue order attribution backfill first, then confidence backfill if needed.
6. If fallback rates spike, split Shopify hint fallback from GA4 fallback and verify deterministic capture before replaying broad windows.

## SQL Checks

Fallback detection by day:

```sql
SELECT
  date_trunc('day', COALESCE(orders.processed_at, orders.created_at_shopify)) AS order_day,
  sources.code AS attribution_source,
  methods.code AS matching_method,
  COUNT(*) AS orders,
  ROUND(AVG(orders.attribution_confidence_score)::numeric, 2) AS avg_confidence
FROM shopify_orders orders
JOIN attribution_sources sources
  ON sources.id = orders.attribution_source_id
JOIN matching_methods methods
  ON methods.id = orders.matching_method_id
WHERE COALESCE(orders.processed_at, orders.created_at_shopify) >= now() - interval '14 days'
  AND sources.code IN ('shopify_hint_fallback', 'ga4_fallback', 'unattributed')
GROUP BY 1, 2, 3
ORDER BY order_day DESC, orders DESC;
```

Confidence distribution:

```sql
SELECT
  orders.attribution_confidence_contract_version AS contract_version,
  orders.attribution_confidence_score,
  CASE
    WHEN orders.attribution_confidence_score IN (1.00, 0.90) THEN 'high'
    WHEN orders.attribution_confidence_score = 0.60 THEN 'medium'
    WHEN orders.attribution_confidence_score IN (0.55, 0.40, 0.35, 0.25) THEN 'low'
    WHEN orders.attribution_confidence_score = 0.00 THEN 'none'
    ELSE 'unexpected'
  END AS derived_label,
  COUNT(*) AS orders
FROM shopify_orders orders
WHERE COALESCE(orders.processed_at, orders.created_at_shopify) >= now() - interval '30 days'
GROUP BY 1, 2, 3
ORDER BY orders.attribution_confidence_score DESC;
```

Unexpected scores or contract versions:

```sql
SELECT
  orders.shopify_order_id,
  orders.attribution_confidence_score,
  orders.attribution_confidence_contract_version,
  sources.code AS source_code,
  methods.code AS method_code,
  orders.last_attribution_run_at
FROM shopify_orders orders
JOIN attribution_sources sources
  ON sources.id = orders.attribution_source_id
JOIN matching_methods methods
  ON methods.id = orders.matching_method_id
WHERE orders.attribution_confidence_contract_version <> 'v1'
   OR orders.attribution_confidence_score NOT IN (1.00, 0.90, 0.60, 0.55, 0.40, 0.35, 0.25, 0.00)
   OR orders.attribution_confidence_score < 0
   OR orders.attribution_confidence_score > 1
ORDER BY orders.last_attribution_run_at DESC NULLS LAST
LIMIT 100;
```

Stale run detection:

```sql
SELECT
  orders.shopify_order_id,
  orders.processed_at,
  orders.created_at_shopify,
  orders.updated_at_shopify,
  orders.last_attribution_run_at,
  sources.code AS source_code,
  orders.attribution_confidence_score
FROM shopify_orders orders
JOIN attribution_sources sources
  ON sources.id = orders.attribution_source_id
WHERE COALESCE(orders.processed_at, orders.created_at_shopify) >= now() - interval '7 days'
  AND (
    orders.last_attribution_run_at IS NULL
    OR orders.last_attribution_run_at < COALESCE(orders.updated_at_shopify, orders.processed_at, orders.created_at_shopify)
  )
ORDER BY COALESCE(orders.last_attribution_run_at, '-infinity'::timestamptz) ASC
LIMIT 100;
```

Order/result consistency checks:

```sql
SELECT
  orders.shopify_order_id,
  order_sources.code AS order_source,
  result_sources.code AS result_source,
  order_methods.code AS order_method,
  result_methods.code AS result_method,
  orders.attribution_confidence_score AS order_score,
  results.confidence_score AS result_score,
  orders.last_attribution_run_at AS order_run_at,
  results.last_attribution_run_at AS result_run_at
FROM shopify_orders orders
JOIN attribution_results results
  ON results.shopify_order_id = orders.shopify_order_id
LEFT JOIN attribution_sources order_sources
  ON order_sources.id = orders.attribution_source_id
LEFT JOIN attribution_sources result_sources
  ON result_sources.id = results.attribution_source_id
LEFT JOIN matching_methods order_methods
  ON order_methods.id = orders.matching_method_id
LEFT JOIN matching_methods result_methods
  ON result_methods.id = results.matching_method_id
WHERE order_sources.code IS DISTINCT FROM result_sources.code
   OR order_methods.code IS DISTINCT FROM result_methods.code
   OR orders.attribution_confidence_score IS DISTINCT FROM results.confidence_score
   OR orders.attribution_confidence_contract_version IS DISTINCT FROM results.confidence_contract_version
ORDER BY GREATEST(
  COALESCE(orders.last_attribution_run_at, '-infinity'::timestamptz),
  COALESCE(results.last_attribution_run_at, '-infinity'::timestamptz)
) DESC
LIMIT 100;
```

Backfill queue and stale processing check:

```sql
SELECT
  status,
  COUNT(*) AS jobs,
  MIN(submitted_at) AS oldest_submitted_at,
  MIN(last_heartbeat_at) FILTER (WHERE status = 'processing') AS oldest_processing_heartbeat
FROM order_attribution_backfill_runs
WHERE submitted_at >= now() - interval '7 days'
GROUP BY status
ORDER BY status;
```

```sql
SELECT
  id,
  submitted_at,
  started_at,
  last_heartbeat_at,
  options,
  error_code,
  error_message
FROM order_attribution_backfill_runs
WHERE status = 'processing'
  AND COALESCE(last_heartbeat_at, started_at, submitted_at) < now() - interval '15 minutes'
ORDER BY COALESCE(last_heartbeat_at, started_at, submitted_at) ASC
LIMIT 50;
```

## API Examples

Set `API_BASE_URL` to the environment being inspected and use an admin bearer token for admin routes.

Reporting order metadata:

```bash
curl -sS "$API_BASE_URL/api/reporting/orders?startDate=2026-05-01&endDate=2026-05-31&attributionModel=last_touch&limit=50" \
  -H "Authorization: Bearer $ROAS_RADAR_TOKEN"
```

Single order metadata:

```bash
curl -sS "$API_BASE_URL/api/reporting/orders/6123456789012" \
  -H "Authorization: Bearer $ROAS_RADAR_TOKEN"
```

Attribution result query:

```bash
curl -sS "$API_BASE_URL/api/attribution/results?modelKey=last_touch&startDate=2026-05-01&endDate=2026-05-31&orderId=6123456789012&limit=50" \
  -H "Authorization: Bearer $ROAS_RADAR_TOKEN"
```

Per-order explainability:

```bash
curl -sS "$API_BASE_URL/api/attribution/orders/6123456789012/explainability?modelKey=last_touch" \
  -H "Authorization: Bearer $ROAS_RADAR_TOKEN"
```

Admin order backfill enqueue, dry run first:

```bash
curl -sS -X POST "$API_BASE_URL/api/admin/attribution/orders/backfill" \
  -H "Authorization: Bearer $ROAS_RADAR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "startDate": "2026-05-01",
    "endDate": "2026-05-07",
    "dryRun": true,
    "limit": 500,
    "webOrdersOnly": true,
    "skipShopifyWriteback": false
  }'
```

Admin order backfill status:

```bash
curl -sS "$API_BASE_URL/api/admin/attribution/orders/backfill/$JOB_ID" \
  -H "Authorization: Bearer $ROAS_RADAR_ADMIN_TOKEN"
```

If the dry run reports expected `scanned`, `recovered`, and `unrecoverable` counts, enqueue the same window with `"dryRun": false`.

## Backfill and Replay Procedures

Use the smallest date window that explains the incident.

1. If Shopify orders are missing locally, import Shopify orders for the window with `/api/admin/shopify/orders/backfill`.
2. If orders exist but Shopify web orders are unattributed, recover Shopify attribution hints with `/api/admin/shopify/orders/recover-attribution`.
3. Queue `/api/admin/attribution/orders/backfill` with `dryRun: true`.
4. Review the job status response and Cloud Logging event `order_attribution_backfill_job_lifecycle`.
5. Queue the same request with `dryRun: false` only after the dry run is understood.
6. Run `npm run attribution:backfill-confidence -- --dry-run` if the winner is correct but source IDs, method IDs, confidence score, contract version, or run timestamps are stale.
7. Run the write-enabled confidence backfill and re-run the SQL consistency checks.

For replayed windows that include writeback, leave `skipShopifyWriteback` as `false` unless the incident is limited to internal reporting metadata. If writeback is disabled, record that decision in the incident notes because Shopify order notes may remain behind internal attribution state.

## Known Failure Modes

- Lookup drift: rows point to inactive, missing, or wrong source/method IDs after a manual data patch.
- Score drift: a resolver path writes a score not listed in the `v1` contract.
- Label drift: legacy rows expose `confidence_label` that does not match the score table.
- Fallback over-selection: Shopify hint or GA4 fallback volume rises because deterministic capture, checkout token propagation, or identity stitching failed upstream.
- GA4 precedence regression: GA4 fallback replaces Shopify hint fallback, which violates resolver precedence.
- Null-session confusion: Shopify hint and GA4 fallback rows can be valid with `session_id = null`; deterministic rows should not rely on that exception.
- Stale attribution runs: `last_attribution_run_at` does not advance after winner fingerprint fields change.
- Split-brain order/result state: `shopify_orders` and `attribution_results` disagree after a partial worker failure or interrupted backfill.
- Backfill lock staleness: `order_attribution_backfill_runs.status = 'processing'` with no heartbeat for more than 15 minutes.

## On-Call Actions

- Page the attribution owner if confidence drift affects production reporting for more than one storefront or more than one hour of orders.
- Pause broad non-dry-run backfills if consistency mismatches increase after a write-enabled run starts.
- Prefer replaying one narrow date window before widening. Keep `limit` at or below `500` until the failure mode is understood.
- Escalate immediately if deterministic high-confidence volume drops while fallback or unattributed volume rises across live orders.
- Open a data quality follow-up when confidence metadata is repaired but historical reporting aggregates need refresh.
- Link the incident to the exact SQL output, API examples, job ID, dry-run report, and final write-enabled report.
