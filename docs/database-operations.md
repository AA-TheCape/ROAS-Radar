# Database Operations

## Migrations

Apply schema changes with:

```bash
npm run db:migrate
```

Validate that migrations remain idempotent with:

```bash
npm run db:migrate:check
```

## Raw Payload Querying

For raw-source retention tables, lookup queries must prefer metadata columns over JSONB predicates.

Use these columns first:

- Shopify webhook receipts: `payload_source`, `payload_external_id`, `received_at`
- Shopify orders: `payload_source`, `payload_external_id`, `payload_received_at`
- Meta Ads connection raw account data: `raw_account_source`, `raw_account_external_id`, `raw_account_received_at`
- Meta Ads raw spend rows: `payload_source`, `payload_external_id`, `payload_received_at`
- Google Ads connection raw customer data: `raw_customer_source`, `raw_customer_external_id`, `raw_customer_received_at`
- Google Ads raw spend rows: `payload_source`, `payload_external_id`, `payload_received_at`

Do not use `meta_ads_daily_spend` or `google_ads_daily_spend` as raw-source lookup surfaces. Those tables are normalized projections for reporting and may legitimately omit malformed upstream rows that were still retained in the raw spend tables.

Preferred query shape:

```sql
SELECT id, raw_payload
FROM meta_ads_raw_spend_records
WHERE payload_source = $1
  AND payload_external_id = $2
ORDER BY payload_received_at DESC
LIMIT 20;
```

Avoid this query shape in application code and operational runbooks unless no metadata column exists yet:

```sql
SELECT id, raw_payload
FROM meta_ads_raw_spend_records
WHERE raw_payload ->> 'campaign_id' = $1;
```

The goal is to avoid wide JSONB GIN indexes and repeated `raw_payload ->>` scans on high-write ingestion tables.

## Raw Payload Index Verification

Use the staging-safe plan check before approving changes to raw-payload lookup paths:

```bash
npm run db:verify-raw-payload-query-plans
```

The script seeds representative rows inside a transaction, runs `EXPLAIN (FORMAT JSON)` against the supported lookup patterns, asserts that the targeted lookup indexes are used, and then rolls the data back.

## Staging Targets

Treat these as the minimum acceptance targets for raw-payload lookups under expected staging volume:

- point lookup by `source + external_id`: p95 under 150 ms
- latest-20 lookup by `source + external_id ORDER BY received_at DESC`: p95 under 200 ms
- ingestion inserts into raw spend tables: no sustained regression beyond 10% versus the pre-index baseline for the same batch size

Use `pg_stat_statements` plus `EXPLAIN (ANALYZE, BUFFERS)` in staging to measure those targets. If a lookup misses its target, fix the query pattern first before adding any new JSONB expression or GIN index.

## Attribution Engine V1 Index Verification

Use the staging-safe plan check before approving changes to the new attribution-engine storage:

```bash
npm run db:verify-attribution-v1-query-plans
```

The script seeds representative attribution runs, normalized orders, touchpoints, summaries, credits, and explain rows inside a transaction, runs `EXPLAIN (FORMAT JSON)` against the expected query patterns, asserts that the targeted indexes are used, and then rolls the data back.

Required lookup patterns for the v1 schema:

- touchpoint lookup by `session_id ORDER BY touchpoint_occurred_at_utc DESC`
- summary lookup by `model_key + order_occurred_at_utc` window
- reporting lookup by `model_key + source + medium + campaign ORDER BY occurred_at_utc DESC`
- explainability lookup by `run_id + order_id + explain_stage ORDER BY created_at_utc DESC`

## Attribution Engine V1 Staging Targets

Treat these as the minimum acceptance targets for the new attribution-engine tables under expected staging volume:

- session-based touchpoint lookup: p95 under 150 ms
- model summary window query over the last 30 days: p95 under 200 ms
- model credit reporting filter by `model_key + source + medium + campaign`: p95 under 250 ms
- explainability lookup for one run and order: p95 under 150 ms

Use `pg_stat_statements` plus `EXPLAIN (ANALYZE, BUFFERS)` in staging to measure those targets before widening any index or adding derived reporting tables.

## Attribution Engine V1 Retention

The new run-scoped attribution tables use retention rather than table partitioning in v1:

- `attribution_runs`, `attribution_order_inputs`, `attribution_model_summaries`, and `attribution_model_credits` default to `400 days`
- `attribution_touchpoint_inputs` and `attribution_explain_records` default to `180 days`

Any pruning job should delete expired rows in child-table order:

1. `attribution_explain_records`
2. `attribution_model_credits`
3. `attribution_model_summaries`
4. `attribution_touchpoint_inputs`
5. `attribution_order_inputs`
6. `attribution_runs`

## Retention Cleanup

Operational pruning runs through the scheduled `session-attribution:retention` Cloud Run job.

The cleanup contract is:

1. delete expired `session_attribution_touch_events` rows in batches
2. delete expired `session_attribution_identities` rows in batches
3. skip rows whose `roas_radar_session_id` is still referenced by `order_attribution_links`

`order_attribution_links` rows are not pruned by the 30-day session cleanup job.
