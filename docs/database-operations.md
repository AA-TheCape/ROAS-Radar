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

## Campaign Metadata Lookup

Use `ad_platform_entity_metadata` as the canonical latest-name resolution table for Google Ads and Meta entity labels.

Do not use these reporting or raw-source tables as the metadata source of truth:

- `meta_ads_raw_spend_records`
- `google_ads_raw_spend_records`
- `meta_ads_daily_spend`
- `google_ads_daily_spend`

Preferred resolution query shape:

```sql
SELECT latest_name, last_seen_at, updated_at
FROM ad_platform_entity_metadata
WHERE platform = $1
  AND account_id = $2
  AND entity_type = $3
  AND entity_id = $4
  AND tenant_id IS NOT DISTINCT FROM $5
  AND workspace_id IS NOT DISTINCT FROM $6;
```

The uniqueness contract is one row per `(platform, account_id, entity_type, entity_id, tenant_id, workspace_id)` scope, with `NULL` tenant and workspace treated as one shared unscoped namespace.

## Campaign Metadata Query Plan Verification

Use the staging-safe plan check before approving changes to metadata resolution lookups:

```bash
npm run db:verify-campaign-metadata-query-plans
```

The script seeds representative rows inside a transaction, runs `EXPLAIN (FORMAT JSON)` against the exact-match lookup paths, asserts that the metadata lookup indexes are used, and then rolls the data back.

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

## Retention Cleanup

Operational pruning runs through the scheduled `session-attribution:retention` Cloud Run job.

The cleanup contract is:

1. delete expired `session_attribution_touch_events` rows in batches
2. delete expired `session_attribution_identities` rows in batches
3. skip rows whose `roas_radar_session_id` is still referenced by `order_attribution_links`

`order_attribution_links` rows are not pruned by the 30-day session cleanup job.
