# Recovery Job Framework

The recovery job framework standardizes automatic backfill and repair jobs that re-read durable source records, rebuild attribution inputs, and apply missing side effects idempotently. It is the contract for manual runs, scheduled jobs, and automatic recovery triggered by monitoring.

## Contract Files

Shared Zod contracts live in `packages/attribution-schema/recovery-jobs.ts`. Committed JSON Schema artifacts live in `docs/json-schema/`:

- `recovery-job-request-v1.schema.json`
- `recovery-job-report-v1.schema.json`
- `shopify-raw-payload-snapshot-v1.schema.json`
- `shopify-attribution-hint-v1.schema.json`
- `ga4-enrichment-fields-v1.schema.json`
- `campaign-metadata-refresh-payload-v1.schema.json`

All recovery contracts use `schemaVersion: 1` and ISO-8601 timestamps with explicit timezone offsets.

## Source Precedence

Recovery jobs must preserve this source precedence rule:

1. Shopify
2. GA4
3. ad platforms

In compact form: Shopify > GA4 > ad platforms.

Shopify raw order payloads and Shopify attribution hints are the highest-priority recovery source because they are attached to the commerce event. GA4 enrichment can fill gaps only when Shopify cannot provide the field. Ad platform metadata refreshes are descriptive enrichment for campaign names and hierarchy, not attribution authority, and must not override Shopify or GA4-derived attribution fields.

`RecoveryJobReportV1.sourcePrecedence` must be exactly `["shopify", "ga4", "ad_platforms"]`. Source-specific payload schemas also carry precedence ranks:

- Shopify attribution hints: `sourcePrecedenceRank: 1`
- GA4 enrichment fields: `sourcePrecedenceRank: 2`
- campaign metadata refresh payloads: `sourcePrecedenceRank: 3`

## Job Lifecycle

Recovery runs follow the registry-backed lifecycle already implemented by `src/modules/recovery`:

1. Create or reuse a run by job type, scope, date range, and idempotency key.
2. Claim a queued run with a worker id.
3. Page through candidate records with a checkpoint.
4. Upsert per-record recovery state using a stable record key and source fingerprint.
5. Process each record with retry-aware failure tracking.
6. Apply side effects only when `dryRun` is false.
7. Finalize with counters, artifacts, failures, and the explicit source precedence vector.

Dry runs are the default for new requests. Write-enabled jobs must keep idempotency keys stable across retries and must suppress duplicate side effects through `sideEffectKey` or an equivalent downstream uniqueness constraint.

## Payload Responsibilities

`RecoveryJobRequestV1` is the common enqueue shape for manual, scheduled, and automatic runs. `inputParameters` is reserved for job-specific filters such as shop domain, campaign ids, GA4 property id, or worker tuning.

`RecoveryJobReportV1` is the common completion artifact. It records lifecycle status, counters, emitted artifacts, failures, and the source precedence rule used by the run.

`ShopifyRawPayloadSnapshotV1` captures immutable Shopify order payload snapshots plus normalized fields used by recovery workers. Store the raw payload hash with every snapshot so replay can detect upstream changes.

`ShopifyAttributionHintV1` captures attribution hints extracted from Shopify order fields such as note attributes, landing site, attributes arrays, or client details. These hints outrank GA4 and ad platform enrichment.

`Ga4EnrichmentFieldsV1` captures GA4-derived traffic fields for an order or transaction. GA4 values are fallback enrichment and must not replace non-null Shopify fields.

`CampaignMetadataRefreshPayloadV1` scopes campaign metadata refreshes for Google Ads and Meta Ads. These payloads may update campaign/adset/ad display metadata and freshness state, but they must not change attribution winners.

## Automatic Recovery Triggers

Automatic jobs should be created from monitoring signals only when the failing condition is bounded by a date window and a deterministic scope key. Examples:

- Shopify attribution capture rate drops below threshold.
- GA4 ingestion lag closes and previously unattributed orders are eligible for fallback enrichment.
- Campaign metadata freshness breaches for a bounded platform/account set.
- Recovery record failures are retryable after a downstream API outage.

Automatic jobs must include the alert id or scheduler execution id in `inputParameters` so repeated signals collapse into the same idempotent run instead of creating overlapping work.

## Failure Handling

Failures should identify `recordType`, `recordKey`, `code`, `message`, `sourceSystem`, and `retryable`. Retryable failures stay tied to the same recovery record and retain attempt counts. Non-retryable failures should be counted in the report and left available for operator review.

When multiple sources disagree during recovery, workers must keep the winning value from the highest-precedence source and report the lower-precedence candidate as suppressed, skipped, or informational metadata.
