# Operational Attribution Contracts

This document is the operator-facing contract that connects the implemented behavior in:

- `src/modules/attribution/index.ts`
- `src/modules/attribution/resolver.ts`
- `src/modules/shopify/writeback.ts`
- `src/modules/tracking/retention.ts`

Use it together with:

- `docs/attribution-schema-v1.md` for field names, normalization, and canonical Shopify keys
- `docs/analytics-playbook.md` for reporting interpretation
- `docs/ga4-fallback-attribution-contract-v1.md` for the approved GA4 fallback eligibility, precedence, and persistence contract
- `docs/runbooks/attribution-completeness.md` for incidents affecting capture, writeback, or resolver quality
- `docs/runbooks/ingestion-failures.md` for `/track` and Shopify webhook ingestion failures
- `docs/runbooks/attribution-worker-backlog.md` for stuck attribution or writeback queues

## Scope

This contract covers:

- deterministic order-to-session resolver precedence
- fallback precedence across Shopify hints, GA4, and unattributed outcomes
- last-non-direct primary winner semantics
- Shopify order note-attribute writeback behavior
- retry, dead-letter, and reconciliation semantics
- 30-day session-attribution retention and preservation rules

It does not replace the schema contract or reporting docs. It explains how the operational pieces behave together.

## Resolver Contract

The attribution worker in `src/modules/attribution/index.ts` collects deterministic candidates from every supported evidence source for the order instead of stopping at the first match:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. stitched `identity_journey`
5. `shopify_hint_fallback`
6. `ga4_fallback`
7. `unattributed`

Current attribution window:

- `7` days
- candidates after the order timestamp are excluded

### Deterministic precedence

`src/modules/attribution/resolver.ts` uses this precedence whenever two candidates tie on timestamp or the same session is visible through multiple evidence sources:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. `identity_journey`

The same precedence drives:

- candidate deduplication for duplicate `sessionId`
- same-timestamp primary-winner selection
- confidence score assignment

Current confidence semantics:

- `1.00`: `landing_session_id` or `checkout_token`
- `0.90`: `cart_token`
- `0.60`: stitched `identity_journey` fallback
- `0.55`: Shopify hint fallback with a supported click ID
- `0.40`: Shopify hint fallback with canonical UTMs and no click ID
- `0.35`: GA4 fallback with a supported click ID
- `0.25`: GA4 fallback with canonical UTMs and no click ID
- `0.00`: no deterministic winner

Current confidence labels:

- `high`: `1.00`, `0.90`
- `medium`: `0.60`
- `low`: `0.55`, `0.40`, `0.35`, `0.25`
- `none`: `0.00`

### Last Non-Direct winner behavior

The primary winner written to `attribution_results` follows the implemented last-non-direct contract:

- partition deterministic candidates into non-direct and direct pools
- if any non-direct candidate exists, ignore all direct candidates for primary-winner selection
- choose the winner by latest `occurredAt`
- if timestamps tie, prefer stronger deterministic source precedence
- if that still ties, prefer the candidate with a click ID
- if still tied, prefer the lexicographically smaller `sessionId`

Classification rules:

- a touch is direct only when source, medium, campaign, content, term, and click ID are all absent
- a click-ID-only touch is non-direct even when all UTM fields are missing
- a later direct revisit stays in history but does not overwrite an earlier in-window non-direct touch

Related references:

- `docs/analytics-playbook.md`
- `docs/ga4-fallback-attribution-contract-v1.md`
- `docs/last-non-direct-touch-approval-matrix.md`
- `docs/visitor-identity-stitching.md`

Rollout note:

- the attribution worker still reads the dual-written `customer_identity_id` compatibility alias internally in some paths
- operators should treat `identity_journey_id` as the canonical contract and `customer_identity_id` as transitional storage only

## Fallback Precedence Contract

When deterministic resolution produces no winner, the resolver may continue through approved fallback steps in this order:

1. Shopify synthetic hint fallback
2. GA4 fallback
3. unattributed

Hard rules:

- Shopify hint fallback is recovery-only and must never override a deterministic winner
- GA4 fallback is eligible only when deterministic resolution has no winner and Shopify hint fallback has no match
- GA4 fallback may replace an otherwise unattributed outcome, but it must not replace deterministic or Shopify hint outcomes

Persistence and provenance rules:

- downstream-facing surfaces must persist first-class provenance in `match_source`
- `match_source` values for current primary outcomes are `landing_session_id`, `checkout_token`, `cart_token`, `customer_identity`, `shopify_hint_fallback`, `ga4_fallback`, and `unattributed`
- `attribution_reason` explains why the system chose the outcome, while `match_source` explains where the winning match came from
- fallback outcomes can keep `session_id = null` when no first-party ROAS Radar session was resolved

GA4-specific behavior is governed by `docs/ga4-fallback-attribution-contract-v1.md`.

Operational note:

- repeated GA4 candidate ingestion must deduplicate on the stable session candidate key instead of enrichment-only fields
- GA4 campaign metadata is reconciled as one bundle, so operators should not expect a stored fallback row to mix `campaign` from one export with `content` from another

## Shopify Writeback Contract

`src/modules/shopify/writeback.ts` is responsible for durable Shopify order note-attribute writeback after attribution or reconciliation identifies a canonical session snapshot.

### Canonical Shopify note attribute keys

Writeback emits the canonical unprefixed keys defined by `packages/attribution-schema/index.ts`:

- `schema_version`
- `roas_radar_session_id`
- `landing_url`
- `referrer_url`
- `page_url`
- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`
- `gclid`
- `gbraid`
- `wbraid`
- `fbclid`
- `ttclid`
- `msclkid`

Writer rules:

- `schema_version` and `roas_radar_session_id` are always included
- nullable fields are omitted when the canonical value is `null`
- URLs are normalized through the shared schema helper
- UTM values are normalized and lowercased
- click IDs are normalized but preserve case

### Value source precedence

Writeback composes canonical attributes from the resolved session and its latest captured touch:

- session identity snapshot in `session_attribution_identities` provides `landing_url`, first-touch referrer, initial UTMs, and initial click IDs
- latest event in `session_attribution_touch_events` provides `page_url`, event referrer, event-level UTMs, and event-level click IDs
- order resolution uses `COALESCE(o.landing_session_id, attribution.session_id)` as the durable session source

Field precedence during payload assembly:

- `referrer_url`: latest touch-event referrer, then first-touch identity referrer
- `utm_*`: latest touch-event value, then initial identity value
- click IDs: latest touch-event value, then initial identity value

### Queue contract

`enqueueShopifyOrderWriteback(...)` writes to `shopify_order_writeback_jobs`.

Queue behavior:

- queue key is one job per Shopify order: `shopify_order:<shopify_order_id>`
- enqueue is idempotent at the queue-key level
- re-enqueue resets `completed` and `failed` rows back to `pending`
- retryable in-flight jobs keep their existing lifecycle instead of creating duplicates

`processShopifyOrderWritebackQueue(...)` claims rows with:

- status `pending` or `retry`
- `available_at <= now()`
- `FOR UPDATE SKIP LOCKED`

It marks claimed rows as `processing`, applies writeback, then finalizes as:

- `completed` when Shopify accepts the payload
- `completed` with `last_error = canonical_attributes_not_available` when no canonical attribute row can be built
- `retry` for retryable failures
- `failed` plus dead-letter entry for terminal failures

### Retry and dead-letter semantics

Retry logic is exponential backoff:

- attempt 1 retry delay: `1000ms`
- attempt 2 retry delay: `2000ms`
- doubles up to a max of `60000ms`

Current defaults from `src/config/env.ts`:

- `SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE = 25`
- `SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES = 5`

A failure is treated as retryable when:

- the raised error is explicitly `retryable`
- or the status code is `429` or any `5xx`
- otherwise the fallback assumption is retryable unless the error marks itself non-retryable

A job is dead-lettered when:

- the error is non-retryable
- or the next attempt would reach `SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES`

Dead-letter behavior:

- event type: `shopify_writeback_failed`
- source table: `shopify_order_writeback_jobs`
- stored payload includes `jobId`, `shopifyOrderId`, `requestedReason`, `attempts`, and `workerId`
- the queue row is marked `failed` and stamped with `dead_lettered_at`

### Skip and terminal-failure conditions

Writeback distinguishes between true skips and invalid canonical state.

Skips:

- no Shopify order row or no canonical attribute source row found
- no canonical attributes returned from the source query
- queue row is completed with `canonical_attributes_not_available`

Terminal failures:

- missing resolved `roas_radar_session_id`
- no `landing_url` and no `page_url` for the resolved session
- writeback is not configured because Shopify credentials are unavailable
- no active Shopify installation is available
- Shopify rejects the request with a non-retryable response such as `4xx` other than `429`

Operational implication:

- missing session capture is not silently retried forever
- malformed or incomplete canonical state is surfaced through failed queue rows and dead letters

### Reconciliation contract

`reconcileRecentShopifyOrderAttributes(...)` scans recent Shopify orders and requeues only the rows that are missing canonical note attributes.

Current defaults:

- `SHOPIFY_RECONCILIATION_ENABLED = true`
- `SHOPIFY_RECONCILIATION_LOOKBACK_DAYS = 7`
- `SHOPIFY_RECONCILIATION_BATCH_SIZE = 100`

Selection rules:

- scan recent orders by `COALESCE(processed_at, created_at_shopify, ingested_at)`
- only inspect orders inside the lookback window
- compare current note attributes from `raw_payload` to the expected canonical writeback payload

Per-order outcomes:

- `upToDateOrders`: every expected canonical key/value already matches
- `ordersNeedingWriteback`: at least one canonical attribute is missing or stale
- `requeuedOrders`: a missing or stale order was successfully enqueued
- `skippedOrders`: order has no `landing_session_id`
- `failedOrders`: expected canonical attributes could not be built or reconciliation hit an exception

Reconciliation requested reason:

- `reconciliation_missing_canonical_attributes`

Operational implication:

- reconciliation repairs recent writeback gaps idempotently
- it does not rewrite already-correct orders
- it does not invent writeback for orders that still lack a resolved session anchor

## Retention Contract

`src/modules/tracking/retention.ts` prunes session-level capture storage while preserving rows that still support attributed orders.

Current default:

- `SESSION_ATTRIBUTION_RETENTION_DAYS = 30`

The retention job computes a cutoff at `asOf - 30 days` and only deletes rows strictly older than that cutoff.

Protected rows are preserved when their `roas_radar_session_id` is still referenced by `order_attribution_links`.

### Deletion order

Each retention batch runs in this order:

1. delete expired rows from `session_attribution_touch_events`
2. delete expired rows from `session_attribution_identities` only when no touch events remain for that session

The job uses batched deletes with `FOR UPDATE SKIP LOCKED`.

Current defaults:

- batch size: `100`
- max batches per run: `50`

### Preservation rules

Rows are skipped when:

- `retained_until` is exactly on the cutoff timestamp
- `retained_until` is newer than the cutoff
- the session is referenced by `order_attribution_links`
- an identity row still has remaining touch events for the same session

Operational implication:

- attributed orders keep the backing session evidence needed for audits and reconciliation
- 30-day cleanup affects capture storage, not order-attribution links
- identities are never deleted before their dependent touch events are gone

Related reference:

- `docs/database-operations.md`

## Incident Routing

Use the runbooks based on the failure mode:

- capture drop, missing session IDs, dual-write mismatch, writeback success degradation, or resolver unattributed spikes:
  `docs/runbooks/attribution-completeness.md`
- `/track` failures, rejected webhooks, payload/schema issues, or secret mismatch:
  `docs/runbooks/ingestion-failures.md`
- growing attribution backlog, stale locks, repeated retries, or queue pressure:
  `docs/runbooks/attribution-worker-backlog.md`

Useful log events called out across these contracts:

- `attribution_capture_observed`
- `tracking_dual_write_consistency`
- `shopify_writeback_observed`
- `attribution_resolver_outcome`
- `attribution_backlog_snapshot`
- `attribution_queue_run`
- `attribution_job_failed`
- `session_attribution_retention_batch_completed`
- `session_attribution_retention_completed`
- `session_attribution_retention_failed`

## Change Discipline

Treat the behaviors in this document as contract-level semantics.

A coordinated docs, code, and test update is required when changing:

- resolver source precedence
- direct vs non-direct classification
- winner tie-breakers
- canonical Shopify writeback keys
- retryability or dead-letter thresholds
- reconciliation lookback semantics
- retention duration or preservation rules
