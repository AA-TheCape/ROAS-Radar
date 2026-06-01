# Operational Attribution Contracts

This document is the operator-facing contract for the attribution pipeline. It summarizes the implemented precedence, writeback, recovery, retention, and rollout rules and points to the deeper specs that must stay aligned with code and tests.

## Source documents

- `docs/attribution-schema-v1.md`
- `docs/confidence-scoring-contract-v1.md`
- `docs/last-non-direct-touch-approval-matrix.md`
- `docs/ga4-fallback-attribution-contract-v1.md`
- `docs/runbooks/attribution-completeness.md`
- `docs/runbooks/attribution-confidence-scoring.md`
- `docs/runbooks/ga4-fallback-rollout.md`

## Resolver precedence

Primary order attribution must evaluate candidate paths in this order:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. `customer_identity`
5. `shopify_hint_fallback`
6. `ga4_fallback`
7. `unattributed`

The resolver must never let GA4 fallback override a deterministic winner or an approved Shopify hint fallback winner.

## Provenance and persistence

- Every winning attribution outcome must expose `match_source`.
- Every persisted order-level winner must also resolve to active lookup rows in `attribution_sources` and `matching_methods`.
- Deterministic winners keep a resolved first-party `session_id`.
- Shopify synthetic fallback and GA4 fallback keep `session_id = null`.
- GA4 fallback writes must use `match_source = 'ga4_fallback'` and `attribution_reason = 'ga4_fallback_derived'`.
- Unattributed outcomes must remain explicit with `match_source = 'unattributed'` and confidence `0.00`.

These rules apply across:

- `attribution_results`
- `attribution_order_credits`
- `shopify_orders.attribution_snapshot`
- reporting and API response shapes that expose the winner or timeline

Lookup tables are migration-owned contract data:

- `attribution_sources` stores stable source IDs and codes for winner provenance.
- `matching_methods` stores stable method IDs and codes for how the winner was matched.
- IDs are persisted in `shopify_orders` and `attribution_results`; API readers expose codes, not numeric IDs.
- Codes must remain lowercase snake case and must not be repointed to different semantics without a new contract and backfill plan.

The attribution worker writes the same winner fingerprint to `shopify_orders` and `attribution_results`:

- attribution source ID and code
- matching method ID and code
- confidence score and confidence contract version
- `last_attribution_run_at`
- winner session, dimensions, `match_source`, and `attribution_reason`

`last_attribution_run_at` is the freshness marker for persisted attribution metadata. It must advance when a run changes any winner fingerprint field, including lookup source, matching method, confidence score, winner dimensions, winner session, `match_source`, or attribution reason. It should not be advanced by unrelated Shopify order updates that do not change attribution output.

## Confidence contract

The canonical versioned confidence scoring contract is `docs/confidence-scoring-contract-v1.md`.

- deterministic exact matches: `high`
- deterministic stitched identity: `medium`
- Shopify synthetic fallback and GA4 fallback: `low`
- unattributed: `none`

GA4 fallback is capped below the Shopify synthetic fallback scores:

- `0.35` with a supported click id
- `0.25` with canonical UTMs only

Runtime writers must persist confidence consistently:

- `attribution_results.confidence_score`
- `attribution_results.confidence_label`
- `attribution_results.confidence_contract_version`
- `attribution_order_credits.confidence_label`
- `attribution_order_credits.confidence_contract_version`
- `shopify_orders.attribution_confidence_score`
- `shopify_orders.attribution_confidence_contract_version`

Reporting and attribution APIs expose the order-level score as `confidenceScore`. Confidence explains match strength only; it must not replace attribution tier, resolver precedence, model selection, revenue, or campaign-performance metrics.

## Recompute and backfill

Recompute persisted attribution metadata when any of these inputs change:

- resolver precedence or winner selection
- Shopify hint recovery eligibility
- GA4 fallback eligibility or rollout mode
- identity stitching output
- click-ID detection or canonical UTM normalization
- attribution source or matching method lookup mappings
- confidence score, label, or contract version
- a stale or failed attribution job later writes a corrected winner

Use this repair order:

1. If Shopify orders are missing, import or replay the Shopify order window.
2. If Shopify hint fields are missing, recover Shopify attribution hints.
3. If winners are wrong, run order attribution backfill with `dryRun: true`, then run the same window write-enabled after review.
4. If winners are correct but lookup IDs, confidence score, contract version, or `last_attribution_run_at` are stale, run `npm run attribution:backfill-confidence -- --dry-run`.
5. Run the write-enabled confidence backfill only after the dry run output is understood.
6. Re-check order/result consistency and refresh aggregates when the incident affected reporting windows.

Use `docs/runbooks/attribution-confidence-scoring.md` for SQL checks, API examples, confidence backfill commands, and stale `lastAttributionRunAt` triage.

## Shopify writeback and recovery

- Writeback is downstream of attribution resolution, not a source of truth for the winner.
- Recovery order is fixed: import Shopify orders, recover Shopify hints, then run attribution backfill if gaps remain.
- Write-enabled backfill runs must be preceded by a dry run for the same date window.

Use `docs/runbooks/attribution-completeness.md` for the step-by-step operator procedure.

## GA4 rollout mode

`GA4_FALLBACK_ROLLOUT_MODE` gates live behavior:

- `off`: do not apply GA4 fallback
- `shadow`: evaluate GA4 fallback and store shadow comparisons without changing live attribution
- `on`: allow GA4 fallback when it is otherwise eligible

Production enablement still requires explicit operator approval after the shadow report passes its thresholds. Use `docs/runbooks/ga4-fallback-rollout.md`.

## Queueing, retries, and dead letters

- Attribution worker jobs are processed from the backend queue and must preserve idempotency.
- GA4 hourly ingestion retries are tracked per hour window inside `ga4_bigquery_hourly_jobs`.
- Repeated unrecoverable failures move into `event_dead_letters`.
- Replay only after fixing the underlying cause and preserving the original date window.

## Retention

- session attribution capture rows are pruned by `retained_until`
- rows referenced by `order_attribution_links` must be preserved
- GA4 fallback candidate retention follows the same operational cleanup discipline

Use `docs/database-operations.md` for the retention and query-plan checks that protect these tables.
