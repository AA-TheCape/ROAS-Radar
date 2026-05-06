# Operational Attribution Contracts

This document is the operator-facing contract for the attribution pipeline. It summarizes the implemented precedence, writeback, recovery, retention, and rollout rules and points to the deeper specs that must stay aligned with code and tests.

## Source documents

- `docs/attribution-schema-v1.md`
- `docs/last-non-direct-touch-approval-matrix.md`
- `docs/ga4-fallback-attribution-contract-v1.md`
- `docs/runbooks/attribution-completeness.md`
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
- Deterministic winners keep a resolved first-party `session_id`.
- Shopify synthetic fallback and GA4 fallback keep `session_id = null`.
- GA4 fallback writes must use `match_source = 'ga4_fallback'` and `attribution_reason = 'ga4_fallback_derived'`.
- Unattributed outcomes must remain explicit with `match_source = 'unattributed'` and confidence `0.00`.

These rules apply across:

- `attribution_results`
- `attribution_order_credits`
- `shopify_orders.attribution_snapshot`
- reporting and API response shapes that expose the winner or timeline

## Confidence contract

- deterministic exact matches: `high`
- deterministic stitched identity: `medium`
- Shopify synthetic fallback and GA4 fallback: `low`
- unattributed: `none`

GA4 fallback is capped below the Shopify synthetic fallback scores:

- `0.35` with a supported click id
- `0.25` with canonical UTMs only

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
