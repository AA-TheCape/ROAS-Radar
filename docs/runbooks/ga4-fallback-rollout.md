# GA4 Fallback Rollout

This runbook defines the staged cutover for GA4 fallback attribution.

## Rollout Flag

Set `GA4_FALLBACK_ROLLOUT_MODE` on the attribution worker and API service to one of:

- `off`: GA4 fallback is computed only when explicitly requested in code paths that bypass the worker. Normal order attribution does not apply or record GA4 shadow comparisons.
- `shadow`: the worker computes the GA4 fallback candidate, keeps the live attribution result unchanged, and writes one comparison row per processed order into `ga4_fallback_shadow_comparisons`.
- `on`: the worker applies GA4 fallback when it is eligible under `docs/ga4-fallback-attribution-contract-v1.md`.

Default mode is `off`.

## Shadow Report

Use:

- `GET /api/admin/attribution/ga4-fallback/shadow-report?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD`

The report returns:

- `evaluatedOrders`
- `shadowGa4FallbackOrders`
- `changedOrders`
- current versus shadow attributed order counts
- current versus shadow attributed revenue
- delta values for both metrics
- acceptance-threshold status and the explicit-approval gate

Shadow rows are keyed by `shopify_order_id`, so the report always reflects the latest evaluation for each order in the requested window.

## Acceptance Thresholds

The report evaluates the shadow data against these runtime thresholds:

- `GA4_FALLBACK_SHADOW_MIN_EVALUATED_ORDERS`
- `GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_ORDER_DELTA_RATE`
- `GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_REVENUE_DELTA_RATE`

Defaults:

- minimum evaluated orders: `100`
- max attributed-order delta rate: `0.02`
- max attributed-revenue delta rate: `0.02`

Passing the thresholds changes report status to `pending_explicit_approval`. It does not enable GA4 fallback automatically.

## Staged Cutover

1. Start in `off` after deploying the schema and code changes.
2. Move staging to `shadow`.
3. Validate that `shadowGa4FallbackOrders` is non-zero for known web-order cohorts and that order/revenue deltas stay within thresholds.
4. Move production to `shadow`.
5. Review the report for a representative window large enough to satisfy `GA4_FALLBACK_SHADOW_MIN_EVALUATED_ORDERS`.
6. Record explicit approval from the attribution owner before changing production to `on`.
7. Change production to `on`.
8. Continue monitoring the same report after cutover to confirm expected GA4 fallback volume and stable metric deltas.

## Approval Requirement

Production enablement requires explicit operator approval even when the thresholds pass. The report exposes this as:

- `requiresExplicitApproval = true`
- `approvalStatus = pending_explicit_approval`

Switching from `shadow` to `on` is a manual environment change and must be tracked in the deployment record.
