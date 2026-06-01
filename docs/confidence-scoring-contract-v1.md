# Confidence Scoring Contract V1

This document is the versioned source of truth for ROAS Radar order-attribution confidence scoring. Backend writers, reporting APIs, dashboards, and analytics readers must use this contract when persisting, exposing, or interpreting attribution confidence.

## Scope

This contract applies to confidence fields attached to resolved Shopify order attribution:

- `attribution_results.confidence_score`
- `attribution_results.confidence_label`
- `attribution_order_credits.confidence_label`
- `attribution_order_credits.confidence_contract_version`
- `shopify_orders.attribution_confidence_score`
- reporting and API response fields exposed as `confidenceScore`

This contract does not score channel quality, campaign performance, revenue reliability, or model quality. It only describes match strength between an order and the attribution evidence that won.

## Score Values

Scores are bounded decimals in the inclusive range `0 <= score <= 1`. Writers must clamp explicit candidate scores into that range and round persisted values to exactly the two-decimal contract precision.

| Winning path | `match_source` or source code | `attribution_reason` | Score | Label |
| --- | --- | --- | ---: | --- |
| Exact landing-session evidence | `landing_session_id` | `matched_by_landing_session` | `1.00` | `high` |
| Exact checkout-token evidence | `checkout_token` | `matched_by_checkout_token` | `1.00` | `high` |
| Cart-token evidence | `cart_token` | `matched_by_cart_token` | `0.90` | `high` |
| Stitched identity fallback | `customer_identity` | `matched_by_customer_identity` | `0.60` | `medium` |
| Shopify synthetic fallback with supported click ID | `shopify_hint_fallback` | `shopify_hint_derived` | `0.55` | `low` |
| Shopify synthetic fallback with canonical UTMs only | `shopify_hint_fallback` | `shopify_hint_derived` | `0.40` | `low` |
| GA4 fallback with supported click ID | `ga4_fallback` | `ga4_fallback_derived` | `0.35` | `low` |
| GA4 fallback with canonical UTMs only | `ga4_fallback` | `ga4_fallback_derived` | `0.25` | `low` |
| No eligible attribution match | `unattributed` | `unattributed` | `0.00` | `none` |

## Label Semantics

Labels are not free-form buckets. Consumers must treat them as the grouped interpretation layer for the explicit score values above:

- `high`: exact or near-exact first-party evidence, currently `1.00` and `0.90`
- `medium`: stitched identity fallback, currently `0.60`
- `low`: synthetic or external recovered attribution, currently `0.55`, `0.40`, `0.35`, and `0.25`
- `none`: no eligible attribution match, currently `0.00`

Readers should prefer the persisted `confidence_label` where it exists instead of computing ad hoc thresholds. If a caller must derive a label from a score for a legacy row, it must derive only from the score table in this document.

## Precedence Rules

Confidence must follow the winner selected by the attribution resolver. It must not change resolver precedence.

Resolver precedence remains:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. `customer_identity`
5. `shopify_hint_fallback`
6. `ga4_fallback`
7. `unattributed`

Lower-confidence fallback paths must never override a higher-precedence deterministic winner. GA4 fallback must never override Shopify synthetic fallback.

## Persistence Contract

Backend attribution writers must persist the same bounded confidence score for the winning order outcome across canonical order-level surfaces:

- `attribution_results.confidence_score`
- `shopify_orders.attribution_confidence_score`
- `shopify_orders.attribution_confidence_contract_version`
- `attribution_results.confidence_contract_version`
- `attribution_order_credits.confidence_contract_version`
- winner snapshots stored in `shopify_orders.attribution_snapshot` when the snapshot includes confidence

`attribution_results.attribution_source_id`, `attribution_results.matching_method_id`, `shopify_orders.attribution_source_id`, and `shopify_orders.matching_method_id` must identify the same winner used to select the confidence score.

`last_attribution_run_at` must advance when an attribution run changes any confidence fingerprint field, including score, attribution source, matching method, winner dimensions, winner session, `match_source`, or attribution reason.

## API and Frontend Contract

Reporting and attribution APIs expose the order-level score as `confidenceScore`.

Frontend consumers must:

- render confidence as explanatory match-strength metadata
- keep attribution tier and source as the primary ordering and filtering fields
- avoid using confidence score as a replacement for attribution tier, model, revenue, or campaign-performance metrics
- treat `null` confidence only as legacy or unavailable data; newly written attributed rows should expose a bounded score

## Compatibility Rules

Existing fallback-specific contracts still apply:

- Shopify synthetic fallback remains recovery-only and keeps `session_id = null`.
- GA4 fallback remains recovery-only, keeps `session_id = null`, and persists `match_source = 'ga4_fallback'`.
- Unattributed rows must stay explicit with `match_source = 'unattributed'`, score `0.00`, and label `none`.

Any score, label, source-code, matching-method, or fallback-precedence change is a contract change and requires a new versioned document.
