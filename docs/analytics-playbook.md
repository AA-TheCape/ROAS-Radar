# Analytics Playbook

This guide is for analysts and engineers who need to understand what ROAS Radar stores, how orders are matched back to sessions, how each attribution model behaves, and how to interpret dashboard and API outputs without reading source first.

Use this document alongside:

- `docs/marketing-dimensions.md` for channel taxonomy and canonicalization rules
- `docs/visitor-identity-stitching.md` for deterministic identity-linking behavior
- `docs/last-non-direct-touch-approval-matrix.md` for the approved primary-winner rule matrix and Shopify fallback caveats
- `docs/ga4-fallback-attribution-contract-v1.md` for GA4 fallback eligibility, precedence, and confidence semantics
- `docs/reporting-metrics.md` for KPI formulas used by the reporting APIs and dashboard

## Dashboard Interpretation Quick Start

Use this sequence when validating the dashboard or answering analytics questions without reopening the whole codebase:

1. Start with `docs/reporting-metrics.md` to confirm KPI formulas and null-versus-zero behavior.
2. Use this playbook for what each reporting table means, how attribution models differ, and why orders can move between channels.
3. Use `docs/attribution-schema-v1.md` when a mismatch looks like a field-naming, normalization, or Shopify-attribute problem.
4. Use `docs/operational-attribution-contracts.md` when a mismatch looks like resolver precedence, writeback, reconciliation, retention, or dead-letter behavior.

Practical rule:

- schema doc explains what the captured values mean
- operational contract explains how capture and recovery behave
- analytics playbook explains how those persisted values should be interpreted in reports

## What ROAS Radar Measures

For the current MVP, ROAS Radar measures:

- tracked website sessions and events from a single Shopify storefront
- Shopify orders and basic customer identifiers from webhooks
- deterministic attribution from tracked sessions to orders
- reporting aggregates by attribution model, date, source, medium, campaign, content, and term

It does not attempt probabilistic measurement. Attribution is based on explicit session evidence, tracked checkout/cart tokens, and deterministic identity stitching.

## Tracking Event Contract

`POST /track` accepts four browser event types:

- `page_view`
- `product_view`
- `add_to_cart`
- `checkout_started`

The request body must be a JSON object with:

- `eventType`: one of the supported values above
- `occurredAt`: ISO timestamp
- `sessionId`: UUID generated and persisted client-side
- `pageUrl`: absolute `http` or `https` URL
- `referrerUrl`: optional absolute `http` or `https` URL
- `shopifyCartToken`: optional string
- `shopifyCheckoutToken`: optional string
- `clientEventId`: optional idempotency key
- `context.userAgent`, `context.screen`, `context.language`: optional context fields

Example:

```json
{
  "eventType": "page_view",
  "occurredAt": "2026-04-10T12:00:00.000Z",
  "sessionId": "8f5c0b53-c812-4a59-a7e6-8df0b0c7a1f1",
  "pageUrl": "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123",
  "referrerUrl": "https://www.google.com/",
  "shopifyCartToken": null,
  "shopifyCheckoutToken": null,
  "clientEventId": "evt_01hrtrack123",
  "context": {
    "userAgent": "browser user agent",
    "screen": "1440x900",
    "language": "en-US"
  }
}
```

### Validation and rejection behavior

Analysts should expect `/track` events to be rejected when:

- `eventType` is not one of the four supported event types
- `sessionId` is not a UUID
- `pageUrl` is missing, malformed, or not `http`/`https`
- `referrerUrl` is malformed or not `http`/`https`
- `occurredAt` is older than the configured max age
  Default: 14 days
- `occurredAt` is too far in the future
  Default: more than 300 seconds ahead of server time
- the browser `Origin` is not in `TRACKING_ALLOWED_ORIGINS` when origin restrictions are configured
- the request exceeds the in-memory rate limit
  Default: 120 requests per 60 seconds per IP/session key

### Deduplication behavior

Tracking ingestion is idempotent in two ways:

- `clientEventId` is a preferred deduplication key when the storefront sends one
- if `clientEventId` is missing or reused inconsistently, the backend also hashes a normalized fingerprint of `sessionId`, `eventType`, `occurredAt`, `pageUrl`, `referrerUrl`, `shopifyCartToken`, and `shopifyCheckoutToken`

This matters for analysis:

- duplicate browser retries should not create duplicate `tracking_events` rows
- event counts may be lower than raw network attempts because duplicate payloads are intentionally collapsed

### Marketing parameter extraction

ROAS Radar parses campaign metadata from `pageUrl` and stores canonicalized values for:

- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`
- `gclid`
- `fbclid`
- `ttclid`
- `msclkid`

Canonical source and medium rules are shared with spend ingestion. Refer to `docs/marketing-dimensions.md` for alias mapping, click-ID inference, and the meaning of `unknown` versus `unmapped`.

## Warehouse Objects

The main analyst-facing tables are below.

### `tracking_sessions`

One row per ROAS Radar browser session.

Key fields:

- `id`: session UUID used across tracking and order linkage
- `first_seen_at`, `last_seen_at`: first and latest accepted activity timestamps
- `landing_page`, `referrer_url`: first known landing context
- `initial_utm_*` and initial click ID fields: first-touch channel values captured for the session
- `customer_identity_id`: canonical deterministic identity when stitching succeeds
- `user_agent`, `ip_hash`: technical context, with IP stored only as a hash

Interpretation notes:

- visits in reporting are counted from `tracking_sessions`, not from raw event totals
- the session stores the first accepted marketing touch for that session, not every later parameter change

### `tracking_events`

Append-only event table for accepted storefront events.

Key fields:

- `session_id`: links back to `tracking_sessions`
- `event_type`, `occurred_at`, `page_url`, `referrer_url`
- canonical `utm_*` and click-ID fields extracted from the event URL
- `shopify_cart_token`, `shopify_checkout_token`: later used for deterministic attribution
- `client_event_id`: optional storefront idempotency key
- `customer_identity_id`: backfilled when identity stitching links the session later
- `raw_payload`: exact parsed browser or capture payload as received at ingestion time

Interpretation notes:

- this table is best for behavioral and debugging analysis
- event counts will usually exceed visit counts because a session can emit multiple events

### `shopify_orders`

Normalized order facts ingested from Shopify webhooks.

Key fields:

- `shopify_order_id`, `shopify_order_number`
- `shopify_customer_id`, `email`, `email_hash`
- `subtotal_price`, `total_price`, `currency_code`
- `processed_at`, `created_at_shopify`, `updated_at_shopify`, `ingested_at`
- `landing_session_id`, `checkout_token`, `cart_token`
- `customer_identity_id`: deterministic identity attached during stitching
- `raw_payload`: original webhook body

Interpretation notes:

- order reporting time uses `processed_at`, then `created_at_shopify`, then `ingested_at`
- `landing_session_id`, `checkout_token`, and `cart_token` are the strongest deterministic evidence fields used by attribution

### `attribution_results`

One summary attribution row per order.

Key fields:

- `shopify_order_id`
- `session_id`
- `match_source`
- `attribution_model`
- `attributed_source`, `attributed_medium`, `attributed_campaign`, `attributed_content`, `attributed_term`
- `attributed_click_id_type`, `attributed_click_id_value`
- `confidence_score`
- `confidence_label`
- `attribution_reason`
- `attributed_at`

Interpretation notes:

- this table stores the primary `last_touch` result used for single-row order summaries
- `match_source` is the durable provenance field for how the primary result was sourced
- analysts comparing models should use `attribution_order_credits` and `daily_reporting_metrics`, not just `attribution_results`

### `attribution_order_credits`

One row per order, per attribution model, per touchpoint position.

Key fields:

- `shopify_order_id`
- `attribution_model`
- `touchpoint_position`
- `match_source`
- attributed channel fields and click IDs
- `credit_weight`
- `revenue_credit`
- `is_primary`
- `confidence_label`
- `attribution_reason`

Interpretation notes:

- this is the canonical multi-touch table for model comparison
- `credit_weight` and `revenue_credit` can be fractional
- an order contributes one full order across all rows within a model, but that order may be split across multiple touchpoints
- `match_source` distinguishes deterministic, Shopify fallback, GA4 fallback, and unattributed provenance

### `daily_reporting_metrics`

Pre-aggregated reporting table used by the dashboard and reporting APIs.

Key fields:

- `metric_date`
- `attribution_model`
- `source`, `medium`, `campaign`, `content`, `term`
- `visits`
- `attributed_orders`
- `attributed_revenue`
- `spend`
- `impressions`
- `clicks`
- `new_customer_orders`, `returning_customer_orders`
- `new_customer_revenue`, `returning_customer_revenue`
- `last_computed_at`

Interpretation notes:

- visits come from `tracking_sessions.first_seen_at`
- orders and revenue come from `attribution_order_credits`
- spend, impressions, and clicks come from ad-platform creative-level spend tables when those integrations are enabled
- because order credit can be fractional, aggregate orders can be decimals for multi-touch models

### `data_quality_check_runs`

Operational and analyst-facing reconciliation output.

Key fields:

- `run_date`
- `check_key`
- `status`
- `severity`
- `discrepancy_count`
- `summary`
- `details`
- `checked_at`

Interpretation notes:

- this table is intentionally date-lagged
- use it to understand expected freshness gaps and anomalies, not same-minute operational state

## Attribution Resolution Order

The attribution worker resolves journeys in this order:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. stitched customer identity fallback
5. Shopify hint fallback
6. GA4 fallback
7. unattributed fallback

### What each step means

`landing_session_id`

- strongest possible evidence
- the order already carries the exact ROAS Radar session UUID
- confidence score: `1.00`
- attribution reason: `matched_by_landing_session`

`checkout_token`

- matches a tracked event with the same Shopify checkout token
- confidence score: `1.00`
- attribution reason: `matched_by_checkout_token`

`cart_token`

- matches a tracked event with the same Shopify cart token
- confidence score: `0.90`
- attribution reason: `matched_by_cart_token`

`customer_identity_id`

- uses deterministic identity stitching from hashed email and Shopify customer ID
- considers eligible sessions for that stitched identity inside the attribution window
- confidence score: `0.60`
- attribution reason: `matched_by_customer_identity`

`shopify_hint_fallback`

- recovery-only synthetic fallback from Shopify order hints
- only eligible when deterministic resolution produces no winner
- confidence score: `0.55` with a supported click ID, `0.40` with canonical UTMs and no click ID
- confidence label: `low`
- attribution reason: `shopify_hint_derived`
- `match_source = 'shopify_hint_fallback'`

`ga4_fallback`

- recovery-only GA4 fallback for Shopify web orders
- only eligible when deterministic resolution produces no winner and Shopify hint fallback has no match
- candidate timestamps after the order timestamp are ineligible
- direct or empty GA4 candidates with no canonical UTMs and no click ID are ineligible
- confidence score: `0.35` with a supported click ID, `0.25` with canonical UTMs and no click ID
- confidence label: `low`
- attribution reason: `ga4_fallback_derived`
- `match_source = 'ga4_fallback'`

`unattributed`

- no eligible deterministic, Shopify fallback, or GA4 fallback match was found
- the engine still creates one unattributed credit row so revenue is conserved across models
- confidence score: `0.00`
- confidence label: `none`
- attribution reason: `unattributed`
- `match_source = 'unattributed'`

### Attribution window and session selection

Current defaults:

- attribution window: 7 days
- identity fallback looks for sessions at or before the order timestamp and within the prior 7 days

When multiple identity-linked sessions are eligible:

- the journey is ordered chronologically
- the attribution models then decide how credit is allocated across those touchpoints
- direct or untagged sessions can still appear, but rule-based weighting discounts them

### Approved primary-winner semantics

For the primary deterministic winner used by `last_touch` and `attribution_results`, ROAS Radar uses an approved last-non-direct-touch contract.

Key rules:

- if any eligible non-direct deterministic candidate exists, direct candidates are ignored for primary winner selection
- a click-ID-only touch is non-direct even when every UTM field is missing
- same-timestamp ties break by deterministic source precedence: `landing_session_id`, then `checkout_token`, then `cart_token`, then `customer_identity`
- if timestamp and source precedence still tie, click-ID presence wins, then the lexicographically smaller `sessionId`

Practical effect:

- a later direct revisit does not overwrite an earlier paid or otherwise non-direct touch
- a later non-direct touch still beats an earlier non-direct touch, even if the newer touch has UTMs and no click ID
- direct traffic only becomes the primary winner when every eligible deterministic candidate is direct

For the full approval matrix and change-discipline expectations, refer to `docs/last-non-direct-touch-approval-matrix.md`.

## Attribution Models

ROAS Radar currently computes six attribution models for every resolved journey.

### `first_touch`

- assigns 100% of order and revenue credit to the earliest touchpoint in the journey
- useful when measuring which campaigns start journeys

### `last_touch`

- assigns 100% of credit to the latest touchpoint in the journey
- this is also the primary model persisted into `attribution_results`
- useful when measuring the final converting interaction

### `linear`

- splits credit evenly across all touchpoints
- useful when you want every eligible touchpoint to share equal responsibility

### `time_decay`

- gives more credit to touchpoints closer to the order timestamp
- current half-life default: 7 days
- useful when recency should matter more than simple equal weighting

### `position_based`

- for journeys with three or more touchpoints, defaults to:
  - 40% first touch
  - 40% last touch
  - 20% split across middle touches
- for two-touch journeys, credit is split evenly
- for one-touch journeys, the single touch gets full credit
- useful when both discovery and conversion moments matter most

### `rule_based_weighted`

- starts from positional weights, then applies deterministic multipliers
- current defaults:
  - first touch weight: `0.3`
  - middle touch weight: `0.2`
  - last touch weight: `0.5`
  - click-ID bonus multiplier: `1.25`
  - direct discount multiplier: `0.5`

Practical effect:

- later touches usually receive more weight
- touches with click IDs such as `gclid` or `fbclid` receive a boost
- direct or untagged touches are discounted

This is the closest thing in the MVP to a tuned heuristic model, but it is still deterministic and code-defined.

## How To Interpret Model Outputs

### Fractional orders and revenue are expected

For `linear`, `time_decay`, `position_based`, and `rule_based_weighted`, analysts should expect:

- fractional `orders`
- fractional `new_customer_orders`
- fractional `returning_customer_orders`
- revenue split across multiple campaigns

That is not a bug. It is how multi-touch credit allocation works in `attribution_order_credits` and `daily_reporting_metrics`.

### `is_primary` does not mean full ownership

`is_primary` marks the touchpoint with the highest credited cents within a model. It does not mean that touchpoint owns 100% of the order.

### `attribution_reason` explains matching, not model quality

`attribution_reason` tells you how the journey was resolved:

- exact session evidence
- checkout/cart token evidence
- stitched identity fallback
- Shopify synthetic fallback
- GA4 fallback
- unattributed fallback

It does not tell you that a campaign is “better” or that one model is more correct than another.

### Shopify synthetic fallback is recovery-only

When deterministic session resolution fails for an eligible web order, ROAS Radar may apply synthetic Shopify-hint attribution as a recovery step.

Analyst expectations:

- this fallback uses `attribution_reason = shopify_hint_derived`
- it does not resolve to a real ROAS Radar session, so `session_id` can remain `null`
- it must not overwrite a resolved deterministic winner
- current confidence is lower than deterministic matching: `0.55` with a click ID, `0.40` without one

Treat this as recovered attribution signal, not as evidence that a real tracked session was stitched successfully.

### GA4 fallback is weaker than Shopify hint fallback

When both deterministic resolution and Shopify synthetic fallback fail for an eligible Shopify web order, ROAS Radar may apply GA4 fallback attribution as a later recovery step.

Analyst expectations:

- this fallback uses `attribution_reason = ga4_fallback_derived`
- it persists `match_source = 'ga4_fallback'`
- it does not resolve to a real ROAS Radar session, so `session_id` remains `null`
- it must not overwrite deterministic or Shopify synthetic fallback outcomes
- current confidence is lower than Shopify synthetic fallback: `0.35` with a click ID, `0.25` without one

Treat this as the weakest approved attributed recovery path before `unattributed`, not as first-party session proof.

### `confidence_score` is about match strength

`confidence_score` reflects how strongly the order was linked to a journey:

- `1.00`: exact session or checkout-token evidence
- `0.90`: cart-token evidence
- `0.60`: stitched identity fallback
- `0.55`: Shopify synthetic fallback with a supported click ID
- `0.40`: Shopify synthetic fallback with canonical UTMs and no click ID
- `0.35`: GA4 fallback with a supported click ID
- `0.25`: GA4 fallback with canonical UTMs and no click ID
- `0.00`: unattributed

It is not a measure of channel performance, campaign quality, or model superiority.

`confidence_label` is the grouped interpretation layer:

- `high`: `1.00`, `0.90`
- `medium`: `0.60`
- `low`: `0.55`, `0.40`, `0.35`, `0.25`
- `none`: `0.00`

### Summary, campaigns, timeseries, and orders endpoints read different shapes

`GET /api/reporting/summary`

- sums `visits`, `attributed_orders`, and `attributed_revenue` for the selected filters and model

`GET /api/reporting/campaigns`

- groups by source, medium, campaign, and content
- `conversionRate` is `orders / visits` within that grouped slice

`GET /api/reporting/timeseries`

- returns chart-ready points grouped by `day`, `source`, or `campaign`

`GET /api/reporting/orders`

- returns one row per order using the primary credit row for the selected model
- useful for debugging why a particular order appears under a channel or campaign

## Dashboard Reading Guide

Use the dashboard in this order when sanity-checking performance:

1. Summary cards
   Confirm the selected date range and attribution model first. Card totals are model-scoped, so a model switch can legitimately move revenue and order counts even when raw orders did not change.
2. Campaign table
   Treat this as the best view for channel mix. `conversionRate`, `roas`, and `cac` are computed from the grouped slice, not from global totals.
3. Timeseries chart
   Use this to spot timing shifts, worker lag, or reconciliation effects. A recent date can look incomplete while attribution jobs or writeback jobs are still catching up.
4. Orders view
   Use this as the debugging surface for a specific order. It shows the primary credit row for the selected model, so it is the fastest place to confirm whether the disagreement is attribution logic or aggregate math.

When a dashboard value looks wrong, ask these questions in order:

- Is the selected attribution model the one you expect?
- Is the issue a metric formula question? If so, use `docs/reporting-metrics.md`.
- Is the issue a field capture or normalization question? If so, use `docs/attribution-schema-v1.md`.
- Is the issue a resolver, Shopify writeback, retry, or reconciliation question? If so, use `docs/operational-attribution-contracts.md`.

## Identity Stitching Impact

ROAS Radar uses deterministic identity stitching, documented in `docs/visitor-identity-stitching.md`.

In practice:

- Shopify order webhooks can create or reuse a canonical `customer_identity_id`
- sessions directly evidenced by landing session, checkout token, or cart token inherit that identity
- those linked sessions make historical touchpoints available for later identity-fallback attribution

Analyst implications:

- an order may be attributed even when the exact checkout-start event is missing, if the same customer identity has a recent eligible session
- sessions already linked to a different identity are not auto-merged
- if email and Shopify customer ID conflict with existing identity records, stitching is rejected rather than guessed

This makes the system conservative by design. It will prefer under-attribution over incorrect cross-user joins.

## Freshness and Caveats

Expected behavior in the current MVP:

- tracking writes are near-real-time once `/track` accepts an event
- Shopify order rows appear near-real-time after webhook receipt
- attribution processing is asynchronous and normally polled every 10 seconds
- reporting can lag ingestion until the attribution worker persists credits and refreshes `daily_reporting_metrics`
- data-quality outputs are intentionally slower and use date-based checks rather than instant checks

Analysts should treat short-lived mismatches between raw order ingestion and reporting aggregates as expected while the worker catches up.

## MVP Limitations Versus Northbeam-Like Platforms

ROAS Radar MVP is intentionally narrower than a Northbeam-like attribution platform.

Current limitations:

- no media mix modeling or incrementality measurement
- no view-through attribution
- no probabilistic identity graph
- no cross-device identity beyond deterministic session, token, email-hash, and Shopify-customer-ID stitching
- no automatic multi-store or multi-brand normalization
- no self-serve attribution-model tuning UI
- no advanced cohort, LTV, or forecasting workflows
- no automatic modeled conversions when tracking is missing
- limited event taxonomy focused on storefront conversion flow
- limited channel integrations compared with a mature attribution suite
- single-tenant assumptions in the current Shopify install and reporting shape

ROAS Radar should currently be interpreted as a deterministic Shopify attribution and reporting system, not as a full-funnel measurement platform.
