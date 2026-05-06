# Meta Attributed Revenue Contract V1

This document defines the v1 contract for campaign-level daily Meta-attributed order value aggregates.

Use it together with:

- `docs/raw-payload-persistence-contract.md` for exact raw Meta payload retention rules
- `docs/reporting-metrics.md` for ROAS interpretation
- `src/modules/meta-ads/index.ts` for Meta sync implementation
- `test/meta-ads.test.ts` and future Meta revenue ingestion tests for contract enforcement

## Scope

This contract applies to the Meta Insights ingestion path that will populate the new Meta order-value aggregate surface.

It covers:

- request shape for Meta Insights campaign-day revenue pulls
- canonical extraction of revenue and purchase count from `action_values` and `actions`
- normalized field meanings for stored campaign-day order-value rows
- raw payload metadata that must remain traceable to upstream Meta responses

This contract does not replace the raw payload contract. Raw insight rows must still be persisted exactly as received before canonical extraction.

## Request Contract

The backend revenue pull must call:

- endpoint: `act_{AD_ACCOUNT_ID}/insights`
- level: `campaign`
- time increment: `1`
- breakdown: `action_breakdowns=action_type`
- attribution behavior: `use_account_attribution_setting=true`

Required fields for the request:

- `campaign_id`
- `campaign_name`
- `date_start`
- `date_stop`
- `spend`
- `actions`
- `action_values`
- `purchase_roas`

Required request semantics:

- the request must return one row per campaign per reporting day per action type emitted by Meta
- the sync must treat `date_start` as the reporting day stored in ROAS Radar
- `date_stop` is expected to match `date_start` for daily pulls and should be retained for audit/debug purposes

### `action_report_time`

V1 must set `action_report_time` explicitly instead of relying on an API default.

Contract default:

- `action_report_time=conversion`

Reasoning:

- this keeps the stored `report_date` aligned to the action/conversion reporting day used for attributed order-value interpretation
- it avoids hidden behavior differences caused by omitted request parameters

This default is an implementation decision inferred from the required field semantics in this work item and should be treated as part of the ROAS Radar contract even if Meta platform behavior evolves.

## Canonical Action-Type Selection

Meta may return multiple purchase-like action types for the same campaign-day.

V1 canonical selection priority is:

1. `purchase`
2. `omni_purchase`
3. `offsite_conversion.fb_pixel_purchase`

### Fallback rule

If none of the preferred action types is present for a campaign-day:

- use the first available action type from the configured allowed purchase-like set for that ingestion run
- persist the actual selected action type in the normalized row
- mark that the row used fallback selection through raw metadata or an explicit selected-type field, not by mutating raw payload

### Matching rule across arrays

Canonical extraction must evaluate both:

- `action_values` for revenue value
- `actions` for purchase count

The selected canonical action type should be applied consistently across both arrays when present.

Per metric fallback behavior:

- `attributed_revenue` comes from the selected action type in `action_values`
- `purchase_count` comes from the selected action type in `actions`
- if the selected action type is present in one array but absent in the other, keep the selected action type and leave the missing metric `null` rather than silently switching types for only one metric

This preserves a stable interpretation of "what action type this row represents".

## Normalized Field Contract

Each normalized Meta order-value aggregate row must represent one campaign for one reporting day.

### Required business fields

- `report_date`: the Meta reporting day from `date_start`; this is the order-value reporting day used by the UI and backend reads
- `campaign_id`: Meta campaign identifier from the insight row
- `campaign_name`: Meta campaign name from the insight row
- `currency`: account/reporting currency associated with the insight row; prefer the account currency already stored on the connection when Meta does not emit row-level currency
- `spend`: campaign spend for the same reporting day
- `attributed_revenue`: canonical revenue extracted from `action_values` for the selected action type
- `purchase_count`: canonical count extracted from `actions` for the selected action type
- `purchase_roas`: purchase ROAS value associated with the same canonical action type when available from Meta; otherwise `null`
- `action_type_used`: the exact action type chosen by the canonical priority/fallback logic

### Context fields

- `organization_id`: owning ROAS Radar organization context
- `meta_connection_id` or equivalent connection foreign key: source connection context
- `ad_account_id`: normalized Meta ad account identifier

### Raw traceability fields

The normalized row must preserve enough metadata to trace back to the raw Meta response without reconstructing it.

Required traceability surface:

- raw payload reference or raw record foreign key when available
- sync job id / ingestion run id
- raw `date_start`
- raw `date_stop`
- selected action type
- indication of whether canonical selection used priority match or fallback match

## Type and Nullability Rules

- `report_date` is required
- `campaign_id` is required for persisted normalized rows
- `campaign_name` may be nullable only if Meta omits it unexpectedly, but the raw row must still be retained
- `currency` should be non-null in normalized rows whenever the connection has account currency; otherwise nullable
- `spend` defaults to `0` only when Meta returns no spend value
- `attributed_revenue` is nullable when no eligible purchase-like `action_values` entry exists
- `purchase_count` is nullable when no eligible purchase-like `actions` entry exists
- `purchase_roas` is nullable when Meta does not return a compatible purchase ROAS value for the selected type
- `action_type_used` is nullable only when no eligible purchase-like action type exists at all and the row is still retained for operational visibility

## Raw Payload Rules

Raw payload handling remains governed by `docs/raw-payload-persistence-contract.md`.

Additional v1 rules:

- the full Meta insight row must be stored exactly as received before extraction
- canonical revenue/count fields must be derived from the raw row after persistence, not by mutating the raw payload
- normalized tables may store derived helper metadata, but must not rewrite or trim `raw_payload`

## Purchase ROAS Interpretation

`purchase_roas` in the normalized row is a Meta-reported platform metric, not a ROAS Radar recomputation.

V1 handling:

- if Meta exposes a matching purchase ROAS entry for the selected canonical action type, persist that numeric value
- do not derive `purchase_roas` by dividing revenue by spend inside ingestion when the Meta field is absent
- downstream reporting may separately compute ROAS from `attributed_revenue / spend`, but that is not a substitute for the raw Meta purchase ROAS field

## Row Acceptance Rules

Persist a normalized campaign-day row when:

- `campaign_id` is present
- `report_date` is present
- the source campaign-day insight row is otherwise structurally valid

A row may still be persisted with null canonical revenue/count fields when:

- spend exists but no eligible purchase-like action type exists
- the purpose is to preserve campaign-day coverage for reads and observability

Do not drop raw rows solely because canonical purchase extraction fails.

## Implementation Linkage Requirements

The following implementation tasks must reference this contract explicitly:

- `src/modules/meta-ads/index.ts` revenue-pull code near the request builder and extraction logic
- schema migration or table-definition code for the new Meta order-value aggregate table
- tests covering canonical action-type selection, fallback behavior, null handling, and raw-payload traceability

Minimum test coverage implied by this contract:

- selects `purchase` over all lower-priority types when multiple are present
- selects `omni_purchase` when `purchase` is absent
- selects `offsite_conversion.fb_pixel_purchase` when both higher-priority types are absent
- falls back to the first configured available purchase-like type when none of the primary three exist
- keeps the selected `action_type_used` even when one metric array is missing the selected type
- preserves exact raw payload storage for source insight rows containing `actions`, `action_values`, and unmapped nested fields

## Docs Placement

Recommended repo placement:

- `docs/meta-attributed-revenue-contract-v1.md`

Recommended docs index update:

- add this document under `docs/README.md` core references near other ingestion/reporting contracts
