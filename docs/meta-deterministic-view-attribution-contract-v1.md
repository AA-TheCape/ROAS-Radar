# Meta Deterministic View Attribution Contract V1

This document defines the approved v1 contract for deterministic Meta view and impression inputs used by ROAS Radar attribution modeling.

Use it together with:

- `docs/meta-attributed-revenue-contract-v1.md` for Meta campaign-day revenue aggregates
- `docs/raw-payload-persistence-contract.md` for exact raw Meta payload retention rules
- `docs/attribution-schema-v1.md` for canonical attribution fields and persistence semantics
- `docs/reporting-api-contract.md` and `docs/reporting-metrics.md` for reporting output contracts

## Scope

This contract applies only to aggregate Meta API view and impression inputs that can be verified through Meta's Ads APIs.

In scope:

- campaign, ad set, ad, or comparable aggregate Meta reporting rows returned by the Meta API
- aggregate impression and view metrics eligible for model features
- deterministic association to Meta account, campaign, ad set, ad, reporting date, and ingestion run
- model-side view/impression features derived from aggregate Meta API responses

Out of scope:

- user-level impression logs
- browser, pixel, or first-party event guesses used to infer whether a person viewed an ad
- synthetic view-through attribution generated from local session timelines alone
- non-Meta source views or impressions
- any row that cannot be traced back to a Meta API response retained by ROAS Radar

The v1 contract is aggregate-only. It must not create, imply, or persist person-level Meta view-through matches.

## Verification Source

Meta deterministic view and impression inputs must be verified through Meta API responses only.

Allowed verification source:

- Meta Ads API or Marketing API response payloads retained by the ingestion system

Disallowed verification sources:

- client-side pageview events
- Shopify order metadata
- GA4 events
- UTMs, referrers, or click identifiers without a corresponding Meta API aggregate row
- manually uploaded estimates unless they are explicitly stored outside this contract

If a view or impression input cannot be traced to a retained Meta API response, it is not eligible for deterministic Meta view attribution features.

## Attribution Window

The approved deterministic view attribution window is 7 days.

Window semantics:

- the model may use eligible Meta API view/impression aggregates whose reporting date falls within 7 days before the attributed conversion date
- the window is inclusive of the conversion date and the prior 6 reporting days
- rows outside the 7-day window must not contribute to deterministic view attribution features for that conversion date

This window is a ROAS Radar contract and must be configured explicitly wherever deterministic Meta view attribution features are computed.

## Model Output Separation

Deterministic Meta view/impression features must produce separate model outputs from click-based, last-touch, Shopify fallback, and GA4 fallback attribution outputs.

Required output separation:

- click attribution winner outputs remain governed by the primary attribution resolver contracts
- deterministic view attribution model outputs must be stored and exposed as separate view/impression model outputs
- reporting surfaces must label deterministic Meta view attribution as model output, not as the primary click winner
- attribution explainability must preserve whether a value came from click resolution, Shopify fallback, GA4 fallback, or deterministic Meta view/impression modeling

The deterministic view/impression model may inform analysis and reporting, but it must not overwrite the canonical order attribution winner.

## Non-Mixing Rule

Reporting and attribution outputs must not mix deterministic Meta view/impression model outputs with primary attribution outputs into a single undifferentiated metric.

Required behavior:

- primary attribution output fields must not include deterministic view/impression model credit
- deterministic view/impression output fields must not include primary click, Shopify fallback, or GA4 fallback credit
- aggregate reporting must expose mixed-source comparisons as separate columns, series, or explicitly labeled sections
- API responses must preserve separate field names or namespaces for primary attribution outputs and deterministic Meta view/impression outputs

Disallowed behavior:

- adding Meta view credit into primary attributed revenue without a separate field
- using deterministic Meta view credit to change the primary attribution winner
- reporting a blended "attributed revenue" value that combines primary attribution and Meta view/impression model credit without explicit separation
- backfilling Shopify writeback fields with deterministic Meta view/impression model results

This non-mixing rule applies to database writes, API responses, dashboard metrics, exports, and analyst-facing derived tables.

## Quarantine Behavior

Rows must be quarantined instead of used for deterministic Meta view/impression features when they fail contract validation.

Quarantine triggers:

- missing retained Meta API raw payload reference
- missing organization, connection, ad account, reporting date, or Meta entity identifier required for the aggregate grain
- reporting date cannot be normalized
- row falls outside the configured ingestion or modeling window being evaluated
- metric value cannot be parsed as a non-negative numeric aggregate
- metric source is not a Meta API response
- aggregate grain is ambiguous or would cause duplicate credit without a stable dedupe key

Quarantined rows:

- must not feed deterministic view/impression features
- must not affect primary attribution outputs
- must retain enough context for operator triage
- may be replayed only after the validation failure is corrected and raw Meta traceability remains intact

Quarantine must fail closed. When eligibility is uncertain, the row stays out of attribution and reporting outputs that depend on this contract.

## Raw Traceability

Every accepted deterministic Meta view/impression input must preserve traceability to the retained Meta API response.

Required traceability surface:

- organization id
- Meta connection id or equivalent connection foreign key
- ad account id
- Meta entity grain and identifier, such as campaign, ad set, or ad
- report date
- metric names and values used as model inputs
- raw payload reference or raw record foreign key
- sync job id or ingestion run id
- validation status showing accepted or quarantined state

Derived model features may be normalized, but raw Meta payloads must remain exact-as-received under the raw payload contract.

## Implementation Linkage Requirements

Any implementation that introduces deterministic Meta view/impression features must reference this contract near:

- Meta API request construction for view and impression metrics
- raw-to-normalized extraction logic
- quarantine validation logic
- deterministic view/impression feature generation
- reporting and API response mapping that exposes model outputs

Minimum enforcement coverage implied by this contract:

- accepts only rows traceable to retained Meta API payloads
- rejects or quarantines non-Meta or untraceable view/impression inputs
- excludes rows outside the 7-day attribution window
- keeps deterministic view/impression outputs separate from primary attribution outputs
- prevents blended reporting fields that violate the non-mixing rule
- prevents deterministic view/impression model outputs from overwriting primary attribution winners

## Docs Placement

Recommended repo placement:

- `docs/meta-deterministic-view-attribution-contract-v1.md`

Recommended docs index update:

- add this document under `docs/README.md` core references near other Meta and attribution contracts
