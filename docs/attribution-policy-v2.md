# Attribution Policy V2

Status: published engineering policy

Version id: `attribution_resolver_v2`

This document defines the V2 attribution policy for order-level canonical attribution and parallel Meta platform-reported evidence. It is the versioned engineering contract for resolver behavior, data surfaces, backfills, and reporting interpretation.

Use it together with:

- `docs/operational-attribution-contracts.md` for the implemented V1/V2 resolver, writeback, reconciliation, retention, and queue behavior
- `docs/last-non-direct-touch-approval-matrix.md` for first-party deterministic winner semantics
- `docs/attribution-schema-v1.md` for canonical browser capture and Shopify attribute fields
- `docs/meta-attributed-revenue-contract-v1.md` for Meta order-value ingestion and raw payload traceability
- `docs/reporting-metrics.md` for KPI formulas and attribution-tier interpretation

## Scope

V2 adds a separate Meta platform-reported attribution tier without allowing Meta to override stronger deterministic evidence.

The policy covers:

- final canonical tier precedence
- Meta canonical and parallel-only eligibility semantics
- canonical-vs-parallel decision policy
- schema, API, and reporting surfaces
- forward-processing and backfill versioning rules
- known limitations of the current implementation

It does not redefine the first-party capture schema, Shopify order ingestion, Meta Insights revenue extraction, or reporting KPI formulas.

## Final Precedence Table

Canonical order attribution chooses the first eligible tier in this table.

| Rank | Canonical tier | Resolver source | Eligibility summary | Confidence default | Canonical behavior |
| --- | --- | --- | --- | --- | --- |
| 1 | `deterministic_first_party` | `landing_session_id`, `checkout_token`, `cart_token`, stitched `customer_identity` / identity journey | At least one deterministic first-party candidate on or before the order timestamp. Winner uses last-non-direct semantics. | `1.00` landing or checkout, `0.90` cart, `0.60` identity | Always wins over all lower tiers. |
| 2 | `deterministic_shopify_hint` | `shopify_marketing_hint` | No first-party winner exists, and a Shopify marketing hint is inside the 7-day lookback window. | `0.55` unless a candidate carries a stronger explicit score | Wins over Meta, GA4, and unattributed. |
| 3 | `platform_reported_meta` | `meta_platform_reported` | No first-party or Shopify-hint winner exists, and at least one Meta evidence row is `eligible_canonical` inside the 7-day lookback window. | Candidate score, with policy floor `0.50` | Becomes canonical only after ranks 1 and 2 fail. |
| 4 | `ga4_fallback` | `ga4_fallback` | No higher tier wins, and a GA4 fallback candidate is inside the 7-day lookback window. | `0.35` unless a candidate carries an explicit score | Last eligible attributed fallback. |
| 5 | `unattributed` | n/a | No eligible candidate exists, or the order timestamp cannot be normalized. | `0.00` | Canonical no-match state. |

Guardrails:

- `platform_reported_meta` is unavailable under `attribution_resolver_v1`.
- V2 database guardrails reject canonical tiers that contradict higher-precedence first-party or Shopify-hint evidence in decision artifacts.
- `shopify_orders.attribution_tier`, `attribution_results`, and `attribution_decision_artifacts.canonical_tier_after` must agree when a decision artifact is linked.

## Meta Soft-Threshold Semantics

Meta evidence is evaluated with two soft thresholds after hard eligibility guards pass.

Default thresholds:

| Threshold | Value | Meaning |
| --- | --- | --- |
| Canonical threshold | `0.50` | Minimum confidence for Meta to be eligible as the canonical tier when no higher tier wins. |
| Parallel threshold | `0.35` | Minimum confidence for Meta to be retained as parallel-only evidence. |

Soft-threshold bands:

- `confidence_score >= 0.50`: `eligible_canonical`
- `0.35 <= confidence_score < 0.50`: `eligible_parallel_only`
- `confidence_score < 0.35`: `ineligible`

These thresholds are "soft" because they determine Meta's evaluation outcome, not final canonical precedence. A Meta row can be above the canonical threshold and still remain non-canonical when a first-party or Shopify-hint winner exists.

Hard guards must pass before threshold evaluation:

- order timestamp is present
- Meta touchpoint timestamp is present
- approved match basis is present
- raw payload traceability is present through `raw_payload_reference` or `raw_record_id`
- ingestion run reference is present
- source kind is `order_scoped` or `order_joinable`
- confidence score is present
- Meta touchpoint occurred on or before the order
- Meta touchpoint is inside the configured attribution window

If any required field is missing, the outcome is `ineligible` with `meta_ineligible_missing_required_fields`. If timing guards fail, the outcome is `ineligible` with `meta_ineligible_failed_hard_guard`.

## Canonical-Vs-Parallel Decision Policy

Meta evidence has two separate roles:

- canonical candidate: can become the persisted order attribution tier
- parallel evidence: can be displayed and audited without changing the persisted canonical winner

Decision rules:

1. Evaluate all Meta evidence independently and persist its eligibility outcome.
2. Resolve first-party deterministic evidence first.
3. Resolve Shopify hint evidence second.
4. If no higher tier wins, allow only `eligible_canonical` Meta evidence to compete for `platform_reported_meta`.
5. If Meta is selected, persist `platform_reported_meta`, set `meta_attribution_affected_canonical = true`, and link the selected `meta_order_attribution_evidence.id`.
6. If a higher-precedence tier wins while Meta evidence exists, retain Meta as parallel evidence and set `meta_attribution_affected_canonical = false`.
7. If Meta is below the canonical threshold but at or above the parallel threshold, expose `eligible_parallel_only` and keep the canonical tier unchanged.
8. If Meta fails hard guards or falls below the parallel floor, record the ineligible outcome for audit but do not treat it as parallel influence.

When multiple canonical-eligible Meta candidates remain, the resolver orders them by:

1. latest Meta touchpoint timestamp
2. match-basis precedence: `fbclid`, `fbc`, `external_id`, `email_hash`, `phone_hash`, `fbp`, `meta_order_reference`, `conversion_api_event_id`
3. click-through over view-through
4. higher confidence score
5. lexical `meta_signal_id` or source key

Parallel-only Meta evidence should answer "Meta also claimed influence" without changing the canonical winner used for ROAS Radar attribution metrics.

## Schema Surfaces

### `shopify_orders`

Order-level canonical summary fields:

- `attribution_tier`: includes `platform_reported_meta`
- `attribution_resolver_rule_version`: `attribution_resolver_v1` or `attribution_resolver_v2`
- `meta_attribution_evidence_id`: selected or primary displayed Meta evidence
- `meta_attribution_evaluation_outcome`: `eligible_canonical`, `eligible_parallel_only`, `ineligible`, or `not_evaluated`
- `meta_attribution_confidence_score`
- `meta_attribution_confidence_label`: `high`, `medium`, or `low`
- `meta_attribution_present`
- `meta_attribution_affected_canonical`
- `latest_attribution_decision_artifact_id`

### `attribution_results`

Resolver summary fields:

- `resolver_rule_version`
- `meta_attribution_evidence_id`
- `meta_attribution_evaluation_outcome`
- `meta_attribution_affected_canonical`
- `attribution_decision_artifact_id`

`attribution_reason = 'meta_platform_reported_match'` maps to the `platform_reported_meta` tier.

### `meta_order_attribution_evidence`

Durable Meta evidence table. Required policy fields include:

- source identity: `organization_id`, `shopify_order_id`, `meta_connection_id`, `raw_record_id`, `sync_job_id`, `ingestion_run_id`, `meta_signal_id`
- source classification: `platform`, `source_kind`, `match_basis`, `observed_match_bases`
- timing: `reported_at_utc`, `order_occurred_at_utc`, `meta_touchpoint_occurred_at_utc`, `reported_conversion_timestamp_utc`, `attribution_window_days`
- campaign context: `campaign_id`, `campaign_name`, `ad_account_id`, `ad_id`, `ad_set_id`, `currency_code`
- Meta-reported value: `reported_conversion_value`, `reported_event_name`, `is_view_through`, `is_click_through`
- evaluation: `confidence_score`, `eligibility_outcome`, `eligibility_reasons`, `disqualification_reasons`, `parallel_only_reasons`, `normalization_failures`, `eligibility_signals`
- traceability: `source_record_ids`, `raw_payload_reference`, `raw_payload_hashes`, `evidence_snapshot_hash`, `source_snapshot_json`, `rule_version`

### `attribution_decision_artifacts`

Replayable decision record for each resolver pass:

- resolver metadata: `resolver_run_source`, `resolver_triggered_by`, `resolver_timestamp`, `resolver_rule_version`, `resolver_model_version`
- canonical result: `canonical_tier_before`, `canonical_tier_after`, `canonical_winner_tier`, `canonical_winner_source`
- Meta summary: `meta_attribution_evidence_id`, `meta_evaluation_outcome`, `meta_affected_canonical`, `parallel_meta_available`
- precedence flags: `first_party_winner_present`, `shopify_hint_winner_present`, `ga4_fallback_candidate_present`
- audit inputs: `decision_reason`, `decision_reason_detail`, `confidence_score`, `confidence_threshold`, `rule_inputs_hash`, `evidence_snapshot_hash`, `order_snapshot_ref`, `replayable`

### `daily_reporting_metrics`

Reporting aggregates include `attribution_tier` in the primary key and support:

- `all`
- `deterministic_first_party`
- `deterministic_shopify_hint`
- `platform_reported_meta`
- `ga4_fallback`
- `unattributed`

The `all` tier is an aggregate view. It must not be interpreted as a canonical order tier.

## API Surfaces

Reporting filters accept `attributionTier` on summary, campaign, timeseries, and order endpoints:

- `deterministic_first_party`
- `deterministic_shopify_hint`
- `platform_reported_meta`
- `ga4_fallback`
- `unattributed`

Order-list and order-detail responses expose Meta policy fields:

- `metaAttributionEvidenceId`
- `metaAttributionEligibilityOutcome`
- `metaAttributionConfidenceScore`
- `metaAttributionConfidenceLabel`
- `metaAttributionPresent`
- `metaAttributionAffectedCanonical`

Order-detail responses additionally expose linked Meta evidence diagnostics when available:

- `metaAttributionMatchBasis`
- `metaAttributionWindowDays`
- `metaAttributionTouchpointOccurredAt`
- `metaAttributionReportedAt`
- `metaAttributionIsClickThrough`
- `metaAttributionIsViewThrough`
- `metaAttributionEligibilityReasons`
- `metaAttributionDisqualificationReasons`
- `metaAttributionParallelOnlyReasons`
- `metaAttributionNormalizationFailures`

The dashboard must label `platform_reported_meta` as "Meta platform-reported" and distinguish canonical Meta winners from parallel-only Meta influence.

## Backfill And Versioning Rules

Forward processing:

- New unattributed or unresolved orders use `attribution_resolver_v2`.
- Orders that already have an attribution tier keep their stored `attribution_resolver_rule_version` when it is valid.
- Existing attributed orders without a valid stored version are treated as `attribution_resolver_v1` until intentionally backfilled.

Manual backfills:

- Backfills that evaluate Meta canonical eligibility must write `attribution_resolver_v2`.
- Backfills must record `backfill_run_id` in `attribution_decision_artifacts` when `resolver_run_source = 'manual_backfill'`.
- Backfills must update tier progress counts for `platform_reported_meta`.
- Backfills must be replayable from decision artifacts, rule version, rule input hash, evidence snapshot hash, and order snapshot reference.

Version compatibility:

- `attribution_resolver_v1` does not select `platform_reported_meta`; it falls through from Shopify hint to GA4 fallback or unattributed.
- `attribution_resolver_v2` inserts Meta between Shopify hint and GA4 fallback.
- Historical reports must preserve the resolver version that produced each persisted order state unless a named backfill intentionally rewrites it.
- Schema changes that alter thresholds, hard guards, match-basis precedence, or tier ordering require a new resolver rule version.

## Known Limitations

- Meta platform-reported evidence is not first-party proof. It is treated as lower precedence than deterministic first-party and Shopify hint evidence.
- The canonical threshold is a policy floor, not a guarantee that Meta's platform claim is correct.
- Aggregate-only Meta source rows are not eligible for canonical attribution.
- Meta cannot become canonical without raw payload traceability and an ingestion run reference.
- Parallel-only Meta influence is not added to canonical attributed revenue; it is exposed for diagnostics and comparison.
- `meta_attribution_confidence_label` is a display summary and should not replace numeric confidence in policy decisions.
- The current resolver stores `customer_identity` as the deterministic source name in code paths that are transitioning to the identity-journey terminology.
- Decision artifact detail currently notes that Meta evidence versioning is not fully wired into every resolver path; consumers should rely on `resolver_rule_version`, `rule_inputs_hash`, and evidence snapshot fields for replay.
- V2 does not change Meta Insights order-value extraction rules from `docs/meta-attributed-revenue-contract-v1.md`.
