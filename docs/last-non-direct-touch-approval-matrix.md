# Last Non-Direct Touch Approval Matrix

This document publishes the team-approved primary-winner semantics for ROAS Radar's deterministic order attribution flow. It exists so analysts, engineers, and operators do not need to infer the approved behavior from resolver code and tests alone.

Use this document alongside:

- `docs/analytics-playbook.md` for reporting and attribution interpretation
- `docs/ga4-fallback-attribution-contract-v1.md` for the approved GA4 fallback contract
- `docs/visitor-identity-stitching.md` for deterministic candidate collection and identity fallback behavior
- `docs/marketing-dimensions.md` for canonical source, medium, and click-ID interpretation rules

## Scope

This matrix governs the primary winner written to `attribution_results` and the primary `last_touch` row in `attribution_order_credits` across deterministic, Shopify synthetic fallback, GA4 fallback, and unattributed outcomes.

It does not redefine:

- fractional multi-touch credit rules for `linear`, `time_decay`, `position_based`, or `rule_based_weighted`
- attribution-window configuration
- Shopify synthetic-hint extraction rules beyond the recovery-only fallback semantics documented here
- GA4 candidate ingestion or extraction rules beyond the fallback semantics documented here and in `docs/ga4-fallback-attribution-contract-v1.md`

## Approved Semantics

### 1. Direct revisit behavior

Approved behavior:

- a later direct revisit does not override an earlier eligible non-direct touch
- if any eligible non-direct deterministic candidate exists, winner selection ignores the direct pool entirely
- a direct touch only wins when every eligible deterministic candidate is direct

Operational meaning:

- a branded or direct return to cart or checkout is retained for history and debugging
- that later direct revisit does not steal primary credit from an earlier tagged paid or otherwise non-direct touch

### 2. Click-ID-only classification

Approved behavior:

- a touch with no populated UTM dimensions but with any supported click ID is non-direct
- click-ID-only touches therefore beat later direct revisits
- same-timestamp ties still use click-ID presence as a tie-breaker after timestamp and ingestion-source precedence

Supported click IDs:

- `gclid`
- `gbraid`
- `wbraid`
- `fbclid`
- `ttclid`
- `msclkid`

Operational meaning:

- missing UTMs do not force a touch to direct when first-party click evidence is still present

### 3. Deterministic source precedence

Approved precedence:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. `customer_identity`

This precedence is used in two places:

- deduplicating the same session when it appears through multiple deterministic evidence sources
- breaking same-timestamp winner ties across different candidates

Operational meaning:

- stronger first-party evidence wins when timestamps are equal
- stitched identity fallback remains the weakest deterministic evidence source and is used only after explicit session, checkout, and cart evidence

### 4. Shopify-hint-derived fallback semantics

Approved behavior:

- Shopify-hint-derived attribution is recovery-only fallback behavior
- it is considered only after deterministic resolution fails to produce a landing session match
- it must never override a resolved deterministic winner
- synthetic hint attribution does not point to a real ROAS Radar session, so `session_id` remains `null`
- the forced fallback row uses `attribution_reason = shopify_hint_derived`

Current confidence semantics:

- `0.55` when a synthetic hint includes a supported click ID
- `0.40` when the synthetic hint has canonical UTMs but no click ID

Caveats:

- this fallback is allowed to create attributed revenue for otherwise unattributed web orders
- it is intended to recover signal when durable deterministic capture is missing, not to compete with deterministic evidence
- reporting and downstream consumers must treat it as synthetic attribution, not as proof of a stitched or resolved session

### 5. GA4 fallback semantics

Approved behavior:

- GA4 fallback is recovery-only fallback behavior after deterministic resolution and Shopify synthetic fallback both fail
- it is considered only for Shopify web orders
- it must never override a resolved deterministic winner
- it must never override a Shopify-hint-derived fallback winner
- GA4 fallback does not point to a real ROAS Radar session, so `session_id` remains `null`
- the fallback row uses `attribution_reason = ga4_fallback_derived`
- downstream surfaces must persist `match_source = 'ga4_fallback'`

Current confidence semantics:

- `0.35` when the GA4 fallback winner includes a supported click ID
- `0.25` when the GA4 fallback winner has canonical UTMs but no click ID

Caveats:

- GA4 fallback may replace an otherwise unattributed outcome
- a GA4 candidate with no canonical UTMs and no supported click ID is ineligible
- reporting and downstream consumers must treat this as weaker recovered attribution than Shopify hint fallback

## Winner Selection Contract

After deterministic candidate collection and deduplication:

1. Partition candidates into non-direct and direct pools.
2. If the non-direct pool is non-empty, ignore all direct candidates.
3. Choose the winner by latest `occurredAt`.
4. If timestamps tie, prefer stronger deterministic source precedence.
5. If that still ties, prefer the candidate with a click ID.
6. If that still ties, prefer the lexicographically smaller `sessionId`.

Implications:

- latest non-direct wins
- later direct revisits do not replace earlier eligible non-direct touches
- when every deterministic candidate is direct, the latest direct touch wins

## Approval Matrix

| Topic | Approved behavior | Reporting implication |
| --- | --- | --- |
| Direct revisit after earlier paid touch | Earlier non-direct touch remains primary winner | A branded/direct return does not steal order credit from the prior tagged touch |
| Click-ID-only touch | Click-ID-only counts as non-direct | Paid traffic can remain attributable even when UTMs are absent |
| Same-timestamp deterministic collision | Stronger source precedence wins | `landing_session_id` and checkout evidence outrank cart and identity fallback |
| Same-timestamp equal-precedence collision | Click-ID presence, then lexical `sessionId`, breaks the tie | Primary selection stays deterministic and reproducible |
| No deterministic winner, Shopify hint present | Synthetic fallback may create attribution with `shopify_hint_derived` | Revenue can be recovered, but without a real ROAS Radar session link |
| No deterministic winner, no Shopify hint winner, eligible GA4 candidate present | GA4 fallback may create attribution with `ga4_fallback_derived` and `match_source = 'ga4_fallback'` | Revenue can be recovered after weaker fallback, still without a real ROAS Radar session link |
| No deterministic, Shopify, or GA4 winner | Unattributed remains the final outcome with `match_source = 'unattributed'` | Revenue stays conserved without manufacturing attribution |

## Merge-Gated Product Decisions

The following points are now approved and should be treated as published product semantics unless a future change explicitly updates this document, the resolver behavior, and the tests together:

- direct revisit handling
- click-ID-only non-direct classification
- deterministic ingestion-source precedence
- Shopify-hint-derived fallback behavior, including null-session synthetic attribution and current confidence semantics
- GA4 fallback behavior, including null-session provenance, precedence after Shopify hints, and current confidence semantics

## Change Discipline

Any future change to these semantics should update all of the following together:

- this document
- `docs/analytics-playbook.md`
- `docs/ga4-fallback-attribution-contract-v1.md`
- `docs/visitor-identity-stitching.md`
- resolver and Shopify hint tests that enforce the behavior
