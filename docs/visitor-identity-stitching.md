# Identity Graph Stitching

This document replaces the old single-row `customer_identities` stitching contract.

ROAS Radar now resolves identity into a canonical `identity_journey_id` backed by:

- `identity_journeys`: canonical entity used by attribution, reporting, and session backfill
- `identity_nodes`: one row per normalized identifier value
- `identity_edges`: active ownership plus historical re-home and quarantine audit trail

During rollout, the legacy `customer_identity_id` columns remain a compatibility alias and are dual-written to the same canonical UUID as `identity_journey_id`.

## Supported Identifiers

The graph accepts these first-class identifiers:

- `session_id`
- `checkout_token`
- `cart_token`
- `shopify_customer_id`
- `hashed_email`
- `phone_hash`

Normalization rules:

- `session_id`: exact UUID string
- `checkout_token`: trim only
- `cart_token`: trim only
- `shopify_customer_id`: trim only
- `hashed_email`: `sha256(trim(lower(email)))`
- `phone_hash`: `sha256(e164(phone))`; invalid phone numbers are ignored

## Merge Precedence

Merge precedence is deterministic and independent from attribution winner precedence.

| Rank | Identifier | Behavior |
| --- | --- | --- |
| `100` | `shopify_customer_id` | Authoritative. Can pull lower-rank identifiers into its journey. |
| `70` | `hashed_email` | Deterministic unless it conflicts with another authoritative Shopify customer. |
| `60` | `phone_hash` | Deterministic but quarantined more aggressively when shared. |
| `40` | `checkout_token` | Session bridge only. |
| `30` | `cart_token` | Session bridge only. |
| `20` | `session_id` | Session-local only. |

When no Shopify customer id is present and multiple anonymous journeys are candidates, the winner is chosen by:

1. highest active precedence rank on the journey
2. latest `last_observed_at`
3. lexicographically smallest `journey_id`

## Deterministic Rules

For every ingestion event:

1. Normalize incoming identifiers and drop blanks.
2. Upsert `identity_nodes` rows for the normalized values.
3. Load active `identity_edges` for the matched nodes.
4. Create a new journey when no candidate exists.
5. Reuse the single candidate journey when only one exists.
6. If a matching `shopify_customer_id` journey exists, that journey wins.
7. When Shopify authority wins, lower-rank identifiers are re-homed with a new active `promoted` edge and the old edge is superseded.
8. Two different active Shopify customer ids are a hard stop. The system writes no automatic merge.
9. Ambiguous nodes are stored but must not drive future auto-merges until repaired.

## Quarantine Rules

The graph never auto-merges two authoritative Shopify customers into the same journey.

Conflict handling:

- `phone_hash` seen under two different authoritative Shopify customers is marked ambiguous and moved into a `quarantined` edge with `phone_hash_conflicts_across_authoritative_customers`.
- `hashed_email` follows the same path with `hashed_email_conflicts_across_authoritative_customers`.
- a merge attempt that still sees multiple authoritative Shopify customers after quarantine returns `authoritative_shopify_customer_conflict` and leaves active ownership unchanged.

Operational implications:

- quarantined nodes stay queryable for audit
- quarantined nodes do not participate in future automatic stitching
- replaying the same conflicted source is idempotent through `identity_edge_ingestion_runs`

## Compatibility Surfaces

Graph stitching updates both canonical and compatibility references on:

- `tracking_sessions`
- `tracking_events`
- `session_attribution_identities`
- `shopify_orders`
- `shopify_customers`

Rollout rule:

- `identity_journey_id` is the contract going forward
- `customer_identity_id` is still written so existing attribution fallback code keeps working until all readers move to `identity_journey_id`

## Attribution Boundary

Identity stitching changes candidate collection, not attribution winner semantics.

Attribution still prefers direct deterministic evidence first:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. stitched identity-journey fallback

The stitched fallback is currently implemented through the dual-written `customer_identity_id` alias, so behavior is unchanged while the attribution worker migrates internally.

## Deterministic Examples

### Example 1: Anonymous journey later promoted by Shopify identity

- anonymous event creates one journey from `session_id`, `checkout_token`, and `cart_token`
- later Shopify evidence with the same session/tokens plus `shopify_customer_id` and `hashed_email` updates that same journey
- no second journey is created

### Example 2: Email belongs to one journey, Shopify customer id points elsewhere

- the Shopify customer id journey wins
- the email edge is superseded and re-homed with `edge_type = promoted`
- the losing anonymous journey is marked `merged` when it no longer owns active nodes

### Example 3: Shared phone across two authoritative Shopify customers

- the phone node is quarantined
- the second Shopify journey stays authoritative for its own customer id
- no automatic merge occurs

### Example 4: No Shopify id, multiple anonymous candidates

- the highest-precedence anonymous journey wins
- lower-rank nodes are re-homed deterministically
- timestamp and lexical tie-breakers make the result stable across retries and concurrent runs

## Operator References

- `docs/operational-attribution-contracts.md`
- `docs/internal-identity-read-api.md`
- `docs/runbooks/identity-data-quality.md`
