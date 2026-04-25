# Visitor Identity Stitching

ROAS Radar now maintains a canonical `customer_identities` record that can be linked to:

- anonymous tracked visitor sessions in `tracking_sessions`,
- historical touchpoints in `tracking_events`,
- Shopify customer records in `shopify_customers`,
- Shopify orders in `shopify_orders`.

## Deterministic Stitching Rules

When a Shopify order webhook arrives, the backend derives two stable identifiers:

- `shopify_customer_id`, when Shopify sends a customer object,
- `hashed_email`, computed as `sha256(trim(lower(email)))` when an order email is present.

The stitcher uses these rules in order:

1. If neither identifier is present, stitching is skipped.
2. If `shopify_customer_id` already belongs to an identity whose stored `hashed_email` is different from the incoming hash, stitching is rejected.
3. If `hashed_email` already belongs to an identity whose stored `shopify_customer_id` is different from the incoming customer id, stitching is rejected.
4. If the incoming `shopify_customer_id` and `hashed_email` resolve to two different identity rows, stitching is rejected.
5. Otherwise, the system reuses the matched identity row or creates a new one and fills any missing identifier on that row.

These conflict checks prevent accidental cross-user joins. The service never auto-merges two pre-existing identities.

## Historical Touchpoint Re-Linking

After an identity row is selected, the webhook flow updates:

- the current `shopify_orders.customer_identity_id`,
- the matching `shopify_customers.customer_identity_id`,
- any tracked session directly evidenced by the order via:
  - `landing_session_id`,
  - matching `checkout_token`,
  - matching `cart_token`.

All `tracking_events` for those sessions inherit the same `customer_identity_id`, which re-links historical touchpoints without rewriting event payloads.

Sessions already linked to a different canonical identity are left untouched.

## Attribution Impact

Attribution still prefers direct evidence first:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`

If none of those match, the worker can now fall back to recent sessions already stitched to the order's `customer_identity_id`. It chooses the most recent eligible non-direct session first, then a direct fallback session only if no tagged session exists.

## Primary Winner Semantics

Identity stitching affects candidate collection, but it does not change the approved primary-winner contract.

Published rules:

- if any eligible deterministic non-direct candidate exists, later direct revisits are ignored for primary winner selection
- a touch with no UTMs but a supported click ID still counts as non-direct
- same-timestamp deterministic ties resolve by source precedence: `landing_session_id`, then `checkout_token`, then `cart_token`, then `customer_identity`
- if precedence also ties, click-ID presence wins, then the lexicographically smaller `sessionId`

This means stitched identity fallback can contribute the winning touch only when stronger deterministic evidence is absent or when it is the latest eligible non-direct candidate after the winner rules are applied.

## Shopify Hint Fallback Boundary

Shopify-hint-derived attribution is not part of deterministic identity stitching.

Boundary rules:

- it is considered only when deterministic resolution fails to recover a landing session match
- it is recovery-only fallback behavior and must not override a deterministic winner
- it writes synthetic attribution with `attribution_reason = shopify_hint_derived`
- the fallback row can carry attributed marketing dimensions while still having `session_id = null`

Refer to `docs/last-non-direct-touch-approval-matrix.md` for the full approved matrix and caveats.
