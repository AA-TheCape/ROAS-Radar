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
