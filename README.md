# ROAS-Radar
Version 0.01

## Shopify Storefront Session Propagation

This repository now includes a Shopify theme integration that preserves the ROAS Radar browser `session_id` into Shopify cart attributes before checkout. That gives the backend a stable path to recover the originating session from order payloads and attribute orders reliably.

### Files

- `shopify/theme/assets/roas-radar-session-propagation.js`
- `shopify/theme/snippets/roas-radar-session-propagation.liquid`

### What it does

- Reads the existing ROAS Radar session identifier from:
  - `window.__ROAS_RADAR_SESSION_ID`
  - the `roas_radar_session_id` cookie
  - `sessionStorage` / `localStorage`
- Writes the session into Shopify cart attributes under `roas_radar_session_id`
- Writes the current landing path into `roas_radar_landing_path`
- Injects hidden cart form fields so non-AJAX cart flows still carry the same metadata
- Syncs attributes on page load and again when checkout/cart forms are submitted

### Shopify theme install

1. Copy `shopify/theme/assets/roas-radar-session-propagation.js` into your theme assets.
2. Copy `shopify/theme/snippets/roas-radar-session-propagation.liquid` into your theme snippets.
3. Render the snippet in `theme.liquid` before `</body>`:

```liquid
{% render 'roas-radar-session-propagation' %}
```

If your tracking cookie uses a different name, pass it explicitly:

```liquid
{% render 'roas-radar-session-propagation',
  cookie_name: 'custom_roas_session',
  cart_attribute_key: 'roas_radar_session_id',
  landing_path_attribute_key: 'roas_radar_landing_path'
%}
```

### Order-side expectation

With this snippet installed, Shopify cart and checkout flows should carry:

- `attributes.roas_radar_session_id`
- `attributes.roas_radar_landing_path`

Your webhook normalization layer should map `attributes.roas_radar_session_id` into `shopify_orders.landing_session_id` when present. That satisfies the MVP requirement to prefer an exact session match before token or email fallbacks.
