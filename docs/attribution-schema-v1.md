# Attribution Schema V1

`@roas-radar/attribution-schema` is the shared contract for first-party attribution capture across browser scripts, backend ingestion, Shopify cart and order attributes, and downstream readers. All producers and consumers of attribution capture data should treat [packages/attribution-schema/index.ts](../packages/attribution-schema/index.ts) as the source of truth for field names, normalization behavior, and maximum lengths.

The current schema version is `1`.

## Canonical Package

Source of truth:

- `packages/attribution-schema/index.ts`

The package defines:

- `ATTRIBUTION_SCHEMA_VERSION = 1`
- field groups for URLs, UTM parameters, and click IDs
- normalization helpers
- the Zod contract for `AttributionCaptureV1`
- consent-state normalization

Writers should normalize through the shared package whenever possible. Readers must assume payloads may arrive from older writers and should remain tolerant as described below.

## Canonical Field Set

`AttributionCaptureV1` contains these fields:

Required:

- `schema_version`
- `roas_radar_session_id`
- `occurred_at`
- `captured_at`

Nullable URL fields:

- `landing_url`
- `referrer_url`
- `page_url`

Nullable UTM fields:

- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`

Nullable click ID fields:

- `gclid`
- `gbraid`
- `wbraid`
- `fbclid`
- `ttclid`
- `msclkid`

Related shared enum:

- consent state: `granted | denied | unknown`

## Field Semantics

- `schema_version`: payload contract version. Writers must currently send `1`.
- `roas_radar_session_id`: durable UUID for the ROAS Radar session. This is the primary cross-system join key and must be written into Shopify cart and order attributes as early as possible.
- `occurred_at`: when the user interaction or observed touch happened.
- `captured_at`: when ROAS Radar captured or emitted the record.
- `landing_url`: the first landing URL preserved for the session when known.
- `referrer_url`: the best available referrer for the capture or current page.
- `page_url`: the page where the capture event occurred.
- `utm_*`: campaign dimensions. These are semantic marketing dimensions and are normalized to lowercase.
- click IDs: raw platform identifiers. These are preserved as trimmed strings and are not lowercased.

## Normalization Rules

Normalization behavior is defined by the shared package and should not be reinterpreted ad hoc.

String normalization:

- trim leading and trailing whitespace
- convert empty strings to `null`

UTM normalization:

- trim whitespace
- convert empty strings to `null`
- lowercase all non-null values

Click ID normalization:

- trim whitespace
- convert empty strings to `null`
- preserve original casing otherwise

URL normalization:

- trim whitespace
- convert empty strings to `null`
- require `http` or `https`
- resolve relative values only when a trusted base URL is explicitly provided
- strip URL fragments
- serialize to normalized absolute URL string

Timestamp normalization:

- accept ISO-8601 timestamp strings matching the package regex
- normalize output to canonical `toISOString()` format

Session ID normalization:

- must be a UUID
- must fit within the shared max length

## Maximum Lengths

These limits are contract-level and are also reflected in database constraints.

- URL fields: `2048`
- UTM fields: `255`
- click ID fields: `255`
- `roas_radar_session_id`: `36`

Implications:

- writers must truncate or reject before persistence rather than relying on database failures
- readers must not assume values longer than these limits are valid schema-compliant inputs
- new storage locations must preserve at least these lengths

## Database And Storage Mapping

The schema maps into two main PostgreSQL tables introduced for attribution capture.

### `session_attribution_identities`

Purpose: durable per-session identity and first-touch capture snapshot.

Field mapping:

- `roas_radar_session_id` -> `roas_radar_session_id`
- `landing_url` -> `landing_url`
- `referrer_url` -> `referrer_url`
- `utm_source` -> `initial_utm_source`
- `utm_medium` -> `initial_utm_medium`
- `utm_campaign` -> `initial_utm_campaign`
- `utm_content` -> `initial_utm_content`
- `utm_term` -> `initial_utm_term`
- `gclid` -> `initial_gclid`
- `gbraid` -> `initial_gbraid`
- `wbraid` -> `initial_wbraid`
- `fbclid` -> `initial_fbclid`
- `ttclid` -> `initial_ttclid`
- `msclkid` -> `initial_msclkid`
- `occurred_at` and `captured_at` inform `first_captured_at` and `last_captured_at` lifecycle handling rather than mapping 1:1 to a single column

### `session_attribution_touch_events`

Purpose: event-level attribution capture history.

Field mapping:

- `roas_radar_session_id` -> `roas_radar_session_id`
- `occurred_at` -> `occurred_at`
- `captured_at` -> `captured_at`
- `page_url` -> `page_url`
- `referrer_url` -> `referrer_url`
- `utm_source` -> `utm_source`
- `utm_medium` -> `utm_medium`
- `utm_campaign` -> `utm_campaign`
- `utm_content` -> `utm_content`
- `utm_term` -> `utm_term`
- `gclid` -> `gclid`
- `gbraid` -> `gbraid`
- `wbraid` -> `wbraid`
- `fbclid` -> `fbclid`
- `ttclid` -> `ttclid`
- `msclkid` -> `msclkid`

Additional event storage outside the package contract:

- `event_type`
- `shopify_cart_token`
- `shopify_checkout_token`
- `ingestion_source`
- `raw_payload`

### Shopify order snapshot

`shopify_orders.attribution_snapshot` stores a JSON snapshot for later reconciliation and operational comparison. Its shape should remain aligned with the shared schema field names and canonical Shopify attribute keys.

## Shopify Attribute Key Conventions

Canonical Shopify attribute keys are the unprefixed shared schema field names plus `schema_version`.

Canonical keys:

- `schema_version`
- `roas_radar_session_id`
- `landing_url`
- `referrer_url`
- `page_url`
- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`
- `gclid`
- `gbraid`
- `wbraid`
- `fbclid`
- `ttclid`
- `msclkid`

Writer rules:

- new writers must emit canonical unprefixed keys
- Shopify cart propagation should write `schema_version` and `roas_radar_session_id` on the earliest cart mutation opportunity
- if a value is null or absent, writers may omit the Shopify attribute rather than writing an empty string

Reader rules:

- readers must first accept canonical keys
- readers must also accept legacy prefixed keys during rollout, including forms like `roas_radar_utm_source`, `roas_radar_gclid`, and related prefixed variants
- `roas_radar_session_id` remains canonical and should be treated as required when present

## Backward Compatibility Expectations

### Writers

- always write `schema_version = 1`
- use canonical field names exactly as defined in the shared package
- normalize before write
- prefer additive rollout over breaking replacement
- do not introduce renamed keys, alternate casing, or source-specific aliases outside documented compatibility windows

### Readers

- must tolerate missing nullable fields
- must tolerate absent `schema_version` only when handling explicitly documented legacy Shopify data paths
- must continue reading both canonical and legacy prefixed Shopify attribute keys until the rollout window is formally closed
- must treat unknown extra fields as ignorable unless a stricter validation boundary is intentionally applied

### Rollout discipline

- package changes are contract changes and must be coordinated across browser capture, backend ingestion, Shopify writeback, and reporting consumers
- adding a new optional field is backward-compatible only if existing readers ignore unknown fields and storage supports the new column or snapshot field safely
- renaming or removing fields is not backward-compatible and requires a new schema version plus a migration plan
- changing normalization semantics is a contract change and must be documented as such

## Operational Rules For Schema Evolution

Use these rules for future revisions.

A new schema version is required when:

- a field is renamed or removed
- a field meaning changes incompatibly
- normalization semantics change incompatibly
- a required field is added
- existing readers would misinterpret newly written data

A new schema version is usually not required when:

- an optional field is added in an additive way
- documentation is clarified without changing runtime behavior
- readers become more permissive while writers remain canonical

## Related Docs

- [Implementation Guide](implementation-guide.md)
- [Shopify App Setup](shopify-app-setup.md)
- [Visitor Identity Stitching](visitor-identity-stitching.md)
- [Analytics Playbook](analytics-playbook.md)
