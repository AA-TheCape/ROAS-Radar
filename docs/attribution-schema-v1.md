# Attribution Schema V1

`@roas-radar/attribution-schema` is the canonical first-party capture contract for browser capture, backend ingestion, Shopify cart and order attributes, and downstream analytics readers. Treat [packages/attribution-schema/index.ts](../packages/attribution-schema/index.ts) as the runtime source of truth for field names, normalization, and contract limits.

This document is the exhaustive reader-facing contract for the current implementation. It covers:

- canonical schema fields
- adjacent operational fields that appear in the tracking pipeline
- normalization and validation rules
- consent behavior
- database and Shopify storage mappings
- backward-compatibility and schema evolution rules

Use `docs/raw-payload-persistence-contract.md` alongside this document when you are changing Shopify, Meta Ads, or Google Ads raw-source JSONB persistence. This document defines canonical attribution fields and normalization behavior. The raw-payload contract defines the stricter exact-as-received storage requirement for covered external-source JSONB columns.

The current schema version is `1`.

## Canonical Package

Source of truth:

- `packages/attribution-schema/index.ts`

The package currently exports:

- `ATTRIBUTION_SCHEMA_VERSION = 1`
- `MAX_ATTRIBUTION_URL_LENGTH = 2048`
- `MAX_ATTRIBUTION_TEXT_LENGTH = 255`
- `MAX_SESSION_ID_LENGTH = 36`
- URL, UTM, and click-ID field lists
- `attributionCaptureV1Schema`
- normalization helpers for strings, URLs, UTMs, click IDs, session IDs, and consent state

## Contract Scope

The canonical payload contract is `AttributionCaptureV1`.

Required fields:

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

- `consent_state`: `granted | denied | unknown`

The shared package defines the payload contract. The broader tracking pipeline persists adjacent operational fields alongside the capture data, including `shopify_cart_token`, `shopify_checkout_token`, `ingestion_source`, and `retained_until`. Those fields are documented here because engineering and analytics readers will encounter them in storage and operational tooling.

## Canonical Field Dictionary

| Field | Type | Required | Max length | Normalization | Meaning |
| --- | --- | --- | --- | --- | --- |
| `schema_version` | literal `1` | yes | n/a | must equal `1` | Version of the payload contract. Writers must currently emit `1`. |
| `roas_radar_session_id` | UUID string | yes | `36` | trim, validate UUID | Durable ROAS Radar session key used to join browser capture, tracking, Shopify propagation, and attribution resolution. |
| `occurred_at` | ISO-8601 timestamp string | yes | n/a | trim, validate, serialize with `toISOString()` | When the touch or event happened. |
| `captured_at` | ISO-8601 timestamp string | yes | n/a | trim, validate, serialize with `toISOString()` | When ROAS Radar assembled or persisted the payload. |
| `landing_url` | `string \| null` | no | `2048` | trim, empty to `null`, require `http/https`, strip fragment | First landing URL known for the session. Preserved as the first-touch landing value. |
| `referrer_url` | `string \| null` | no | `2048` | trim, empty to `null`, require `http/https`, strip fragment | Best available upstream referrer for the capture or session. |
| `page_url` | `string \| null` | no | `2048` | trim, empty to `null`, require `http/https`, strip fragment | Current page URL for the event or emitted capture. |
| `utm_source` | `string \| null` | no | `255` | trim, empty to `null`, lowercase | Marketing source dimension. |
| `utm_medium` | `string \| null` | no | `255` | trim, empty to `null`, lowercase | Marketing medium dimension. |
| `utm_campaign` | `string \| null` | no | `255` | trim, empty to `null`, lowercase | Marketing campaign dimension. |
| `utm_content` | `string \| null` | no | `255` | trim, empty to `null`, lowercase | Marketing content dimension. |
| `utm_term` | `string \| null` | no | `255` | trim, empty to `null`, lowercase | Marketing term or keyword dimension. |
| `gclid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | Google Ads click identifier. |
| `gbraid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | Google app-to-web click identifier. |
| `wbraid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | Google web-to-app click identifier. |
| `fbclid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | Meta click identifier. |
| `ttclid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | TikTok click identifier. |
| `msclkid` | `string \| null` | no | `255` | trim, empty to `null`, preserve case | Microsoft Ads click identifier. |
| `consent_state` | enum | pipeline field | n/a | normalize to `granted`, `denied`, or `unknown` | Consent status persisted alongside tracking and attribution touch events. Not part of `AttributionCaptureV1`, but part of the effective capture pipeline contract. |

## Adjacent Operational Field Dictionary

These fields are not part of the shared `AttributionCaptureV1` object, but they are stored next to capture data and routinely appear in queries, debugging, reconciliation, and retention jobs.

| Field | Type | Max length | Where it appears | Meaning |
| --- | --- | --- | --- | --- |
| `event_type` | text | implementation-defined | `tracking_events`, `session_attribution_touch_events` | Event category such as page view or other tracked interaction. Used to describe what emitted the capture. |
| `shopify_cart_token` | `string \| null` | `255` | `tracking_events`, `session_attribution_touch_events`, raw payload snapshots | Shopify cart token observed at capture time. Used for deterministic order/session stitching. |
| `shopify_checkout_token` | `string \| null` | `255` | `tracking_events`, `session_attribution_touch_events`, raw payload snapshots | Shopify checkout token observed at capture time. Used for deterministic order/session stitching. |
| `ingestion_source` | text | `64` in `session_attribution_touch_events` | `tracking_events`, `session_attribution_touch_events` | How the event entered the system. Current persisted values include `browser`, `server`, and `request_query`. |
| `raw_payload` | `jsonb` | n/a | `tracking_events`, `session_attribution_touch_events`, `shopify_orders` snapshots, ad raw spend tables | Storage surface for payload snapshots. For Shopify, Meta Ads, and Google Ads external-source raw tables, exact parsed-payload requirements are governed by `docs/raw-payload-persistence-contract.md`. Derived tables may retain normalized snapshots separately. |
| `retained_until` | timestamptz | n/a | `session_attribution_identities`, `session_attribution_touch_events`, `order_attribution_links` | Retention cutoff used by cleanup jobs. Not a business attribution field. |
| `first_captured_at` | timestamptz | n/a | `session_attribution_identities` | Earliest capture timestamp retained for the session snapshot lifecycle. |
| `last_captured_at` | timestamptz | n/a | `session_attribution_identities` | Latest capture timestamp retained for the session snapshot lifecycle. |

## Normalization Rules

Normalization is defined by the shared package and should not be reimplemented ad hoc.

### String handling

For nullable string fields:

1. trim leading and trailing whitespace
2. convert `''` to `null`
3. persist `string | null`, never raw `undefined`

This rule applies to URL fields, UTM fields, click IDs, and adjacent token fields where the pipeline accepts nullable strings.

Raw-source JSONB retention is stricter:

- these normalization rules apply to typed columns and canonical capture objects
- they do not authorize trimming, lowercasing, key projection, or reconstruction inside covered raw-source `raw_payload` columns
- Shopify, Meta Ads, and Google Ads external-source exactness rules are defined in `docs/raw-payload-persistence-contract.md`
- for ad-platform spend ingestion, this exactness requirement applies to `meta_ads_raw_spend_records.raw_payload` and `google_ads_raw_spend_records.raw_payload`, while `*_daily_spend.raw_payload` remains a derived projection field

### URL fields

Applies to `landing_url`, `referrer_url`, and `page_url`.

- trim whitespace
- empty string becomes `null`
- only `http` and `https` are allowed
- fragments are removed before persistence
- query strings are preserved
- values are serialized as normalized absolute URLs
- relative values may only be resolved when a trusted base URL is explicitly supplied

### UTM fields

Applies to `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, and `utm_term`.

- trim whitespace
- empty string becomes `null`
- lowercase non-null values

The lowercase rule is part of canonical persistence behavior and must not vary between browser, backend, and Shopify readers.

### Click ID fields

Applies to `gclid`, `gbraid`, `wbraid`, `fbclid`, `ttclid`, and `msclkid`.

- trim whitespace
- empty string becomes `null`
- preserve case otherwise

Click IDs are treated as attributable paid evidence even when UTMs are missing.

### Timestamp fields

Applies to `occurred_at` and `captured_at`.

- accept ISO-8601 timestamp strings matching the shared regex
- normalize to canonical `toISOString()` output
- reject invalid timestamps

The tracking ingestion layer may apply additional freshness checks for operational acceptance, but those runtime limits do not change the schema contract.

### Session ID

Applies to `roas_radar_session_id`.

- trim through shared string normalization where applicable
- must be a valid UUID
- must not exceed `36` characters

## Maximum Lengths

These limits are contract-level and are mirrored in database constraints where the values are persisted.

| Field family | Limit |
| --- | --- |
| URL fields | `2048` |
| UTM fields | `255` |
| click ID fields | `255` |
| `roas_radar_session_id` | `36` |
| `shopify_cart_token` | `255` |
| `shopify_checkout_token` | `255` |
| `ingestion_source` in `session_attribution_touch_events` | `64` |

Implications:

- writers should reject or truncate before persistence rather than relying on database failures
- readers should not treat over-limit values as schema-compliant
- new storage locations must preserve at least these lengths for the canonical field families

## Consent Behavior

Consent is part of the effective first-party capture contract even though it is stored adjacent to the shared capture payload rather than inside `AttributionCaptureV1`.

Allowed values:

- `granted`
- `denied`
- `unknown`

Rules:

- `normalizeAttributionConsentState` defaults missing or unspecified consent to `unknown`
- `tracking_events.consent_state` and `session_attribution_touch_events.consent_state` are both non-null and constrained to the allowed enum values
- downstream reporting or governance logic should filter on `consent_state` rather than inferring opt-out from missing attribution dimensions
- legacy payloads that carried `consentState` or `consent_state` in raw JSON were backfilled to the canonical enum during migration

## Database And Storage Mapping

The canonical fields are persisted across three main storage layers:

1. `tracking_sessions` and `tracking_events` for the operational web tracking pipeline
2. `session_attribution_identities` and `session_attribution_touch_events` for durable attribution capture
3. Shopify order snapshots and note attributes for cross-system propagation and reconciliation

### `tracking_sessions`

Purpose: session-level operational tracking state for the original tracking pipeline.

First-touch mappings:

- `roas_radar_session_id` aligns with `tracking_sessions.id`
- `landing_url` -> `landing_page`
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

Notes:

- `tracking_sessions` is an older operational table, but it remains part of the live pipeline.
- Session-level writes preserve the earliest accepted landing metadata via `COALESCE(...)` semantics in the ingestion code.

### `tracking_events`

Purpose: operational event stream for browser and mirrored server-side ingestion.

Event-level mappings:

- `roas_radar_session_id` -> `session_id`
- `occurred_at` -> `occurred_at`
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
- `consent_state` -> `consent_state`
- `shopify_cart_token` -> `shopify_cart_token`
- `shopify_checkout_token` -> `shopify_checkout_token`
- source metadata -> `ingestion_source`

Notes:

- `captured_at` is retained inside `raw_payload` for canonical attribution-capture mirror writes; it is not a first-class column in `tracking_events`.
- `tracking_events` can receive both browser-originated and mirrored server-side data so browser failures do not eliminate attribution evidence.

### `session_attribution_identities`

Purpose: durable per-session identity and first-touch attribution snapshot.

Field mappings:

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

Lifecycle fields encountered with the capture data:

- `first_captured_at`
- `last_captured_at`
- `retained_until`
- `customer_identity_id`

Notes:

- `landing_url` and all initial marketing dimensions are first-touch values and are preserved with `COALESCE(...)` semantics once set.
- `occurred_at` and `captured_at` influence lifecycle timestamps rather than mapping to single like-named columns here.

### `session_attribution_touch_events`

Purpose: durable event-level attribution touch history used by attribution resolution and operational diagnostics.

Field mappings:

- `roas_radar_session_id` -> `roas_radar_session_id`
- `event_type` -> `event_type`
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
- `consent_state` -> `consent_state`
- `shopify_cart_token` -> `shopify_cart_token`
- `shopify_checkout_token` -> `shopify_checkout_token`
- source metadata -> `ingestion_source`
- full debug snapshot -> `raw_payload`
- retention metadata -> `retained_until`

Notes:

- this table is the most complete event-level attribution capture history
- it includes both canonical schema fields and operational join fields used for deterministic order matching
- `raw_payload` keeps canonical field names for compatibility and troubleshooting on attribution-capture surfaces

### Shopify order snapshot

Purpose: preserve attribution data observed on Shopify orders for reconciliation and comparison.

Storage:

- `shopify_orders.attribution_snapshot`

Rules:

- snapshot JSON should align with canonical schema field names and Shopify attribute keys
- snapshot readers should tolerate legacy prefixed keys during rollout
- synthetic or fallback attribution logic should not mutate the shared field semantics

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

- writers must emit canonical unprefixed keys
- Shopify cart propagation should write `schema_version` and `roas_radar_session_id` at the earliest cart mutation opportunity
- nullable fields may be omitted rather than written as empty strings

Reader rules:

- readers must first accept canonical keys
- readers must also accept legacy prefixed forms during rollout, such as `roas_radar_utm_source` and `roas_radar_gclid`
- `roas_radar_session_id` remains canonical and should be treated as the primary durable join key when present

## Reader Expectations For Operational Fields

Engineering and analytics readers will often encounter additional columns alongside the canonical field set. Interpret them as follows:

- `shopify_cart_token` and `shopify_checkout_token` are deterministic stitching aids, not marketing dimensions
- `ingestion_source` explains provenance of the event record and should not be treated as campaign source/medium
- `retained_until` is retention metadata only and should not be surfaced as user-facing attribution time
- `raw_payload` in attribution-capture tables is mainly a debug and recovery surface; canonical analytics should prefer normalized top-level columns when they exist
- `raw_payload` in covered Shopify, Meta Ads, and Google Ads raw-source tables is governed by `docs/raw-payload-persistence-contract.md` and must remain exact parsed source payload

## Backward Compatibility Expectations

### Writers

- always write `schema_version = 1`
- use canonical field names exactly as defined in the shared package
- normalize before persistence or emission
- do not introduce source-specific aliases or alternate casing

### Readers

- must tolerate missing nullable fields
- must tolerate absent `schema_version` only on explicitly legacy paths
- must continue reading canonical and legacy prefixed Shopify keys during rollout
- should ignore unknown extra fields unless a stricter validation boundary is intentionally required

### Rollout discipline

- package changes are contract changes and must be coordinated across browser capture, backend ingestion, Shopify propagation, and reporting readers
- adding an optional field is backward-compatible only if readers ignore unknown fields and storage can accept the addition safely
- renaming or removing a field is not backward-compatible and requires a new schema version plus migration planning
- changing normalization semantics is a contract change even when field names stay the same

## Schema Evolution Rules

A new schema version is required when:

- a field is renamed or removed
- a field meaning changes incompatibly
- normalization semantics change incompatibly
- a new required field is added
- existing readers would misinterpret newly written data

A new schema version is usually not required when:

- an optional field is added additively
- readers become more permissive while writers stay canonical
- documentation is clarified without changing runtime behavior

## Related Docs

- [Implementation Guide](implementation-guide.md)
- [Raw Payload Persistence Contract](raw-payload-persistence-contract.md)
- [Shopify App Setup](shopify-app-setup.md)
- [Visitor Identity Stitching](visitor-identity-stitching.md)
- [Analytics Playbook](analytics-playbook.md)
