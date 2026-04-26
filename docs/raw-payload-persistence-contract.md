# Raw Payload Persistence Contract

This document defines how ROAS Radar stores raw external-source payloads in JSONB so ingestion changes do not silently trim, subset, normalize, or reconstruct source records before persistence.

Use it together with:

- `docs/attribution-schema-v1.md` for first-party capture field names, normalization rules, and storage mappings
- `docs/implementation-guide.md` for local setup, service topology, and ingestion workflow
- `docs/operational-attribution-contracts.md` for resolver, writeback, retention, and recovery behavior

## Goal

Raw source payloads from supported external ingestion modules must be persisted exactly as received after transport decoding and JSON parsing only.

No other transformation is allowed between source receipt and raw JSONB persistence.

## Scope

This contract applies to the external ingestion families currently implemented in the repository:

- Shopify ingestion in `src/modules/shopify/index.ts`
- Meta Ads ingestion in `src/modules/meta-ads/index.ts`
- Google Ads ingestion in `src/modules/google-ads/index.ts`

It applies to JSONB columns intended to retain source payloads, including:

- `shopify_webhook_receipts.raw_payload`
- `shopify_orders.raw_payload`
- `meta_ads_raw_spend_records.raw_payload`
- `meta_ads_connections.raw_account_data`
- `google_ads_raw_spend_records.raw_payload`
- `google_ads_connections.raw_customer_data`

This contract does not apply to derived or normalized tables whose job is projection, reporting, or canonicalization rather than raw-source retention.

## Allowed Transformations

Only these transformations are allowed before writing to a covered raw JSONB column:

- transport decoding required to read the payload, such as gzip or equivalent content-encoding decode
- character decoding required to interpret the transport body as text
- JSON parsing into the in-memory object or array structure that is then written to JSONB

## Prohibited Transformations

The following are not allowed before persistence into a covered raw JSONB column:

- dropping keys
- projecting a schema-defined subset
- renaming keys
- lowercasing, trimming, or normalizing values
- rebuilding a new object from selected fields
- storing only reporting-safe or canonicalized fields in place of the source object
- injecting ROAS Radar derived metadata into the raw source object

## Source-Specific Requirements

### Shopify

For Shopify webhooks and Shopify order backfills:

- `shopify_webhook_receipts.raw_payload` must equal the decoded-and-parsed source payload
- `shopify_orders.raw_payload` must equal the decoded-and-parsed source payload used for normalization
- normalized order columns may be derived from the payload, but `raw_payload` must remain unchanged
- raw line-item storage must use the original decoded line-item node rather than a schema-reduced line-item object

### Meta Ads

For Meta Ads account metadata and daily insight rows:

- `meta_ads_connections.raw_account_data` must equal the decoded-and-parsed account payload returned by Meta
- each `meta_ads_raw_spend_records.raw_payload` row must equal the decoded-and-parsed insight row returned by Meta
- normalized spend dimensions must not be injected into raw JSONB columns

### Google Ads

For Google Ads customer metadata and spend rows:

- `google_ads_connections.raw_customer_data` must equal the decoded-and-parsed customer payload returned by Google Ads
- each `google_ads_raw_spend_records.raw_payload` row must equal the decoded-and-parsed Google Ads API row used to create that raw spend record
- normalized spend dimensions must not replace the original source row in raw JSONB columns

## Invariants

### Invariant 1: Exact parsed-payload equality

The JSON value written to a covered raw JSONB column must deep-equal the source payload after transport decode and JSON parse.

Fail examples:

- a source key is missing in stored JSONB
- a value differs because it was trimmed, lowercased, coerced, or reshaped
- nested objects or arrays are partially omitted

### Invariant 2: No schema-only subset storage

Fields not modeled in normalized columns must still remain present in raw JSONB unchanged.

### Invariant 3: No enrichment inside raw JSONB

Covered raw JSONB columns must not contain ROAS Radar derived fields that were not present in the source payload.

### Invariant 4: Source coverage

Tests that enforce raw-payload exactness must cover all three external ingestion families:

- Shopify
- Meta Ads
- Google Ads

## Derived Tables

Derived tables may continue to store normalized or reporting-oriented projections.

Current examples:

- `meta_ads_daily_spend`
- `google_ads_daily_spend`

Those tables are not canonical raw-source storage and must not be used to justify changing covered raw JSONB persistence behavior.

## Change Discipline

Any change to covered ingestion behavior must update:

- this contract document when the documented guarantee or surface area changes
- nearby source references in the relevant ingestion module
- tests that verify exact parsed-payload persistence for the affected source family

Any PR that changes covered raw-source persistence behavior without updating docs and tests should be treated as incomplete.
