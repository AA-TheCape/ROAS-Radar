# Attribution QA Payload Schema V1

`AttributionQaPayloadV1` is the checked-in contract for per-order attribution QA exports. It is intended for debugging attribution outcomes, comparing candidate pools, and validating no-match cases without querying multiple storage tables by hand.

Runtime source of truth:

- `packages/attribution-schema/index.ts`
- `attributionQaPayloadV1Schema`
- `normalizeAttributionQaPayloadV1`
- `attributionQaPayloadV1JsonSchema`
- `attributionQaPayloadV1SuccessFixture`
- `attributionQaPayloadV1NoMatchFixture`

The current QA payload schema version is `1`.

## Top-Level Shape

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `schema_version` | literal `1` | yes | QA payload schema version. |
| `generated_at_utc` | ISO-8601 timestamp | yes | When the QA payload was assembled. |
| `order` | object | yes | Normalized order identifiers and commercial fields. |
| `outcome` | object | yes | Final tiered attribution outcome for the order. |
| `candidates` | object | yes | Candidate pools considered by the tier resolver. |
| `model_summaries` | `AttributionResultRecordV1[]` | yes | Attribution model summary records for the order. |
| `credits` | `AttributionCreditRecordV1[]` | yes | Touchpoint revenue credit records. Empty for no-match payloads. |
| `explainability` | `AttributionExplainRecordV1[]` | yes | Decision trace records used by QA tooling. |
| `diagnostics` | object | yes | Normalization failures and human-readable QA notes. |

All timestamps must include a timezone offset and are normalized to `toISOString()` output.

## Enums

`outcome.status`:

- `success`
- `no_match`

`outcome.attribution_tier`:

- `deterministic_first_party`
- `deterministic_shopify_hint`
- `ga4_fallback`
- `unattributed`

`match_source`:

- `landing_session_id`
- `checkout_token`
- `cart_token`
- `customer_identity`
- `stitched_identity_journey`
- `shopify_marketing_hint`
- `ga4_fallback`
- `unattributed`

`confidence_label`:

- `high`
- `medium`
- `low`
- `none`

`candidate_group`:

- `deterministic_first_party`
- `shopify_hint`
- `ga4_fallback`

## Order Object

| Field | Type | Meaning |
| --- | --- | --- |
| `order_id` | string | Shopify order identifier used by attribution storage. |
| `order_platform` | literal `shopify` | Source commerce platform. |
| `order_name` | `string \| null` | Merchant-facing order name or number. |
| `order_occurred_at_utc` | `timestamp \| null` | Best available order timestamp. |
| `order_timestamp_source` | enum or `null` | `processed_at`, `created_at_shopify`, or `ingested_at`. |
| `currency_code` | string | Uppercased currency code. |
| `subtotal_amount` | decimal string | Order subtotal. |
| `total_amount` | decimal string | Order total used for QA comparison. |
| `source_name` | `string \| null` | Shopify source name, commonly `web`. |
| `identifiers` | object | Join identifiers used by candidate extraction. |

`identifiers` contains `landing_session_id`, `checkout_token`, `cart_token`, `shopify_customer_id`, `email_hash`, and `identity_journey_id`. Empty strings normalize to `null`; UUID and email-hash fields are validated.

## Outcome Rules

A `success` payload must:

- use an attributed tier other than `unattributed`
- use a `match_source` other than `unattributed`
- include either `winner_touchpoint_id` or `winner_session_id`
- include at least one selected candidate

A `no_match` payload must:

- use `attribution_tier: unattributed`
- use `match_source: unattributed`
- use `confidence_score: 0`
- use `confidence_label: none`
- have no winner identifiers
- have no selected candidates

## Candidate Object

Each candidate records the resolver input that QA tooling needs to explain the outcome:

- candidate identity: `candidate_group`, `source_key`, `touchpoint_id`, `session_id`, `source_touch_event_id`
- timing: `occurred_at_utc`
- marketing dimensions: `source`, `medium`, `campaign`, `content`, `term`, `click_id_type`, `click_id_value`
- decision metadata: `match_source`, `attribution_reason`, `confidence_score`, `confidence_label`, `selected`
- classification flags: `is_direct`, `is_synthetic`

UTM-like dimensions normalize to lowercase. Click IDs preserve case.

## Fixtures

The package exports two validated fixture examples:

- `attributionQaPayloadV1SuccessFixture`: deterministic first-party success using `landing_session_id`
- `attributionQaPayloadV1NoMatchFixture`: unattributed no-match with no selected candidates and no credits

Use these fixtures in downstream QA tooling tests instead of constructing ad hoc payloads.

## Admin Debug Endpoint

`GET /api/admin/attribution/orders/{orderId}/qa-debug` returns the full internal QA debug response for one Shopify order. It is restricted to authenticated app users with admin access and does not accept internal service tokens, because the response includes unredacted order identifiers, raw Shopify hint payloads, raw tracking touchpoint payloads, and optional GA4 fallback candidate identifiers.

The `payload` field is an `AttributionQaPayloadV1` object and is validated by the same runtime schema described above. The surrounding debug envelope includes:

- `source`: `persisted_snapshot` or `generated_on_read`
- `selectedRunId` and `selectedRunReason`: the persisted attribution run used for raw evidence, or a clear no-run reason
- `evidenceState`: `available`, `missing`, or `expired_or_pruned` states for the attribution run, raw evidence, Shopify hints, touchpoints, and GA4 fallback candidate
- `rawShopifyHints`: raw `attribution_raw_evidence` rows where `evidence_type = shopify_hint`
- `rawTouchpoints`: raw `attribution_raw_evidence` rows where `evidence_type = tracking_touchpoint`
- `ga4FallbackCandidate`: the matching GA4 fallback candidate when one can still be loaded

When the Shopify order is absent the endpoint returns `404 shopify_order_not_found`. When a requested `runId` is absent for that order it returns `404 attribution_run_not_found`. If the long-retention order input exists but raw evidence rows have already been pruned, the response stays `200` and reports `expired_or_pruned` with empty raw evidence arrays.
