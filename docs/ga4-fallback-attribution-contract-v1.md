# GA4 Fallback Attribution Contract v1

## Purpose

Define the only approved conditions under which GA4-derived attribution may be written into ROAS Radar order attribution outputs.

This contract extends the existing deterministic and Shopify synthetic fallback behavior documented in:

- `docs/operational-attribution-contracts.md`
- `docs/analytics-playbook.md`
- `docs/last-non-direct-touch-approval-matrix.md`
- `docs/attribution-schema-v1.md`

## Contract Status

- Version: `v1`
- Intended committed spec path: `docs/ga4-fallback-attribution-contract-v1.md`
- Change discipline: any behavior change requires coordinated updates to docs, resolver code, persistence/schema, and tests.

## Scope

This contract governs:

- GA4 fallback eligibility
- precedence against deterministic matching and Shopify hint fallback
- required input and output schema for resolver and downstream consumers
- confidence labels for GA4 fallback outcomes
- mandatory provenance tagging via `match_source = 'ga4_fallback'`

It does not redefine:

- deterministic candidate collection rules
- Shopify hint extraction rules
- multi-touch model math
- ad-platform spend normalization

## Approved Precedence

ROAS Radar order attribution must evaluate match paths in this order:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. `customer_identity`
5. `shopify_hint_fallback`
6. `ga4_fallback`
7. `unattributed`

### Hard rule

GA4 is eligible only when both are true:

1. no first-party deterministic winner exists
2. no Shopify hint fallback match exists

GA4 must never override:

- any deterministic winner
- any Shopify hint-derived fallback winner already approved by contract

## Required Resolver Inputs

The GA4 fallback decision step must receive a normalized object with at least:

```ts
{
  shopifyOrderId: string;
  orderOccurredAt: string;
  sourceName: string | null;
  landingSessionId: string | null;
  checkoutToken: string | null;
  cartToken: string | null;
  customerIdentityId: string | null;
  emailHash: string | null;
  deterministicResult: {
    winnerExists: boolean;
    winnerSessionId: string | null;
    attributionReason: string | null;
  };
  shopifyHintResult: {
    matchExists: boolean;
    source: string | null;
    medium: string | null;
    campaign: string | null;
    content: string | null;
    term: string | null;
    clickIdType: string | null;
    clickIdValue: string | null;
    confidenceScore: number | null;
  };
  ga4Candidates: Array<{
    ga4ClientId: string | null;
    ga4SessionId: string | null;
    occurredAt: string;
    source: string | null;
    medium: string | null;
    campaign: string | null;
    content: string | null;
    term: string | null;
    clickIdType: string | null;
    clickIdValue: string | null;
    transactionId: string | null;
    sessionHasRequiredFields: boolean;
  }>;
}
```

### Input expectations

- `source_name` must still gate to Shopify web orders only.
- `ga4Candidates` may be empty.
- Missing `landingSessionId`, `checkoutToken`, `cartToken`, or `customerIdentityId` do not make GA4 eligible by themselves; they only contribute to deterministic failure.
- GA4 candidate timestamps after the order timestamp are ineligible.

## Eligibility Rules

A GA4 fallback winner may be selected only when all are true:

- order is a Shopify web order
- deterministic result has no winner
- Shopify hint result has no match
- at least one eligible GA4 candidate remains after filtering

A GA4 candidate is eligible only when all are true:

- `occurredAt <= orderOccurredAt`
- candidate falls within the same attribution window used by deterministic resolution unless a separate GA4 window is explicitly documented
- candidate has at least one attribution signal:
  - any non-null canonical UTM field, or
  - any supported click ID
- candidate is not rejected for missing required session identity fields under the rules below

## Multiple Candidate Selection Rules

If multiple eligible GA4 candidates remain:

1. prefer the latest `occurredAt`
2. if tied, prefer a candidate with a supported click ID
3. if still tied, prefer the candidate with more populated canonical dimensions among `source`, `medium`, `campaign`, `content`, `term`
4. if still tied, prefer the lexicographically smaller stable identifier in this order:
   - `ga4SessionId`
   - `ga4ClientId`
   - `transactionId`
5. if still tied, select deterministically by original sorted order and persist that same order in tests

ROAS Radar must not create multiple primary fallback rows for the same order.

## Confidence Contract

### Numeric scores

GA4 fallback confidence must be below deterministic and below Shopify click-ID synthetic fallback.

Approved initial scores:

- `0.35` for GA4 fallback with supported click ID
- `0.25` for GA4 fallback with canonical UTMs but no click ID
- `0.00` when no GA4 winner exists

### Confidence labels

Persist a label alongside or derivable from the numeric score using this contract:

- `high`: deterministic exact match (`1.00`, `0.90`)
- `medium`: deterministic stitched identity (`0.60`)
- `low`: synthetic fallback (`0.55`, `0.40`, `0.35`, `0.25`)
- `none`: unattributed (`0.00`)

For GA4 fallback specifically:

- click-ID-backed GA4 fallback => `confidence_label = 'low'`
- UTM-only GA4 fallback => `confidence_label = 'low'`

## Mandatory Output Contract

Any GA4 fallback write must emit:

```ts
{
  sessionId: null;
  matchSource: 'ga4_fallback';
  attributionReason: 'ga4_fallback_derived';
  confidenceScore: 0.35 | 0.25;
  confidenceLabel: 'low';
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  ga4ClientId: string | null;
  ga4SessionId: string | null;
  occurredAt: string;
}
```

### Persistence requirements

The equivalent `match_source='ga4_fallback'` tag is mandatory in all downstream-facing surfaces that persist attribution state, including:

- `attribution_results`
- `attribution_order_credits`
- `shopify_orders.attribution_snapshot.winner`
- `shopify_orders.attribution_snapshot.timeline[]`
- any reporting or API response shape that currently exposes fallback provenance

If the existing schema lacks `match_source`, add it rather than overloading `ingestionSource` or `attributionReason`.

### Provenance semantics

- `match_source` answers where the winning match came from
- `attribution_reason` answers why the system chose it
- `session_id` remains `null` for GA4 fallback because no first-party ROAS Radar session was resolved

## Edge Cases

### Missing session fields

- If deterministic fields are missing and GA4 has a valid candidate, GA4 may still win, subject to normal precedence.
- If a GA4 candidate is missing both `ga4ClientId` and `ga4SessionId`, it is ineligible unless a stable `transactionId` plus usable attribution dimensions are present and that behavior is explicitly tested.
- If a GA4 candidate lacks attribution dimensions and lacks click IDs, it is ineligible.

### Multiple candidates

- Resolve using the deterministic tie-break rules above.
- Persist only one primary GA4 fallback winner.
- Snapshot timeline may contain all evaluated GA4 fallback candidates only if the implementation explicitly models them; otherwise persist only the chosen winner and document that behavior.

### Null click IDs

- Null click IDs are allowed.
- A candidate with null click IDs may still win when canonical UTMs are present.
- Null click IDs must lower the numeric confidence from `0.35` to `0.25`.

### Direct or empty GA4 traffic

- A GA4 candidate with no canonical UTM dimensions and no click ID is treated as direct/empty and is ineligible for fallback.
- GA4 must not be used to manufacture direct attribution.

### Existing unattributed rows

- GA4 fallback may replace an `unattributed` result for the same order.
- GA4 fallback must not replace deterministic or Shopify fallback outcomes.

## Precedence Matrix

| Match path available | Winner | Required tags | Session ID | Confidence |
| --- | --- | --- | --- | --- |
| `landing_session_id` present and resolved | deterministic landing | `match_source='landing_session_id'` | resolved UUID | `1.00`, `high` |
| no landing winner, checkout token resolved | deterministic checkout | `match_source='checkout_token'` | resolved UUID | `1.00`, `high` |
| no stronger winner, cart token resolved | deterministic cart | `match_source='cart_token'` | resolved UUID | `0.90`, `high` |
| no stronger winner, identity session resolved | deterministic identity | `match_source='customer_identity'` | resolved UUID | `0.60`, `medium` |
| no deterministic winner, Shopify hint present | Shopify synthetic fallback | `match_source='shopify_hint_fallback'` | `null` | `0.55` or `0.40`, `low` |
| no deterministic winner, no Shopify hint winner, GA4 candidate selected | GA4 fallback | `match_source='ga4_fallback'` | `null` | `0.35` or `0.25`, `low` |
| no eligible path | unattributed | `match_source='unattributed'` | `null` | `0.00`, `none` |

## Implementation Notes

- Current synthetic fallback code should stop encoding fallback provenance as `ingestionSource='customer_identity'` in snapshots.
- Introduce a first-class fallback provenance field or enum shared by resolver, persistence, and API readers.
- Prefer additive schema changes over reusing deterministic-only types.
- Update operator and analyst docs to explain that GA4 fallback is weaker than Shopify hint fallback.

## Required Test Coverage

Add tests for:

- deterministic winner suppresses GA4 fallback
- Shopify hint winner suppresses GA4 fallback
- GA4 click-ID candidate wins when deterministic and Shopify hint both fail
- GA4 UTM-only candidate wins when click IDs are null
- multiple GA4 candidates choose latest eligible timestamp
- same-timestamp GA4 tie prefers click ID
- same-timestamp no-click tie prefers richer dimensions
- completely empty GA4 candidate is rejected
- future-dated GA4 candidate is rejected
- order remains unattributed when every GA4 candidate is ineligible

## Cross-Document Updates Required

When implementing this contract, update:

- `docs/operational-attribution-contracts.md`
- `docs/analytics-playbook.md`
- `docs/last-non-direct-touch-approval-matrix.md`
- `docs/attribution-schema-v1.md`
- resolver and integration tests covering fallback precedence
