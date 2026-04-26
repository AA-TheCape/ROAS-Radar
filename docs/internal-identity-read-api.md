# Internal Identity Read API

These endpoints expose canonical identity graph state for internal debugging and analytics consumers.

## Authorization

- Base path: `/api/internal/identity`
- Authentication: `Authorization: Bearer <REPORTING_API_TOKEN>`
- Interactive app sessions are not accepted, even for admins
- Responses never include plaintext email or phone fields
- Contact lookup and response fields are limited to approved hashes:
  - `hashed_email`
  - `phone_hash`

## Endpoints

### `GET /api/internal/identity/lookup`

Looks up the active canonical journey attached to a single identity node.

Query parameters:

- `nodeType`: one of `session_id`, `checkout_token`, `cart_token`, `shopify_customer_id`, `hashed_email`, `phone_hash`
- `nodeKey`: normalized identifier value

Validation rules:

- `hashed_email` and `phone_hash` must be a 64-character SHA-256 hex digest
- `session_id` must be a UUID

Example:

```http
GET /api/internal/identity/lookup?nodeType=hashed_email&nodeKey=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
Authorization: Bearer <REPORTING_API_TOKEN>
```

### `GET /api/internal/identity/journeys/:journeyId`

Returns the same response shape when the canonical `identity_journey_id` is already known.

## Response Shape

```json
{
  "lookup": {
    "nodeType": "hashed_email",
    "nodeKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "journey": {
    "journeyId": "11111111-1111-4111-8111-111111111111",
    "status": "active",
    "authoritativeShopifyCustomerId": "sc-1",
    "primaryIdentifiers": {
      "hashedEmail": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "phoneHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    },
    "mergeVersion": 3,
    "mergedIntoJourneyId": null,
    "lookbackWindow": {
      "startedAt": "2026-04-01T00:00:00.000Z",
      "expiresAt": "2026-05-01T00:00:00.000Z",
      "lastTouchEligibleAt": "2026-04-30T00:00:00.000Z"
    },
    "createdAt": "2026-04-01T00:00:00.000Z",
    "updatedAt": "2026-04-25T12:00:00.000Z",
    "lastResolvedAt": "2026-04-25T12:00:00.000Z"
  },
  "identifiers": {
    "total": 2,
    "activeCount": 2,
    "ambiguousCount": 0,
    "nodes": [
      {
        "edgeId": "edge-1",
        "nodeId": "node-1",
        "nodeType": "shopify_customer_id",
        "nodeKey": "sc-1",
        "isAuthoritative": true,
        "isAmbiguous": false,
        "edgeType": "authoritative",
        "precedenceRank": 100,
        "evidenceSource": "shopify_order_webhook",
        "sourceTable": "shopify_orders",
        "sourceRecordId": "order-1",
        "isActive": true,
        "conflictCode": null,
        "firstObservedAt": "2026-04-02T00:00:00.000Z",
        "lastObservedAt": "2026-04-25T12:00:00.000Z",
        "createdAt": "2026-04-02T00:00:00.000Z",
        "updatedAt": "2026-04-25T12:00:00.000Z"
      }
    ]
  },
  "timeline": {
    "sessions": [
      {
        "sessionId": "22222222-2222-4222-8222-222222222222",
        "startedAt": "2026-04-10T10:00:00.000Z",
        "endedAt": "2026-04-10T10:15:00.000Z",
        "journeySessionNumber": 1,
        "reverseJourneySessionNumber": 1,
        "metrics": {
          "eventCount": 4,
          "pageViewCount": 2,
          "productViewCount": 1,
          "addToCartCount": 1,
          "checkoutStartedCount": 1,
          "orderCount": 1,
          "orderRevenue": 88.5
        },
        "flags": {
          "isFirstSession": true,
          "isLastSession": true,
          "isConvertingSession": true
        },
        "acquisition": {
          "anonymousUserId": "anon-1",
          "landingPage": "https://store.example.com/products/widget",
          "referrerUrl": "https://www.google.com/",
          "utmSource": "google",
          "utmMedium": "cpc",
          "utmCampaign": "spring",
          "utmContent": "hero",
          "utmTerm": "widget",
          "gclid": "gclid-1",
          "gbraid": null,
          "wbraid": null,
          "fbclid": null,
          "ttclid": null,
          "msclkid": null
        }
      }
    ],
    "orders": [
      {
        "shopifyOrderId": "order-1",
        "shopifyOrderNumber": "1001",
        "shopifyCustomerId": "sc-1",
        "emailHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "currencyCode": "USD",
        "totalPrice": 88.5,
        "financialStatus": "paid",
        "fulfillmentStatus": "fulfilled",
        "processedAt": "2026-04-10T10:20:00.000Z",
        "createdAtShopify": "2026-04-10T10:19:00.000Z",
        "updatedAtShopify": "2026-04-10T10:21:00.000Z",
        "landingSessionId": "22222222-2222-4222-8222-222222222222",
        "checkoutToken": "co-1",
        "cartToken": "ca-1",
        "sourceName": "web",
        "ingestedAt": "2026-04-10T10:21:30.000Z"
      }
    ]
  }
}
```

## Error Codes

- `401 unauthorized`: bearer token missing or invalid
- `403 forbidden`: authenticated app user session supplied instead of the internal service token
- `404 identity_lookup_not_found`: no active journey matched the supplied node
- `404 journey_not_found`: journey id does not exist
- `400 invalid_request`: invalid node type, unhashed contact lookup, or malformed journey UUID
