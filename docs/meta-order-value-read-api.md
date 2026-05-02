# Meta Order Value Read API

This document defines the backend read contract for the Meta order value dashboard card.

Use it with:

- `docs/meta-attributed-revenue-contract-v1.md` for the normalized aggregate storage contract
- `src/modules/reporting/meta-order-value.ts` for the request validation and query implementation
- `test/meta-order-value-read-api.integration.test.ts` for auth, scoping, and filtering coverage

## Endpoint

- Method: `GET`
- Path: `/api/reporting/meta-order-value`
- Auth: standard reporting auth middleware
  - app user session bearer token is allowed
  - internal reporting bearer token is allowed

## Tenant And Organization Scope

- The route always scopes reads to one `organization_id`
- Default scope is `DEFAULT_ORGANIZATION_ID`
- Internal service callers may override the scope with `x-roas-radar-tenant-id: <positive integer>`
- Authenticated app-user sessions must not override the tenant to a different organization id

## Query Parameters

Required:

- `startDate`: `YYYY-MM-DD`
- `endDate`: `YYYY-MM-DD`

Optional filters:

- `campaignIds`: comma-delimited campaign ids or repeated query values
- `campaignSearch`: case-insensitive match against `campaign_id` or `campaign_name`
- `actionType`: exact canonical action type filter

Optional pagination:

- `limit`: integer, default `50`, max `200`
- `offset`: integer, default `0`

Optional sorting:

- `sortBy`: one of `reportDate`, `campaignName`, `attributedRevenue`, `purchaseCount`, `spend`, `roas`, `actionType`
- `sortDirection`: `asc` or `desc`

Default ordering:

- `reportDate desc`
- secondary `attributedRevenue desc`
- secondary `campaignId asc`

## Fixed Query Semantics

The route reads only the dashboard’s canonical Meta order value surface:

- `action_report_time = conversion`
- `use_account_attribution_setting = true`

This prevents duplicate campaign-day rows if additional reporting variants are stored later.

## Response Shape

```json
{
  "scope": {
    "organizationId": 77
  },
  "range": {
    "startDate": "2026-04-28",
    "endDate": "2026-04-29"
  },
  "filters": {
    "campaignIds": ["cmp_1"],
    "campaignSearch": "Prospecting",
    "actionType": null
  },
  "sort": {
    "by": "reportDate",
    "direction": "desc"
  },
  "pagination": {
    "limit": 50,
    "offset": 0,
    "returned": 2,
    "totalRows": 2,
    "hasMore": false
  },
  "totals": {
    "attributedRevenue": 200,
    "purchaseCount": 3,
    "spend": 60,
    "roas": 3.3333333333333335
  },
  "rows": [
    {
      "date": "2026-04-29",
      "campaignId": "cmp_1",
      "campaignName": "Prospecting US",
      "attributedRevenue": 120,
      "purchaseCount": 2,
      "spend": 40,
      "roas": 3,
      "calculatedRoas": 3,
      "canonicalActionType": "omni_purchase",
      "canonicalSelectionMode": "fallback",
      "currency": "USD"
    }
  ]
}
```

## Metric Semantics

- `totals.roas` is computed from `SUM(attributedRevenue) / SUM(spend)` for the filtered result set
- `rows[].roas` is the stored Meta `purchase_roas` value for that campaign-day row
- `rows[].calculatedRoas` is derived from `attributedRevenue / spend` for UI comparison when the stored Meta ROAS is missing
- `canonicalActionType` and `canonicalSelectionMode` must reflect the ingestion selection stored in `meta_ads_order_value_aggregates`

## Performance Expectations

- The endpoint is intended for dashboard-card usage and must remain bounded by pagination defaults
- Read queries use `organization_id` plus `report_date` range predicates
- `db/migrations/0042_add_meta_order_value_read_index.sql` adds the dedicated index that supports this access pattern
