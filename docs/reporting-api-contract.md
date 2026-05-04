# Reporting API Contract

This document defines the backward-compatibility contract for authenticated reporting endpoints under `/api/reporting`.

Use it together with:

- `docs/analytics-playbook.md` for how reporting slices should be interpreted
- `docs/reporting-metrics.md` for KPI formulas
- `docs/database-operations.md` and the campaign metadata resolution contract for how resolved labels are stored

## Schema Versioning Strategy

ROAS Radar uses additive schema evolution for reporting responses.

- Existing response fields must not be renamed or removed in-place.
- New capabilities ship as additive fields on the same endpoint.
- The current reporting response schema version is `2026-05-02`.
- Every `/api/reporting/*` response includes `X-ROAS-Radar-Reporting-Schema: 2026-05-02`.
- Consumers that care about the contract version should read the response header rather than infer version from field presence.

This means no path-level version bump is required for additive reporting changes in this phase.

## Campaign Label Enrichment

Campaign-oriented responses may include metadata-resolution fields when the grouped row can be matched to the canonical metadata lookup table.

Affected endpoints:

- `GET /api/reporting/campaigns`
- `GET /api/reporting/spend-details`
- `GET /api/reporting/timeseries?groupBy=campaign`

## Backward-Compatible Fields

The following flat fields remain in place for existing consumers:

- `campaignDisplayName`
- `campaignEntityId`
- `campaignPlatform`
- `campaignNameResolutionStatus`

These fields are still supported and are the compatibility surface for legacy consumers already deployed against the reporting API.

## Forward Schema

New consumers should prefer the additive nested object:

```json
{
  "campaignLabel": {
    "displayName": "Google Brand Search Latest",
    "entityId": "cmp_google_1",
    "platform": "google_ads",
    "resolutionStatus": "resolved",
    "lastSeenAt": "2026-04-10T08:00:00.000Z",
    "updatedAt": "2026-04-10T08:05:00.000Z"
  }
}
```

Field meanings:

- `displayName`: resolved display label after lookup or fallback ordering
- `entityId`: native platform campaign id
- `platform`: `google_ads` or `meta_ads`
- `resolutionStatus`: `resolved`, `fallback_name`, or `unresolved`
- `lastSeenAt`: upstream observation timestamp from metadata sync when available
- `updatedAt`: lookup-row mutation timestamp when available

## Resolution Semantics

Display resolution follows the metadata contract:

1. Canonical metadata lookup `latest_name`
2. Native platform fallback name already present on the reporting row
3. Raw campaign id
4. No label field when the grouped row has no resolvable campaign metadata candidate

Reporting ids and grouping keys are not rewritten by this enrichment layer.

## Deprecation Guidance

No removal date is scheduled for the flat `campaignDisplayName` compatibility fields in this phase.

- New consumers should read `campaignLabel`.
- Existing consumers may continue reading the flat fields without change.
- A future removal would require a new documented schema version and an explicit migration window.
