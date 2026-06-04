# Deterministic Attribution Behavior

This page explains how ROAS Radar treats deterministic view and impression inputs in product reporting. Use it as the behavior guide for analysts, support, and engineers who need to explain why Clicks, Deterministic Views, Meta view-through, and combined comparison totals can differ.

Use it together with:

- [Meta Deterministic View Attribution Contract V1](meta-deterministic-view-attribution-contract-v1.md) for the source-of-truth contract
- [Meta Deterministic Ingestion](runbooks/meta-deterministic-ingestion.md) for scheduler, restart, rollback, and backfill operations
- [Reporting API Contract](reporting-api-contract.md) and [Reporting Metrics](reporting-metrics.md) for response compatibility and KPI formulas

## Scope

Deterministic view/impression behavior is Meta-only in v1.

In scope:

- Meta API view and impression aggregates retained by ROAS Radar
- Meta account, campaign, ad set, ad, report date, event type, and model output traceability
- deterministic view/impression model outputs stored separately from click attribution
- reporting comparisons that show Clicks and Deterministic Views as separate layers

Out of scope:

- Google Ads, GA4, Shopify, browser, or manually inferred view/impression inputs
- person-level Meta impression logs
- local-session guesses that infer ad views from pageviews, UTMs, referrers, or click ids
- any view/impression row that cannot be traced to retained Meta API evidence

When scope is uncertain, treat the row as out of scope until the Meta API provenance is proven.

## API-Only Verification Policy

Deterministic view/impression inputs are eligible only when verified through retained Meta API responses. The accepted evidence origin is API evidence, not local inference.

Allowed evidence:

- Meta Ads API or Marketing API response payloads retained by ingestion
- normalized deterministic facts and aggregates that preserve raw Meta traceability
- verified deterministic model outputs with `platform_verified = true`

Rejected or quarantined evidence:

- browser pageviews, tracking events, or first-party session timelines by themselves
- Shopify order metadata or recovered Shopify marketing hints
- GA4 events or session attribution exports
- UTMs, referrers, `fbclid`, or campaign ids without retained Meta API aggregate evidence
- negative, unparsable, duplicate, or ambiguous aggregate metrics

This policy is intentionally conservative. Missing Meta API evidence means no deterministic view/impression credit, even when other local evidence suggests a Meta interaction.

## Separate-Model Philosophy

Clicks remain the canonical reporting model. Deterministic Views are a separate model layer.

Required separation:

- Click attribution reads canonical click-attributed order credits from `daily_reporting_metrics`.
- Deterministic Views read API-verified model outputs from `deterministic_model_outputs`.
- Meta API view-through reads Meta-reported impression-time order value aggregates from `meta_ads_order_value_aggregates`.
- Combined reporting is comparison-only and must not be used as canonical revenue.

Deterministic view/impression model output must not:

- overwrite the primary attribution winner
- change Shopify writeback fields
- backfill first-party session winners
- be blended into a generic attributed revenue field without an explicit label

Use Deterministic Views to answer "what extra Meta API-verified view/impression model credit exists?" Use Clicks to answer "what is the canonical click-attributed reporting total?"

## Reporting Behavior

`GET /api/reporting/summary` defaults to Clicks.

Default behavior:

- omitted `reportingMode` means `reportingMode=clicks`
- `totals` contains Click attribution totals
- `totalsCanonical` is `true`
- `layers.deterministicViews` is still returned for side-by-side inspection
- `comparisonTotals.combined` is returned as a non-canonical Clicks plus Deterministic Views comparison

Supported summary reporting modes:

| `reportingMode` | Label | Canonical | Meaning |
| --- | --- | --- | --- |
| `clicks` | Click attribution | yes | Canonical reporting totals from click-attributed order credits. |
| `deterministic_views` | Deterministic view layer | no | Layer-only Meta API-verified deterministic view/impression model credit. |
| `meta_view_through` | Meta API view-through | no | Meta API-reported impression-time purchases, revenue, spend, and ROAS. |
| `combined` | Non-canonical comparison total | no | Comparison-only sum of Clicks and Deterministic Views. |

Current summary behavior also emits `X-ROAS-Radar-Reporting-Schema: 2026-05-27`.

## API Contract Examples

Default Clicks request:

```http
GET /api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10
```

Example shape:

```json
{
  "reportingMode": "clicks",
  "reportingModeLabel": "Click attribution",
  "totalsCanonical": true,
  "totals": {
    "visits": 100,
    "orders": 4,
    "revenue": 400,
    "spend": 100,
    "conversionRate": 0.04,
    "roas": 4
  },
  "comparisonTotals": {
    "combined": {
      "label": "Non-canonical comparison total",
      "canonical": false,
      "totals": {
        "visits": 100,
        "orders": 5.5,
        "revenue": 550,
        "spend": 100,
        "conversionRate": 0.055,
        "roas": 5.5
      }
    }
  },
  "layers": {
    "clicks": {
      "label": "Click attribution",
      "canonical": true,
      "totals": {
        "visits": 100,
        "orders": 4,
        "revenue": 400,
        "spend": 100,
        "conversionRate": 0.04,
        "roas": 4
      }
    },
    "deterministicViews": {
      "label": "Deterministic view layer",
      "canonical": false,
      "totals": {
        "visits": 0,
        "orders": 1.5,
        "revenue": 150,
        "spend": 0,
        "conversionRate": 0,
        "roas": null
      }
    }
  }
}
```

Deterministic Views layer-only request:

```http
GET /api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=deterministic_views
```

Expected behavior:

- `totals` equals `layers.deterministicViews.totals`
- `totalsCanonical` is `false`
- `comparisonTotals.combined` remains available but non-canonical
- visits and spend are usually `0` because the layer represents order credit from deterministic model outputs, not traffic or media-cost ownership

Meta API view-through request:

```http
GET /api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=meta_view_through&source=meta
```

Expected behavior:

- `totals` equals `layers.metaViewThrough.totals`
- the source reads Meta order-value aggregates with `action_report_time = 'impression'`
- values reflect Meta API-reported view-through purchase metrics, not ROAS Radar deterministic model credit

Combined comparison request:

```http
GET /api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=combined
```

Expected behavior:

- `totals` equals `comparisonTotals.combined.totals`
- `totalsCanonical` is `false`
- revenue and orders are Clicks plus Deterministic Views
- spend comes from Clicks only in the current summary implementation

## Known Limitations

- Deterministic Views are aggregate model outputs, not person-level ad-view matches.
- The approved deterministic view attribution window is 7 days.
- The v1 behavior is Meta-only; non-Meta view/impression sources are out of scope.
- Deterministic Views do not mutate canonical order winners or Shopify writeback fields.
- Deterministic Views do not own visits or spend in the summary API.
- `combined` can double count when interpreted as canonical revenue because it intentionally adds a non-canonical model layer to Clicks.
- Missing, stale, or quarantined Meta deterministic ingestion can make the Deterministic Views layer appear lower than expected.
- Meta API view-through and Deterministic Views are different surfaces and should not be reconciled as if they were the same metric.

## Support And Troubleshooting

When a customer or analyst asks why deterministic attribution differs from Clicks:

1. Confirm the requested `reportingMode`. If omitted, the API is returning canonical Clicks.
2. Compare `totals`, `layers.clicks.totals`, `layers.deterministicViews.totals`, and `comparisonTotals.combined.totals`.
3. Confirm the date range and filters. Campaign filters for Deterministic Views match Meta entity ids, while Clicks read reporting campaign dimensions.
4. Check for stale model outputs in `combined_report_api_health` logs. A `modelOutputFreshnessStatus` of `stale` means the summary API found old or missing deterministic model output.
5. If deterministic rows are missing, use [Meta Deterministic Ingestion](runbooks/meta-deterministic-ingestion.md) to inspect scheduler state, Cloud Run execution logs, row counts, freshness, quarantine, and reconciliation.
6. If rows are quarantined, keep them out of reporting until the dominant reason is corrected and raw Meta traceability remains intact.
7. Escalate to application engineering when verified Meta rows exist but `deterministic_model_outputs` are absent, stale, or leaking into canonical Clicks.
8. Escalate to the ads owner when Meta returns permissions errors or empty view/impression metrics for active campaigns.

Useful log and metric surfaces:

- `combined_report_api_health` for summary API status, selected reporting mode, deterministic view totals, and model freshness
- `roas_combined_report_model_output_freshness_hours` for stale deterministic model outputs
- `meta_ads_deterministic_sync_job_completed` and `meta_ads_deterministic_sync_job_failed` for ingestion job health
- `meta_ads_deterministic_verification_summary` and `roas_meta_deterministic_verification_rejection_rate` for verification rejections
- `meta_ads_deterministic_api_reconciliation` for API-to-persisted-row mismatches

Support language:

> Clicks are the canonical reporting total. Deterministic Views are a separate Meta API-verified model layer. The combined value is a non-canonical comparison that helps quantify additional view/impression model credit without changing the primary click attribution winner.
