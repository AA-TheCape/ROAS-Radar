# MMM Daily Input Mart

`mmm_daily_input_mart_v1` is the deterministic daily export surface for marketing mix modeling inputs.

The mart is versioned in the table name and `mart_version` column. Version `v1` has two row types:

- `paid_media`: native ad platform rows from deterministic spend projections. These rows preserve platform, connection id, account/campaign/ad set/ad/creative ids and names, source/medium/campaign/content/term taxonomy, spend, impressions, clicks, currency, and spend freshness. `attribution_model` is `none`.
- `attribution`: Shopify outcome and attribution credit rows aggregated by source/medium/campaign/content/term and attribution model. These rows preserve order counts, Shopify revenue, fractional credit orders/revenue, new/returning customer credit splits, match-source coverage, confidence-label coverage, Shopify freshness, and attribution freshness.

The split row model avoids duplicating Shopify attribution metrics across multiple native ad entities that share the same taxonomy. MMM consumers can aggregate by taxonomy for modeled response variables while still retaining native id rows for paid media spend audits and platform-level reconciliation.

Refresh entry points:

- `refreshDailyMmmInputMart(client, metricDates)` rebuilds specific dates.
- `refreshAllDailyMmmInputMart(client)` rebuilds all dates observed in Shopify orders, Meta spend, or Google Ads spend.

## Read API and Export

The approved mart is exposed through `GET /api/reporting/mmm`.

Authentication matches the other reporting APIs: callers must send the configured bearer token or an authenticated app session. Responses include `X-ROAS-Radar-MMM-Schema: mmm_daily_input_mart_v1`, and JSON responses also include `schemaVersion`.

Supported query parameters:

- `startDate` and `endDate` are required `YYYY-MM-DD` dates.
- `martRowType` optionally filters to `paid_media` or `attribution`.
- `attributionModel` optionally filters attribution rows to a supported attribution model.
- `platform` optionally filters to `meta`, `google`, or `taxonomy`.
- `source` and `campaign` optionally filter canonical taxonomy dimensions.
- `limit` and `offset` page export rows. `limit` defaults to `1000` and is capped at `10000`.
- `format=json|csv` defaults to JSON. CSV responses repeat `schemaVersion`, `generationTimestamp`, and `readinessStatus` on each row for model-training export jobs.

JSON responses include:

- `readiness.status`: `ready` when every requested date has matching mart rows, `partial` when only some dates do, and `not_ready` when none do.
- `readiness.generationTimestamp`: the latest `last_computed_at` value across matching mart rows in the requested window.
- `readiness.excludedDateWindows`: date windows excluded from the filtered training set. Reasons are `no_mmm_mart_rows` when the mart has no rows for the date, or `no_rows_matching_filters` when the mart has rows but none match the requested filters.
- `rows`: schema-versioned mart rows in camelCase with native paid-media ids, taxonomy dimensions, metrics, freshness timestamps, and coverage JSON.
