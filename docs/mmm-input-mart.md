# MMM Daily Input Mart

`mmm_daily_input_mart_v1` is the deterministic daily export surface for marketing mix modeling inputs.

The mart is versioned in the table name and `mart_version` column. Version `v1` has two row types:

- `paid_media`: native ad platform rows from deterministic spend projections. These rows preserve platform, connection id, account/campaign/ad set/ad/creative ids and names, source/medium/campaign/content/term taxonomy, spend, impressions, clicks, currency, and spend freshness. `attribution_model` is `none`.
- `attribution`: Shopify outcome and attribution credit rows aggregated by source/medium/campaign/content/term and attribution model. These rows preserve order counts, Shopify revenue, fractional credit orders/revenue, new/returning customer credit splits, match-source coverage, confidence-label coverage, Shopify freshness, and attribution freshness.

The split row model avoids duplicating Shopify attribution metrics across multiple native ad entities that share the same taxonomy. MMM consumers can aggregate by taxonomy for modeled response variables while still retaining native id rows for paid media spend audits and platform-level reconciliation.

Refresh entry points:

- `refreshDailyMmmInputMart(client, metricDates)` rebuilds specific dates.
- `refreshAllDailyMmmInputMart(client)` rebuilds all dates observed in Shopify orders, Meta spend, or Google Ads spend.

