# Reporting Metrics

ROAS Radar computes reporting KPIs from a single shared module at `src/shared/metrics.ts`. The backend reporting service and the React dashboard both consume that module so metric math stays aligned.

Use this doc together with:

- `docs/analytics-playbook.md` for how attribution outputs and reporting tables should be interpreted
- `docs/deterministic-attribution-behavior.md` for Clicks, Deterministic Views, Meta view-through, and combined comparison behavior
- `docs/attribution-schema-v1.md` for canonical capture field names and normalization rules
- `docs/operational-attribution-contracts.md` for resolver, writeback, retention, and recovery behavior that can affect reported values

## Formulas

- `attributedRevenue`: sum of attributed revenue credit in the selected attribution model.
- `conversionRate`: `orders / visits`
- `roas`: `attributedRevenue / spend`
- `cac`: `spend / newCustomerOrders`
- `blendedCac`: `spend / orders`
- `averageOrderValue`: `attributedRevenue / orders`
- `clickThroughRate`: `clicks / impressions`
- `newCustomerRate`: `newCustomerOrders / orders`
- `returningCustomerRate`: `returningCustomerOrders / orders`

Division-based metrics return `null` when their denominator is zero, except `conversionRate`, `newCustomerRate`, and `returningCustomerRate`, which default to `0` for empty slices.

## Attribution Layers And Non-Mixing

Reporting metrics are calculated inside one explicitly selected reporting layer. Do not blend layer inputs before applying the formulas above.

Layer rules:

- Canonical Click attribution reads click-attributed order credit and spend from `daily_reporting_metrics`. This is the default and only canonical reporting total.
- Deterministic Views read API-verified Meta deterministic view/impression model outputs from `deterministic_model_outputs`. They are layer-only and non-canonical.
- Meta view-through reads Meta API-reported view-through order-value aggregates from `meta_ads_order_value_aggregates` with impression-time semantics. It is not the same surface as Deterministic Views.
- Combined totals are comparison-only Clicks plus Deterministic Views. They must remain labeled non-canonical and must not be written back into canonical revenue, order, ROAS, CAC, or Shopify attribution fields.

Required reporting behavior:

- `attributedRevenue`, `orders`, `roas`, `cac`, and `averageOrderValue` must be computed from the selected layer's revenue and order inputs.
- `reportingMode=clicks` and an omitted `reportingMode` must keep `totalsCanonical=true`.
- `reportingMode=deterministic_views`, `reportingMode=meta_view_through`, and `reportingMode=combined` must keep `totalsCanonical=false`.
- API responses, dashboards, exports, and analyst tables must use separate field names or sections for Clicks, Deterministic Views, Meta view-through, and combined comparison totals.
- A metric named generically as `attributedRevenue` without a layer label refers to canonical Click attribution only.

## Model Comparisons

Use `compareModelMetrics(...)` when comparing performance across attribution models. It returns stable absolute and relative deltas for:

- attributed revenue
- ROAS
- CAC
- blended CAC
- conversion rate
- average order value
- click-through rate
- new customer rate

## Dashboard Interpretation Notes

- A dashboard model switch changes the attribution credit source, so revenue, orders, ROAS, CAC, and conversion-rate slices can change without any raw order ingestion change.
- A reporting mode switch changes the reporting layer. Clicks, Deterministic Views, Meta view-through, and combined comparison totals are different surfaces and should not be reconciled as if they are one metric.
- Combined totals can double count if treated as canonical because they intentionally add a non-canonical deterministic view layer to Clicks.
- Order-level consumer views must read `attribution_tier` first when interpreting attribution strength. `attribution_reason` only explains how the winning tier or credit row was resolved inside that tier.
- `conversionRate`, `newCustomerRate`, and `returningCustomerRate` return `0` for empty slices; ratio metrics like `roas` and `cac` return `null` when their denominator is zero.
- Multi-touch models can create fractional orders and revenue in grouped reporting because credit is allocated from `attribution_order_credits`, not forced into whole-order rows.
- If the math looks right but the inputs look wrong, move to `docs/analytics-playbook.md` for table interpretation and then to the schema or operational docs for capture and lifecycle questions.
