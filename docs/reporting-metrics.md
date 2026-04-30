# Reporting Metrics

ROAS Radar computes reporting KPIs from a single shared module at `src/shared/metrics.ts`. The backend reporting service and the React dashboard both consume that module so metric math stays aligned.

Use this doc together with:

- `docs/analytics-playbook.md` for how attribution outputs and reporting tables should be interpreted
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
- Order-level consumer views must read `attribution_tier` first when interpreting attribution strength. `attribution_reason` only explains how the winning tier or credit row was resolved inside that tier.
- `conversionRate`, `newCustomerRate`, and `returningCustomerRate` return `0` for empty slices; ratio metrics like `roas` and `cac` return `null` when their denominator is zero.
- Multi-touch models can create fractional orders and revenue in grouped reporting because credit is allocated from `attribution_order_credits`, not forced into whole-order rows.
- If the math looks right but the inputs look wrong, move to `docs/analytics-playbook.md` for table interpretation and then to the schema or operational docs for capture and lifecycle questions.
