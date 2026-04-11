# Reporting Metrics

ROAS Radar computes reporting KPIs from a single shared module at `src/shared/metrics.ts`. The backend reporting service and the React dashboard both consume that module so metric math stays aligned.

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
