const COMPARABLE_METRICS = [
    'attributedRevenue',
    'revenue',
    'roas',
    'cac',
    'blendedCac',
    'conversionRate',
    'averageOrderValue',
    'clickThroughRate',
    'newCustomerRate'
];
export function toNumber(value) {
    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : 0;
    }
    if (typeof value !== 'string') {
        return 0;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
}
export function safeDivide(numerator, denominator) {
    const normalizedNumerator = toNumber(numerator);
    const normalizedDenominator = toNumber(denominator);
    if (!Number.isFinite(normalizedNumerator) || !Number.isFinite(normalizedDenominator) || normalizedDenominator === 0) {
        return null;
    }
    return normalizedNumerator / normalizedDenominator;
}
export function calculatePerformanceMetrics(input) {
    const visits = toNumber(input.visits);
    const orders = toNumber(input.orders);
    const attributedRevenue = toNumber(input.attributedRevenue);
    const spend = toNumber(input.spend);
    const clicks = toNumber(input.clicks);
    const impressions = toNumber(input.impressions);
    const newCustomerOrders = toNumber(input.newCustomerOrders);
    const returningCustomerOrders = toNumber(input.returningCustomerOrders);
    const newCustomerRevenue = toNumber(input.newCustomerRevenue);
    const returningCustomerRevenue = toNumber(input.returningCustomerRevenue);
    return {
        visits,
        orders,
        attributedRevenue,
        revenue: attributedRevenue,
        spend,
        clicks,
        impressions,
        conversionRate: safeDivide(orders, visits) ?? 0,
        roas: safeDivide(attributedRevenue, spend),
        cac: safeDivide(spend, newCustomerOrders),
        blendedCac: safeDivide(spend, orders),
        averageOrderValue: safeDivide(attributedRevenue, orders),
        clickThroughRate: safeDivide(clicks, impressions),
        newCustomerOrders,
        returningCustomerOrders,
        newCustomerRevenue,
        returningCustomerRevenue,
        newCustomerRate: safeDivide(newCustomerOrders, orders) ?? 0,
        returningCustomerRate: safeDivide(returningCustomerOrders, orders) ?? 0
    };
}
export function compareMetricValues(left, right) {
    if (typeof left === 'number' || typeof right === 'number') {
        return (typeof left === 'number' ? left : Number.NEGATIVE_INFINITY) -
            (typeof right === 'number' ? right : Number.NEGATIVE_INFINITY);
    }
    return (left ?? '').localeCompare(right ?? '', undefined, { sensitivity: 'base' });
}
export function compareModelMetrics(baseline, comparison) {
    return {
        baselineModel: baseline.attributionModel,
        comparisonModel: comparison.attributionModel,
        deltas: COMPARABLE_METRICS.map((metric) => {
            const baselineValue = baseline.metrics[metric];
            const comparisonValue = comparison.metrics[metric];
            const absoluteDelta = baselineValue === null || comparisonValue === null ? null : comparisonValue - baselineValue;
            return {
                metric,
                baseline: baselineValue,
                comparison: comparisonValue,
                absoluteDelta,
                relativeDelta: absoluteDelta === null || baselineValue === null || baselineValue === 0
                    ? null
                    : absoluteDelta / baselineValue
            };
        })
    };
}
