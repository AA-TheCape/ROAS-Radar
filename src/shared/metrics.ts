export type NumericLike = number | string | null | undefined;

export type PerformanceMetricsInput = {
	visits?: NumericLike;
	orders?: NumericLike;
	attributedRevenue?: NumericLike;
	spend?: NumericLike;
	clicks?: NumericLike;
	impressions?: NumericLike;
	newCustomerOrders?: NumericLike;
	returningCustomerOrders?: NumericLike;
	newCustomerRevenue?: NumericLike;
	returningCustomerRevenue?: NumericLike;
};

export type PerformanceMetrics = {
	visits: number;
	orders: number;
	attributedRevenue: number;
	revenue: number;
	spend: number;
	clicks: number;
	impressions: number;
	conversionRate: number;
	roas: number | null;
	cac: number | null;
	blendedCac: number | null;
	averageOrderValue: number | null;
	clickThroughRate: number | null;
	newCustomerOrders: number;
	returningCustomerOrders: number;
	newCustomerRevenue: number;
	returningCustomerRevenue: number;
	newCustomerRate: number;
	returningCustomerRate: number;
};

export type ComparableMetricKey =
	| "attributedRevenue"
	| "revenue"
	| "roas"
	| "cac"
	| "blendedCac"
	| "conversionRate"
	| "averageOrderValue"
	| "clickThroughRate"
	| "newCustomerRate";

export type MetricModelComparisonInput = {
	attributionModel: string;
	metrics: PerformanceMetrics;
};

export type ModelMetricComparison = {
	baselineModel: string;
	comparisonModel: string;
	deltas: Array<{
		metric: ComparableMetricKey;
		baseline: number | null;
		comparison: number | null;
		absoluteDelta: number | null;
		relativeDelta: number | null;
	}>;
};

const COMPARABLE_METRICS: ComparableMetricKey[] = [
	"attributedRevenue",
	"revenue",
	"roas",
	"cac",
	"blendedCac",
	"conversionRate",
	"averageOrderValue",
	"clickThroughRate",
	"newCustomerRate",
];

export function toNumber(value: NumericLike): number {
	if (typeof value === "number") {
		return Number.isFinite(value) ? value : 0;
	}

	if (typeof value !== "string") {
		return 0;
	}

	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : 0;
}

export function safeDivide(
	numerator: NumericLike,
	denominator: NumericLike,
): number | null {
	const normalizedNumerator = toNumber(numerator);
	const normalizedDenominator = toNumber(denominator);

	if (
		!Number.isFinite(normalizedNumerator) ||
		!Number.isFinite(normalizedDenominator) ||
		normalizedDenominator === 0
	) {
		return null;
	}

	return normalizedNumerator / normalizedDenominator;
}

export function calculatePerformanceMetrics(
	input: PerformanceMetricsInput,
): PerformanceMetrics {
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
		returningCustomerRate: safeDivide(returningCustomerOrders, orders) ?? 0,
	};
}

export function compareMetricValues(
	left: string | number | null,
	right: string | number | null,
): number {
	if (typeof left === "number" || typeof right === "number") {
		return (
			(typeof left === "number" ? left : Number.NEGATIVE_INFINITY) -
			(typeof right === "number" ? right : Number.NEGATIVE_INFINITY)
		);
	}

	return (left ?? "").localeCompare(right ?? "", undefined, {
		sensitivity: "base",
	});
}

export function compareModelMetrics(
	baseline: MetricModelComparisonInput,
	comparison: MetricModelComparisonInput,
): ModelMetricComparison {
	return {
		baselineModel: baseline.attributionModel,
		comparisonModel: comparison.attributionModel,
		deltas: COMPARABLE_METRICS.map((metric) => {
			const baselineValue = baseline.metrics[metric];
			const comparisonValue = comparison.metrics[metric];
			const absoluteDelta =
				baselineValue === null || comparisonValue === null
					? null
					: comparisonValue - baselineValue;

			return {
				metric,
				baseline: baselineValue,
				comparison: comparisonValue,
				absoluteDelta,
				relativeDelta:
					absoluteDelta === null ||
					baselineValue === null ||
					baselineValue === 0
						? null
						: absoluteDelta / baselineValue,
			};
		}),
	};
}
