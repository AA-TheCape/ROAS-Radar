import assert from "node:assert/strict";
import test from "node:test";

import {
	calculatePerformanceMetrics,
	compareModelMetrics,
} from "../src/shared/metrics.js";

function toSnapshot(value: unknown): string {
	return JSON.stringify(value, null, 2);
}

test("performance metrics snapshot stays stable", () => {
	const metrics = calculatePerformanceMetrics({
		visits: 240,
		orders: 12,
		attributedRevenue: 1380,
		spend: 360,
		clicks: 96,
		impressions: 4800,
		newCustomerOrders: 9,
		returningCustomerOrders: 3,
		newCustomerRevenue: 1050,
		returningCustomerRevenue: 330,
	});

	assert.equal(
		toSnapshot(metrics),
		`{
  "visits": 240,
  "orders": 12,
  "attributedRevenue": 1380,
  "revenue": 1380,
  "spend": 360,
  "clicks": 96,
  "impressions": 4800,
  "conversionRate": 0.05,
  "roas": 3.8333333333333335,
  "cac": 40,
  "blendedCac": 30,
  "averageOrderValue": 115,
  "clickThroughRate": 0.02,
  "newCustomerOrders": 9,
  "returningCustomerOrders": 3,
  "newCustomerRevenue": 1050,
  "returningCustomerRevenue": 330,
  "newCustomerRate": 0.75,
  "returningCustomerRate": 0.25
}`,
	);
});

test("model comparison snapshot stays stable", () => {
	const comparison = compareModelMetrics(
		{
			attributionModel: "last_touch",
			metrics: calculatePerformanceMetrics({
				visits: 240,
				orders: 12,
				attributedRevenue: 1380,
				spend: 360,
				clicks: 96,
				impressions: 4800,
				newCustomerOrders: 9,
				returningCustomerOrders: 3,
				newCustomerRevenue: 1050,
				returningCustomerRevenue: 330,
			}),
		},
		{
			attributionModel: "linear",
			metrics: calculatePerformanceMetrics({
				visits: 240,
				orders: 12,
				attributedRevenue: 1260,
				spend: 360,
				clicks: 96,
				impressions: 4800,
				newCustomerOrders: 8,
				returningCustomerOrders: 4,
				newCustomerRevenue: 940,
				returningCustomerRevenue: 320,
			}),
		},
	);

	assert.equal(
		toSnapshot(comparison),
		`{
  "baselineModel": "last_touch",
  "comparisonModel": "linear",
  "deltas": [
    {
      "metric": "attributedRevenue",
      "baseline": 1380,
      "comparison": 1260,
      "absoluteDelta": -120,
      "relativeDelta": -0.08695652173913043
    },
    {
      "metric": "revenue",
      "baseline": 1380,
      "comparison": 1260,
      "absoluteDelta": -120,
      "relativeDelta": -0.08695652173913043
    },
    {
      "metric": "roas",
      "baseline": 3.8333333333333335,
      "comparison": 3.5,
      "absoluteDelta": -0.3333333333333335,
      "relativeDelta": -0.08695652173913047
    },
    {
      "metric": "cac",
      "baseline": 40,
      "comparison": 45,
      "absoluteDelta": 5,
      "relativeDelta": 0.125
    },
    {
      "metric": "blendedCac",
      "baseline": 30,
      "comparison": 30,
      "absoluteDelta": 0,
      "relativeDelta": 0
    },
    {
      "metric": "conversionRate",
      "baseline": 0.05,
      "comparison": 0.05,
      "absoluteDelta": 0,
      "relativeDelta": 0
    },
    {
      "metric": "averageOrderValue",
      "baseline": 115,
      "comparison": 105,
      "absoluteDelta": -10,
      "relativeDelta": -0.08695652173913043
    },
    {
      "metric": "clickThroughRate",
      "baseline": 0.02,
      "comparison": 0.02,
      "absoluteDelta": 0,
      "relativeDelta": 0
    },
    {
      "metric": "newCustomerRate",
      "baseline": 0.75,
      "comparison": 0.6666666666666666,
      "absoluteDelta": -0.08333333333333337,
      "relativeDelta": -0.11111111111111116
    }
  ]
}`,
	);
});
