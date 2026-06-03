import assert from 'node:assert/strict';
import test from 'node:test';

import { buildBaselineMmmArtifact } from '../src/modules/mmm/baseline.js';

test('MMM baseline uses attribution metrics as calibration diagnostics instead of segment labels', () => {
  const rows = [
    {
      metric_date: '2026-04-01',
      mart_row_type: 'paid_media' as const,
      attribution_model: 'none',
      platform: 'meta',
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      spend: '100',
      impressions: '1000',
      clicks: '50',
      shopify_revenue: '0',
      attribution_credit_revenue: '0',
      attribution_credit_orders: '0',
      match_source_coverage: {},
      confidence_label_coverage: {}
    },
    {
      metric_date: '2026-04-02',
      mart_row_type: 'paid_media' as const,
      attribution_model: 'none',
      platform: 'meta',
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      spend: '150',
      impressions: '1200',
      clicks: '60',
      shopify_revenue: '0',
      attribution_credit_revenue: '0',
      attribution_credit_orders: '0',
      match_source_coverage: {},
      confidence_label_coverage: {}
    },
    {
      metric_date: '2026-04-03',
      mart_row_type: 'paid_media' as const,
      attribution_model: 'none',
      platform: 'google',
      source: 'google',
      medium: 'cpc',
      campaign: 'brand',
      spend: '80',
      impressions: '900',
      clicks: '70',
      shopify_revenue: '0',
      attribution_credit_revenue: '0',
      attribution_credit_orders: '0',
      match_source_coverage: {},
      confidence_label_coverage: {}
    },
    {
      metric_date: '2026-04-01',
      mart_row_type: 'attribution' as const,
      attribution_model: 'last_touch',
      platform: 'taxonomy',
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      spend: '0',
      impressions: '0',
      clicks: '0',
      shopify_revenue: '300',
      attribution_credit_revenue: '300',
      attribution_credit_orders: '3',
      match_source_coverage: { first_party: 3 },
      confidence_label_coverage: { high: 3 }
    },
    {
      metric_date: '2026-04-02',
      mart_row_type: 'attribution' as const,
      attribution_model: 'last_touch',
      platform: 'taxonomy',
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      spend: '0',
      impressions: '0',
      clicks: '0',
      shopify_revenue: '450',
      attribution_credit_revenue: '450',
      attribution_credit_orders: '4.5',
      match_source_coverage: { first_party: 4.5 },
      confidence_label_coverage: { high: 4.5 }
    },
    {
      metric_date: '2026-04-03',
      mart_row_type: 'attribution' as const,
      attribution_model: 'last_touch',
      platform: 'taxonomy',
      source: 'google',
      medium: 'cpc',
      campaign: 'brand',
      spend: '0',
      impressions: '0',
      clicks: '0',
      shopify_revenue: '240',
      attribution_credit_revenue: '240',
      attribution_credit_orders: '2.4',
      match_source_coverage: { first_party: 2.4 },
      confidence_label_coverage: { high: 2.4 }
    }
  ];

  const run = buildBaselineMmmArtifact(rows, {
    startDate: '2026-04-01',
    endDate: '2026-04-03',
    attributionModel: 'last_touch',
    adstockDecay: 0,
    holdoutRatio: 0
  });

  assert.equal(run.modelVersion, 'baseline_linear_mmm_v1');
  assert.equal(run.runConfig.responseVariable, 'daily_total_shopify_revenue_from_mart_outcomes');
  assert.equal(run.calibrationReport.deterministicAttributionUsage, 'calibration_and_validation_segments_only');
  assert.equal(run.validationReport.train.observationCount, 3);
  assert.deepEqual(run.inputSummary.selectedSegments, ['meta|paid_social|prospecting', 'google|cpc|brand']);

  const calibrationSegments = run.calibrationReport.segments as Array<{ key: string; attributedRevenue: number }>;
  assert.deepEqual(
    calibrationSegments.map((segment) => [segment.key, segment.attributedRevenue]),
    [
      ['meta|paid_social|prospecting', 750],
      ['google|cpc|brand', 240]
    ]
  );
});

test('MMM baseline rejects insufficient mart observations', () => {
  assert.throws(
    () =>
      buildBaselineMmmArtifact(
        [
          {
            metric_date: '2026-04-01',
            mart_row_type: 'attribution' as const,
            attribution_model: 'last_touch',
            platform: 'taxonomy',
            source: 'meta',
            medium: 'paid_social',
            campaign: 'prospecting',
            spend: '0',
            impressions: '0',
            clicks: '0',
            shopify_revenue: '300',
            attribution_credit_revenue: '300',
            attribution_credit_orders: '3',
            match_source_coverage: {},
            confidence_label_coverage: {}
          }
        ],
        {
          startDate: '2026-04-01',
          endDate: '2026-04-01'
        }
      ),
    /requires at least 3 daily observations/
  );
});
