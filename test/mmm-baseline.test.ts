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
    posteriorChains: 2,
    posteriorDraws: 200,
    holdoutRatio: 0
  });

  assert.equal(run.modelVersion, 'baseline_linear_mmm_v1');
  assert.equal(run.runConfig.bayesianEngine, 'closed_form_linear_gaussian_posterior_v1');
  assert.equal(run.runConfig.responseVariable, 'daily_total_shopify_revenue_from_mart_outcomes');
  assert.equal(run.calibrationReport.deterministicAttributionUsage, 'calibration_and_validation_segments_only');
  assert.ok(run.calibrationReport.governance);
  assert.equal(run.validationReport.train.observationCount, 3);
  assert.equal(run.validationReport.posteriorSanityChecks.status, 'pass');
  assert.equal(run.validationReport.posteriorDiagnostics.chains, 2);
  assert.equal(run.validationReport.posteriorDiagnostics.totalDraws, 200);
  assert.deepEqual(run.inputSummary.selectedSegments, ['meta|paid_social|prospecting', 'google|cpc|brand']);
  assert.ok(run.modelArtifact.posteriorCoefficients);
  assert.ok(run.modelArtifact.contributionOutputs);

  const calibrationSegments = run.calibrationReport.segments as Array<{ key: string; attributedRevenue: number }>;
  assert.deepEqual(
    calibrationSegments.map((segment) => [segment.key, segment.attributedRevenue]),
    [
      ['meta|paid_social|prospecting', 750],
      ['google|cpc|brand', 240]
    ]
  );

  const contributionOutputs = run.modelArtifact.contributionOutputs as {
    channels: Array<{
      key: string;
      contribution: { credibleInterval95: { lower: number; upper: number } };
      contributionShare: { mean: number };
    }>;
  };
  assert.deepEqual(
    contributionOutputs.channels.map((channel) => channel.key),
    ['meta|paid_social|prospecting', 'google|cpc|brand', '__other_paid__']
  );
  assert.ok(contributionOutputs.channels[0]?.contribution.credibleInterval95.upper);
  assert.ok(contributionOutputs.channels[0]?.contributionShare.mean);

  const governance = run.calibrationReport.governance as {
    status: string;
    thresholds: { warnDivergenceRate: number; alertDivergenceRate: number };
    reconciliationLogic: string;
    rowCount: number;
    channelWeekReconciliation: Array<{ weekStartDate: string; key: string; governanceTier: string }>;
  };
  assert.equal(governance.thresholds.warnDivergenceRate, 0.25);
  assert.equal(governance.thresholds.alertDivergenceRate, 0.5);
  assert.equal(governance.rowCount, 9);
  assert.ok(governance.reconciliationLogic.includes('deterministic attribution_credit_revenue'));
  assert.deepEqual(
    governance.channelWeekReconciliation.slice(0, 3).map((row) => [row.weekStartDate, row.key]),
    [
      ['2026-04-01', 'meta|paid_social|prospecting'],
      ['2026-04-01', 'google|cpc|brand'],
      ['2026-04-01', '__other_paid__']
    ]
  );

  const strictRun = buildBaselineMmmArtifact(rows, {
    startDate: '2026-04-01',
    endDate: '2026-04-03',
    attributionModel: 'last_touch',
    adstockDecay: 0,
    posteriorChains: 2,
    posteriorDraws: 200,
    holdoutRatio: 0,
    calibrationWarnDivergenceRate: 0,
    calibrationAlertDivergenceRate: 0
  });
  const strictGovernance = strictRun.calibrationReport.governance as { status: string; alertCount: number };
  assert.equal(strictGovernance.status, 'alert');
  assert.ok(strictGovernance.alertCount > 0);
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
