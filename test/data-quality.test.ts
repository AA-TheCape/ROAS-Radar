import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.DATA_QUALITY_TARGET_LAG_DAYS = '1';
process.env.DATA_QUALITY_ANOMALY_LOOKBACK_DAYS = '7';
process.env.DATA_QUALITY_ANOMALY_THRESHOLD_RATIO = '0.35';
process.env.DATA_QUALITY_ANOMALY_MIN_BASELINE = '5';
process.env.DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD = '0';
process.env.DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD = '2';
process.env.DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD = '1';
process.env.DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD = '0';
process.env.DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD = '3';

const { __dataQualityTestUtils } = await import('../src/modules/data-quality/index.js');

test('resolveRunDate defaults to the previous UTC day based on configured lag', () => {
  const runDate = __dataQualityTestUtils.resolveRunDate(new Date('2026-04-11T12:00:00.000Z'));
  assert.equal(runDate, '2026-04-10');
});

test('buildLookbackDates returns an inclusive trailing window ending on the run date', () => {
  const dates = __dataQualityTestUtils.buildLookbackDates('2026-04-10', 3);
  assert.deepEqual(dates, ['2026-04-08', '2026-04-09', '2026-04-10']);
});

test('detectAnomalyFlags identifies metrics that materially deviate from the trailing baseline', () => {
  const flags = __dataQualityTestUtils.detectAnomalyFlags(
    [
      { metric_date: '2026-04-03', visits: '100', orders: '10', revenue: '1000', spend: '300' },
      { metric_date: '2026-04-04', visits: '96', orders: '9', revenue: '980', spend: '290' },
      { metric_date: '2026-04-05', visits: '104', orders: '11', revenue: '1020', spend: '310' },
      { metric_date: '2026-04-06', visits: '98', orders: '10', revenue: '990', spend: '305' },
      { metric_date: '2026-04-10', visits: '42', orders: '3', revenue: '390', spend: '120' }
    ],
    '2026-04-10'
  );

  assert.deepEqual(
    flags.map((flag: { metric: string }) => flag.metric).sort(),
    ['orders', 'revenue', 'spend', 'visits']
  );
  assert.ok(flags.every((flag: { relativeDelta: number | null }) => (flag.relativeDelta ?? 0) >= 0.35));
});

test('evaluateDiscrepancyCount marks critical threshold breaches as failed alerts', () => {
  const result = __dataQualityTestUtils.evaluateDiscrepancyCount({
    discrepancyCount: 3,
    threshold: 2,
    severityOnAlert: 'critical',
    healthySummary: 'healthy',
    warningSummary: 'warning',
    alertSummary: 'alert',
    details: {}
  });

  assert.equal(result.status, 'failed');
  assert.equal(result.severity, 'critical');
  assert.equal(result.alertTriggered, true);
});

test('evaluateDiscrepancyCount keeps sub-threshold discrepancies as non-alert warnings', () => {
  const result = __dataQualityTestUtils.evaluateDiscrepancyCount({
    discrepancyCount: 2,
    threshold: 3,
    severityOnAlert: 'warning',
    healthySummary: 'healthy',
    warningSummary: 'warning',
    alertSummary: 'alert',
    details: {}
  });

  assert.equal(result.status, 'warning');
  assert.equal(result.severity, 'warning');
  assert.equal(result.alertTriggered, false);
});
