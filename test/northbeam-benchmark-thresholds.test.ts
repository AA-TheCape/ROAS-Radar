import assert from 'node:assert/strict';
import test from 'node:test';

import {
  assertNorthbeamBenchmarkThresholds,
  buildNorthbeamBenchmarkReport,
  evaluateNorthbeamBenchmarkThresholds,
  formatNorthbeamBenchmarkThresholdFailures
} from '../src/modules/attribution/benchmark.js';

test('northbeam parity benchmark stays within documented thresholds', () => {
  const report = buildNorthbeamBenchmarkReport();

  assert.doesNotThrow(() => {
    assertNorthbeamBenchmarkThresholds(report);
  });
});

test('northbeam parity threshold gate reports model, channel, and cohort regressions', () => {
  const report = buildNorthbeamBenchmarkReport();
  const failingReport = {
    ...report,
    modelSlices: report.modelSlices.map((slice, index) =>
      index === 0
        ? {
            ...slice,
            severity: 'red' as const,
            attributedRevenueDeltaPercent: 9.1
          }
        : slice
    ),
    channelSlices: report.channelSlices.map((slice, index) =>
      index === 0
        ? {
            ...slice,
            severity: 'red' as const,
            revenueDeltaPercent: 9.2
          }
        : slice
    ),
    cohortSlices: report.cohortSlices.map((slice, index) =>
      index === 0
        ? {
            ...slice,
            severity: 'red' as const,
            primaryWinnerMismatchRate: 9.3
          }
        : slice
    )
  };

  const result = evaluateNorthbeamBenchmarkThresholds(failingReport);

  assert.equal(result.ok, false);
  assert.equal(result.violations.length, 3);
  assert.match(
    formatNorthbeamBenchmarkThresholdFailures(result),
    /model .*attributed_revenue_delta_percent[\s\S]*channel .*revenue_delta_percent[\s\S]*cohort .*primary_winner_mismatch_rate/
  );
  assert.throws(() => {
    assertNorthbeamBenchmarkThresholds(failingReport);
  }, /Northbeam benchmark parity threshold failures:/);
});
