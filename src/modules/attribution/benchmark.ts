import {
  ATTRIBUTION_MODELS,
  executeAttributionModels,
  type AttributionCredit,
  type AttributionLookbackRule,
  type AttributionModel,
  type AttributionModelSummary
} from './engine.js';
import {
  NORTHBEAM_BENCHMARK_FIXTURES,
  NORTHBEAM_BENCHMARK_FIXTURE_SET_VERSION,
  type NorthbeamBenchmarkCohort,
  type NorthbeamBenchmarkFixture,
  type NorthbeamBenchmarkReferenceCredit,
  type NorthbeamBenchmarkReferenceModel
} from './benchmark-fixtures.js';

type Severity = 'green' | 'yellow' | 'red';

type ComparableCredit = {
  touchpointId: string;
  source: string | null;
  medium: string | null;
  revenueCredit: string;
  creditWeight: number;
  isPrimary: boolean;
  isDirect: boolean;
  isSynthetic: boolean;
};

type ComparableSummary = {
  allocationStatus: AttributionModelSummary['allocationStatus'];
  winnerTouchpointId: string | null;
  totalRevenueCredited: string;
  touchpointCountConsidered: number;
  eligibleClickCount: number;
  eligibleViewCount: number;
  lookbackRuleApplied: AttributionLookbackRule;
  directSuppressionApplied: boolean;
  deterministicBlockApplied: boolean;
};

type ComparableRun = {
  summary: ComparableSummary;
  credits: ComparableCredit[];
};

type OrderModelComparison = {
  fixtureId: string;
  fixtureName: string;
  cohort: NorthbeamBenchmarkCohort;
  orderId: string;
  orderRevenue: string;
  modelKey: AttributionModel;
  actual: ComparableRun;
  reference: ComparableRun;
};

type ThresholdBand = {
  greenMax: number;
  yellowMax: number;
};

type BenchmarkThresholds = {
  modelRevenueDeltaPercent: ThresholdBand;
  channelRevenueDeltaPercent: ThresholdBand;
  winnerMismatchRate: ThresholdBand;
  unattributedRateDeltaPercentagePoints: ThresholdBand;
};

export type NorthbeamBenchmarkModelSlice = {
  modelKey: AttributionModel;
  actualAttributedRevenue: string;
  referenceAttributedRevenue: string;
  attributedRevenueDelta: string;
  attributedRevenueDeltaPercent: number;
  actualUnattributedOrders: number;
  referenceUnattributedOrders: number;
  unattributedOrderCountDelta: number;
  unattributedRateDeltaPercentagePoints: number;
  winnerMismatchRate: number;
  touchpointCountMismatchRate: number;
  severity: Severity;
};

export type NorthbeamBenchmarkChannelSlice = {
  modelKey: AttributionModel;
  channelKey: string;
  actualRevenue: string;
  referenceRevenue: string;
  absoluteRevenueDelta: string;
  revenueDeltaPercent: number;
  actualOrderCount: number;
  referenceOrderCount: number;
  orderCountDelta: number;
  severity: Severity;
};

export type NorthbeamBenchmarkCohortSlice = {
  cohort: NorthbeamBenchmarkCohort;
  modelKey: AttributionModel;
  orderCount: number;
  primaryWinnerMismatchRate: number;
  directClassificationMismatchRate: number;
  lookbackEligibilityMismatchRate: number;
  syntheticFallbackUsageMismatchRate: number;
  severity: Severity;
};

export type NorthbeamBenchmarkOrderMismatch = {
  fixtureId: string;
  fixtureName: string;
  cohort: NorthbeamBenchmarkCohort;
  orderId: string;
  modelKey: AttributionModel;
  severity: Severity;
  mismatchReasons: string[];
  actualWinnerTouchpointId: string | null;
  referenceWinnerTouchpointId: string | null;
  actualRevenueCredited: string;
  referenceRevenueCredited: string;
};

export type NorthbeamBenchmarkReport = {
  reportVersion: 1;
  fixtureSetVersion: string;
  orderCount: number;
  thresholds: BenchmarkThresholds;
  modelSlices: NorthbeamBenchmarkModelSlice[];
  channelSlices: NorthbeamBenchmarkChannelSlice[];
  cohortSlices: NorthbeamBenchmarkCohortSlice[];
  orderLevelMismatches: NorthbeamBenchmarkOrderMismatch[];
  topVarianceDrivers: NorthbeamBenchmarkOrderMismatch[];
};

export type NorthbeamBenchmarkGateViolation =
  | { slice: 'model'; modelKey: AttributionModel; severity: Severity; metric: string; details: string }
  | {
      slice: 'channel';
      modelKey: AttributionModel;
      channelKey: string;
      severity: Severity;
      metric: string;
      details: string;
    }
  | {
      slice: 'cohort';
      cohort: NorthbeamBenchmarkCohort;
      modelKey: AttributionModel;
      severity: Severity;
      metric: string;
      details: string;
    };

export type NorthbeamBenchmarkGateResult = {
  ok: boolean;
  violations: NorthbeamBenchmarkGateViolation[];
};

export const NORTHBEAM_BENCHMARK_THRESHOLDS: BenchmarkThresholds = {
  modelRevenueDeltaPercent: { greenMax: 1, yellowMax: 3 },
  channelRevenueDeltaPercent: { greenMax: 2, yellowMax: 5 },
  winnerMismatchRate: { greenMax: 3, yellowMax: 8 },
  unattributedRateDeltaPercentagePoints: { greenMax: 0.5, yellowMax: 1.5 }
};

function revenueToCents(value: string): number {
  return Math.round(Number.parseFloat(value) * 100);
}

function centsToRevenue(cents: number): string {
  return (cents / 100).toFixed(2);
}

function percentage(numerator: number, denominator: number): number {
  if (denominator === 0) {
    return 0;
  }

  return Number(((numerator / denominator) * 100).toFixed(2));
}

function deltaPercent(actualCents: number, referenceCents: number): number {
  if (referenceCents === 0) {
    return actualCents === 0 ? 0 : 100;
  }

  return Number((Math.abs(actualCents - referenceCents) / referenceCents * 100).toFixed(2));
}

function resolveSeverity(value: number, thresholds: ThresholdBand): Severity {
  if (value <= thresholds.greenMax) {
    return 'green';
  }

  if (value <= thresholds.yellowMax) {
    return 'yellow';
  }

  return 'red';
}

function maxSeverity(current: Severity, next: Severity): Severity {
  const order: Severity[] = ['green', 'yellow', 'red'];
  return order[Math.max(order.indexOf(current), order.indexOf(next))] ?? current;
}

function formatChannelKey(source: string | null, medium: string | null, isDirect: boolean): string {
  if (isDirect || (!source && !medium)) {
    return 'direct / none';
  }

  return `${source ?? '(unknown source)'} / ${medium ?? '(unknown medium)'}`;
}

function referenceCreditToComparable(
  fixture: NorthbeamBenchmarkFixture,
  credit: NorthbeamBenchmarkReferenceCredit
): ComparableCredit {
  const touchpoint = fixture.touchpoints.find((candidate) => candidate.touchpointId === credit.touchpointId);

  return {
    touchpointId: credit.touchpointId,
    source: credit.source ?? touchpoint?.source ?? null,
    medium: credit.medium ?? touchpoint?.medium ?? null,
    revenueCredit: credit.revenueCredit,
    creditWeight: credit.creditWeight,
    isPrimary: credit.isPrimary,
    isDirect: credit.isDirect ?? touchpoint?.isDirect ?? false,
    isSynthetic: touchpoint?.isSynthetic ?? false
  };
}

function buildReferenceRun(
  fixture: NorthbeamBenchmarkFixture,
  reference: NorthbeamBenchmarkReferenceModel
): ComparableRun {
  return {
    summary: {
      allocationStatus: reference.allocationStatus,
      winnerTouchpointId: reference.winnerTouchpointId,
      totalRevenueCredited: reference.totalRevenueCredited,
      touchpointCountConsidered: reference.touchpointCountConsidered,
      eligibleClickCount: reference.eligibleClickCount,
      eligibleViewCount: reference.eligibleViewCount,
      lookbackRuleApplied: reference.lookbackRuleApplied,
      directSuppressionApplied: reference.directSuppressionApplied ?? false,
      deterministicBlockApplied: reference.deterministicBlockApplied ?? false
    },
    credits: reference.credits.map((credit) => referenceCreditToComparable(fixture, credit))
  };
}

function buildActualRun(
  fixture: NorthbeamBenchmarkFixture,
  modelKey: AttributionModel
): ComparableRun {
  const result = executeAttributionModels(fixture.touchpoints, {
    orderOccurredAt: new Date(fixture.orderOccurredAt),
    orderRevenue: fixture.orderRevenue,
    attributionModels: [modelKey]
  });
  const summary = result.summariesByModel[modelKey];

  return {
    summary: {
      allocationStatus: summary.allocationStatus,
      winnerTouchpointId: summary.winnerTouchpointId,
      totalRevenueCredited: summary.totalRevenueCredited,
      touchpointCountConsidered: summary.touchpointCountConsidered,
      eligibleClickCount: summary.eligibleClickCount,
      eligibleViewCount: summary.eligibleViewCount,
      lookbackRuleApplied: summary.lookbackRuleApplied,
      directSuppressionApplied: summary.directSuppressionApplied,
      deterministicBlockApplied: summary.deterministicBlockApplied
    },
    credits: result.creditsByModel[modelKey]
      .filter((credit) => revenueToCents(credit.revenueCredit) > 0)
      .map((credit: AttributionCredit) => ({
        touchpointId: credit.touchpointId ?? 'unattributed',
        source: credit.source,
        medium: credit.medium,
        revenueCredit: credit.revenueCredit,
        creditWeight: credit.creditWeight,
        isPrimary: credit.isPrimary,
        isDirect: credit.isDirect,
        isSynthetic: credit.isSynthetic
      }))
  };
}

function buildComparisons(fixtures: NorthbeamBenchmarkFixture[]): OrderModelComparison[] {
  return fixtures.flatMap((fixture) =>
    ATTRIBUTION_MODELS.map((modelKey) => ({
      fixtureId: fixture.fixtureId,
      fixtureName: fixture.name,
      cohort: fixture.cohort,
      orderId: fixture.orderId,
      orderRevenue: fixture.orderRevenue,
      modelKey,
      actual: buildActualRun(fixture, modelKey),
      reference: buildReferenceRun(fixture, fixture.northbeamReferenceByModel[modelKey])
    }))
  );
}

function buildModelSlices(comparisons: OrderModelComparison[], orderCount: number): NorthbeamBenchmarkModelSlice[] {
  return ATTRIBUTION_MODELS.map((modelKey) => {
    const scoped = comparisons.filter((comparison) => comparison.modelKey === modelKey);
    const actualAttributedRevenueCents = scoped.reduce(
      (sum, comparison) => sum + revenueToCents(comparison.actual.summary.totalRevenueCredited),
      0
    );
    const referenceAttributedRevenueCents = scoped.reduce(
      (sum, comparison) => sum + revenueToCents(comparison.reference.summary.totalRevenueCredited),
      0
    );
    const actualUnattributedOrders = scoped.filter(
      (comparison) => comparison.actual.summary.allocationStatus !== 'attributed'
    ).length;
    const referenceUnattributedOrders = scoped.filter(
      (comparison) => comparison.reference.summary.allocationStatus !== 'attributed'
    ).length;
    const winnerMismatchRate = percentage(
      scoped.filter(
        (comparison) => comparison.actual.summary.winnerTouchpointId !== comparison.reference.summary.winnerTouchpointId
      ).length,
      orderCount
    );
    const touchpointCountMismatchRate = percentage(
      scoped.filter(
        (comparison) =>
          comparison.actual.summary.touchpointCountConsidered !== comparison.reference.summary.touchpointCountConsidered
      ).length,
      orderCount
    );
    const attributedRevenueDeltaPercent = deltaPercent(actualAttributedRevenueCents, referenceAttributedRevenueCents);
    const unattributedRateDeltaPercentagePoints = percentage(
      Math.abs(actualUnattributedOrders - referenceUnattributedOrders),
      orderCount
    );

    let severity = resolveSeverity(attributedRevenueDeltaPercent, NORTHBEAM_BENCHMARK_THRESHOLDS.modelRevenueDeltaPercent);
    severity = maxSeverity(
      severity,
      resolveSeverity(winnerMismatchRate, NORTHBEAM_BENCHMARK_THRESHOLDS.winnerMismatchRate)
    );
    severity = maxSeverity(
      severity,
      resolveSeverity(
        unattributedRateDeltaPercentagePoints,
        NORTHBEAM_BENCHMARK_THRESHOLDS.unattributedRateDeltaPercentagePoints
      )
    );

    return {
      modelKey,
      actualAttributedRevenue: centsToRevenue(actualAttributedRevenueCents),
      referenceAttributedRevenue: centsToRevenue(referenceAttributedRevenueCents),
      attributedRevenueDelta: centsToRevenue(Math.abs(actualAttributedRevenueCents - referenceAttributedRevenueCents)),
      attributedRevenueDeltaPercent,
      actualUnattributedOrders,
      referenceUnattributedOrders,
      unattributedOrderCountDelta: actualUnattributedOrders - referenceUnattributedOrders,
      unattributedRateDeltaPercentagePoints,
      winnerMismatchRate,
      touchpointCountMismatchRate,
      severity
    };
  });
}

function buildChannelSlices(comparisons: OrderModelComparison[]): NorthbeamBenchmarkChannelSlice[] {
  const channelMap = new Map<
    string,
    {
      modelKey: AttributionModel;
      channelKey: string;
      actualRevenueCents: number;
      referenceRevenueCents: number;
      actualOrders: Set<string>;
      referenceOrders: Set<string>;
    }
  >();

  for (const comparison of comparisons) {
    for (const credit of comparison.actual.credits) {
      const channelKey = formatChannelKey(credit.source, credit.medium, credit.isDirect);
      const key = `${comparison.modelKey}::${channelKey}`;
      const entry = channelMap.get(key) ?? {
        modelKey: comparison.modelKey,
        channelKey,
        actualRevenueCents: 0,
        referenceRevenueCents: 0,
        actualOrders: new Set<string>(),
        referenceOrders: new Set<string>()
      };

      entry.actualRevenueCents += revenueToCents(credit.revenueCredit);
      entry.actualOrders.add(comparison.orderId);
      channelMap.set(key, entry);
    }

    for (const credit of comparison.reference.credits) {
      const channelKey = formatChannelKey(credit.source, credit.medium, credit.isDirect);
      const key = `${comparison.modelKey}::${channelKey}`;
      const entry = channelMap.get(key) ?? {
        modelKey: comparison.modelKey,
        channelKey,
        actualRevenueCents: 0,
        referenceRevenueCents: 0,
        actualOrders: new Set<string>(),
        referenceOrders: new Set<string>()
      };

      entry.referenceRevenueCents += revenueToCents(credit.revenueCredit);
      entry.referenceOrders.add(comparison.orderId);
      channelMap.set(key, entry);
    }
  }

  return Array.from(channelMap.values())
    .map((entry) => {
      const revenueDeltaPercent = deltaPercent(entry.actualRevenueCents, entry.referenceRevenueCents);
      return {
        modelKey: entry.modelKey,
        channelKey: entry.channelKey,
        actualRevenue: centsToRevenue(entry.actualRevenueCents),
        referenceRevenue: centsToRevenue(entry.referenceRevenueCents),
        absoluteRevenueDelta: centsToRevenue(Math.abs(entry.actualRevenueCents - entry.referenceRevenueCents)),
        revenueDeltaPercent,
        actualOrderCount: entry.actualOrders.size,
        referenceOrderCount: entry.referenceOrders.size,
        orderCountDelta: entry.actualOrders.size - entry.referenceOrders.size,
        severity: resolveSeverity(revenueDeltaPercent, NORTHBEAM_BENCHMARK_THRESHOLDS.channelRevenueDeltaPercent)
      };
    })
    .sort((left, right) => {
      if (left.severity !== right.severity) {
        return ['red', 'yellow', 'green'].indexOf(left.severity) - ['red', 'yellow', 'green'].indexOf(right.severity);
      }

      if (right.revenueDeltaPercent !== left.revenueDeltaPercent) {
        return right.revenueDeltaPercent - left.revenueDeltaPercent;
      }

      return left.channelKey.localeCompare(right.channelKey);
    });
}

function winnerIsDirect(run: ComparableRun): boolean | null {
  const winner = run.credits.find((credit) => credit.isPrimary);
  return winner ? winner.isDirect : null;
}

function usesSyntheticFallback(run: ComparableRun): boolean {
  return run.credits.some((credit) => credit.isSynthetic);
}

function buildCohortSlices(comparisons: OrderModelComparison[]): NorthbeamBenchmarkCohortSlice[] {
  const cohorts = Array.from(new Set(comparisons.map((comparison) => comparison.cohort)));

  return cohorts.flatMap((cohort) =>
    ATTRIBUTION_MODELS.map((modelKey) => {
      const scoped = comparisons.filter((comparison) => comparison.cohort === cohort && comparison.modelKey === modelKey);
      const orderCount = scoped.length;
      const primaryWinnerMismatchRate = percentage(
        scoped.filter(
          (comparison) => comparison.actual.summary.winnerTouchpointId !== comparison.reference.summary.winnerTouchpointId
        ).length,
        orderCount
      );
      const directClassificationMismatchRate = percentage(
        scoped.filter((comparison) => winnerIsDirect(comparison.actual) !== winnerIsDirect(comparison.reference)).length,
        orderCount
      );
      const lookbackEligibilityMismatchRate = percentage(
        scoped.filter(
          (comparison) =>
            comparison.actual.summary.touchpointCountConsidered !== comparison.reference.summary.touchpointCountConsidered ||
            comparison.actual.summary.lookbackRuleApplied !== comparison.reference.summary.lookbackRuleApplied
        ).length,
        orderCount
      );
      const syntheticFallbackUsageMismatchRate = percentage(
        scoped.filter((comparison) => usesSyntheticFallback(comparison.actual) !== usesSyntheticFallback(comparison.reference))
          .length,
        orderCount
      );

      let severity = resolveSeverity(primaryWinnerMismatchRate, NORTHBEAM_BENCHMARK_THRESHOLDS.winnerMismatchRate);
      severity = maxSeverity(
        severity,
        resolveSeverity(directClassificationMismatchRate, NORTHBEAM_BENCHMARK_THRESHOLDS.winnerMismatchRate)
      );
      severity = maxSeverity(
        severity,
        resolveSeverity(lookbackEligibilityMismatchRate, NORTHBEAM_BENCHMARK_THRESHOLDS.winnerMismatchRate)
      );
      severity = maxSeverity(
        severity,
        resolveSeverity(syntheticFallbackUsageMismatchRate, NORTHBEAM_BENCHMARK_THRESHOLDS.winnerMismatchRate)
      );

      return {
        cohort,
        modelKey,
        orderCount,
        primaryWinnerMismatchRate,
        directClassificationMismatchRate,
        lookbackEligibilityMismatchRate,
        syntheticFallbackUsageMismatchRate,
        severity
      };
    })
  );
}

function buildOrderLevelMismatches(comparisons: OrderModelComparison[]): NorthbeamBenchmarkOrderMismatch[] {
  const mismatches: NorthbeamBenchmarkOrderMismatch[] = [];

  for (const comparison of comparisons) {
    const mismatchReasons: string[] = [];
    let severity: Severity = 'green';

    if (comparison.actual.summary.winnerTouchpointId !== comparison.reference.summary.winnerTouchpointId) {
      mismatchReasons.push('winner_mismatch');
      severity = maxSeverity(severity, 'red');
    }

    if (comparison.actual.summary.allocationStatus !== comparison.reference.summary.allocationStatus) {
      mismatchReasons.push('allocation_status_mismatch');
      severity = maxSeverity(severity, 'red');
    }

    if (comparison.actual.summary.touchpointCountConsidered !== comparison.reference.summary.touchpointCountConsidered) {
      mismatchReasons.push('touchpoint_count_mismatch');
      severity = maxSeverity(severity, 'yellow');
    }

    if (usesSyntheticFallback(comparison.actual) !== usesSyntheticFallback(comparison.reference)) {
      mismatchReasons.push('synthetic_fallback_usage_mismatch');
      severity = maxSeverity(severity, 'yellow');
    }

    if (winnerIsDirect(comparison.actual) !== winnerIsDirect(comparison.reference)) {
      mismatchReasons.push('direct_classification_mismatch');
      severity = maxSeverity(severity, 'yellow');
    }

    if (comparison.actual.summary.lookbackRuleApplied !== comparison.reference.summary.lookbackRuleApplied) {
      mismatchReasons.push('lookback_rule_mismatch');
      severity = maxSeverity(severity, 'yellow');
    }

    if (mismatchReasons.length === 0) {
      continue;
    }

    mismatches.push({
      fixtureId: comparison.fixtureId,
      fixtureName: comparison.fixtureName,
      cohort: comparison.cohort,
      orderId: comparison.orderId,
      modelKey: comparison.modelKey,
      severity,
      mismatchReasons,
      actualWinnerTouchpointId: comparison.actual.summary.winnerTouchpointId,
      referenceWinnerTouchpointId: comparison.reference.summary.winnerTouchpointId,
      actualRevenueCredited: comparison.actual.summary.totalRevenueCredited,
      referenceRevenueCredited: comparison.reference.summary.totalRevenueCredited
    });
  }

  return mismatches.sort((left, right) => {
    if (left.severity !== right.severity) {
      return ['red', 'yellow', 'green'].indexOf(left.severity) - ['red', 'yellow', 'green'].indexOf(right.severity);
    }

    if (left.fixtureId !== right.fixtureId) {
      return left.fixtureId.localeCompare(right.fixtureId);
    }

    return left.modelKey.localeCompare(right.modelKey);
  });
}

export function buildNorthbeamBenchmarkReport(
  fixtures: NorthbeamBenchmarkFixture[] = NORTHBEAM_BENCHMARK_FIXTURES
): NorthbeamBenchmarkReport {
  const comparisons = buildComparisons(fixtures);
  const modelSlices = buildModelSlices(comparisons, fixtures.length);
  const channelSlices = buildChannelSlices(comparisons);
  const cohortSlices = buildCohortSlices(comparisons);
  const orderLevelMismatches = buildOrderLevelMismatches(comparisons);

  return {
    reportVersion: 1,
    fixtureSetVersion: NORTHBEAM_BENCHMARK_FIXTURE_SET_VERSION,
    orderCount: fixtures.length,
    thresholds: NORTHBEAM_BENCHMARK_THRESHOLDS,
    modelSlices,
    channelSlices,
    cohortSlices,
    orderLevelMismatches,
    topVarianceDrivers: orderLevelMismatches.slice(0, 10)
  };
}

export function evaluateNorthbeamBenchmarkThresholds(
  report: NorthbeamBenchmarkReport
): NorthbeamBenchmarkGateResult {
  const violations: NorthbeamBenchmarkGateViolation[] = [];

  for (const slice of report.modelSlices) {
    if (slice.severity === 'red') {
      if (slice.attributedRevenueDeltaPercent > report.thresholds.modelRevenueDeltaPercent.yellowMax) {
        violations.push({
          slice: 'model',
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'attributed_revenue_delta_percent',
          details: `${slice.attributedRevenueDeltaPercent}% > ${report.thresholds.modelRevenueDeltaPercent.yellowMax}%`
        });
      }

      if (slice.winnerMismatchRate > report.thresholds.winnerMismatchRate.yellowMax) {
        violations.push({
          slice: 'model',
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'winner_mismatch_rate',
          details: `${slice.winnerMismatchRate}% > ${report.thresholds.winnerMismatchRate.yellowMax}%`
        });
      }

      if (
        slice.unattributedRateDeltaPercentagePoints >
        report.thresholds.unattributedRateDeltaPercentagePoints.yellowMax
      ) {
        violations.push({
          slice: 'model',
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'unattributed_rate_delta_percentage_points',
          details: `${slice.unattributedRateDeltaPercentagePoints}pp > ${report.thresholds.unattributedRateDeltaPercentagePoints.yellowMax}pp`
        });
      }
    }
  }

  for (const slice of report.channelSlices) {
    if (
      slice.severity === 'red' &&
      slice.revenueDeltaPercent > report.thresholds.channelRevenueDeltaPercent.yellowMax
    ) {
      violations.push({
        slice: 'channel',
        modelKey: slice.modelKey,
        channelKey: slice.channelKey,
        severity: slice.severity,
        metric: 'revenue_delta_percent',
        details: `${slice.revenueDeltaPercent}% > ${report.thresholds.channelRevenueDeltaPercent.yellowMax}%`
      });
    }
  }

  for (const slice of report.cohortSlices) {
    if (slice.severity === 'red') {
      if (slice.primaryWinnerMismatchRate > report.thresholds.winnerMismatchRate.yellowMax) {
        violations.push({
          slice: 'cohort',
          cohort: slice.cohort,
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'primary_winner_mismatch_rate',
          details: `${slice.primaryWinnerMismatchRate}% > ${report.thresholds.winnerMismatchRate.yellowMax}%`
        });
      }

      if (slice.directClassificationMismatchRate > report.thresholds.winnerMismatchRate.yellowMax) {
        violations.push({
          slice: 'cohort',
          cohort: slice.cohort,
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'direct_classification_mismatch_rate',
          details: `${slice.directClassificationMismatchRate}% > ${report.thresholds.winnerMismatchRate.yellowMax}%`
        });
      }

      if (slice.lookbackEligibilityMismatchRate > report.thresholds.winnerMismatchRate.yellowMax) {
        violations.push({
          slice: 'cohort',
          cohort: slice.cohort,
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'lookback_eligibility_mismatch_rate',
          details: `${slice.lookbackEligibilityMismatchRate}% > ${report.thresholds.winnerMismatchRate.yellowMax}%`
        });
      }

      if (slice.syntheticFallbackUsageMismatchRate > report.thresholds.winnerMismatchRate.yellowMax) {
        violations.push({
          slice: 'cohort',
          cohort: slice.cohort,
          modelKey: slice.modelKey,
          severity: slice.severity,
          metric: 'synthetic_fallback_usage_mismatch_rate',
          details: `${slice.syntheticFallbackUsageMismatchRate}% > ${report.thresholds.winnerMismatchRate.yellowMax}%`
        });
      }
    }
  }

  return {
    ok: violations.length === 0,
    violations
  };
}

export function formatNorthbeamBenchmarkThresholdFailures(result: NorthbeamBenchmarkGateResult): string {
  if (result.ok) {
    return 'Northbeam benchmark parity thresholds satisfied.';
  }

  const lines = ['Northbeam benchmark parity threshold failures:'];

  for (const violation of result.violations) {
    if (violation.slice === 'model') {
      lines.push(`- model ${violation.modelKey} ${violation.metric}: ${violation.details}`);
      continue;
    }

    if (violation.slice === 'channel') {
      lines.push(
        `- channel ${violation.modelKey} ${violation.channelKey} ${violation.metric}: ${violation.details}`
      );
      continue;
    }

    lines.push(
      `- cohort ${violation.cohort} ${violation.modelKey} ${violation.metric}: ${violation.details}`
    );
  }

  return lines.join('\n');
}

export function assertNorthbeamBenchmarkThresholds(report: NorthbeamBenchmarkReport): void {
  const result = evaluateNorthbeamBenchmarkThresholds(report);

  if (!result.ok) {
    throw new Error(formatNorthbeamBenchmarkThresholdFailures(result));
  }
}

export function renderNorthbeamBenchmarkReportJson(report: NorthbeamBenchmarkReport): string {
  return `${JSON.stringify(report, null, 2)}\n`;
}

function formatSeverityLabel(severity: Severity): string {
  return severity.toUpperCase();
}

export function renderNorthbeamBenchmarkReportMarkdown(report: NorthbeamBenchmarkReport): string {
  const lines: string[] = [];

  lines.push('# Northbeam parity report');
  lines.push('');
  lines.push(`- Fixture set: \`${report.fixtureSetVersion}\``);
  lines.push(`- Orders benchmarked: \`${report.orderCount}\``);
  lines.push(`- Models benchmarked: \`${ATTRIBUTION_MODELS.join('`, `')}\``);
  lines.push('');
  lines.push('## Model slice');
  lines.push('');
  lines.push('| Model | Severity | Actual revenue | Reference revenue | Delta | Delta % | Winner mismatch | Touchpoint mismatch | Unattributed delta |');
  lines.push('| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');

  for (const slice of report.modelSlices) {
    lines.push(
      `| ${slice.modelKey} | ${formatSeverityLabel(slice.severity)} | ${slice.actualAttributedRevenue} | ${slice.referenceAttributedRevenue} | ${slice.attributedRevenueDelta} | ${slice.attributedRevenueDeltaPercent}% | ${slice.winnerMismatchRate}% | ${slice.touchpointCountMismatchRate}% | ${slice.unattributedOrderCountDelta} (${slice.unattributedRateDeltaPercentagePoints}pp) |`
    );
  }

  lines.push('');
  lines.push('## Channel slice');
  lines.push('');
  lines.push('| Model | Channel | Severity | Actual revenue | Reference revenue | Delta | Delta % | Order delta |');
  lines.push('| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |');

  for (const slice of report.channelSlices) {
    lines.push(
      `| ${slice.modelKey} | ${slice.channelKey} | ${formatSeverityLabel(slice.severity)} | ${slice.actualRevenue} | ${slice.referenceRevenue} | ${slice.absoluteRevenueDelta} | ${slice.revenueDeltaPercent}% | ${slice.orderCountDelta} |`
    );
  }

  lines.push('');
  lines.push('## Cohort slice');
  lines.push('');
  lines.push('| Cohort | Model | Severity | Orders | Winner mismatch | Direct mismatch | Lookback mismatch | Synthetic fallback mismatch |');
  lines.push('| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |');

  for (const slice of report.cohortSlices) {
    lines.push(
      `| ${slice.cohort} | ${slice.modelKey} | ${formatSeverityLabel(slice.severity)} | ${slice.orderCount} | ${slice.primaryWinnerMismatchRate}% | ${slice.directClassificationMismatchRate}% | ${slice.lookbackEligibilityMismatchRate}% | ${slice.syntheticFallbackUsageMismatchRate}% |`
    );
  }

  lines.push('');
  lines.push('## Top variance drivers');
  lines.push('');

  if (report.topVarianceDrivers.length === 0) {
    lines.push('No mismatches detected.');
  } else {
    for (const driver of report.topVarianceDrivers) {
      lines.push(
        `- ${driver.severity.toUpperCase()} ${driver.modelKey} / ${driver.cohort} / ${driver.fixtureId}: ${driver.mismatchReasons.join(', ')}. Actual winner \`${driver.actualWinnerTouchpointId ?? 'none'}\`, reference winner \`${driver.referenceWinnerTouchpointId ?? 'none'}\`.`
      );
    }
  }

  lines.push('');
  lines.push('## Thresholds');
  lines.push('');
  lines.push('- Green: model revenue delta <= 1.0%, channel delta <= 2.0%, winner mismatch <= 3.0%, unattributed delta <= 0.5pp.');
  lines.push('- Yellow: model revenue delta <= 3.0%, channel delta <= 5.0%, winner mismatch <= 8.0%, unattributed delta <= 1.5pp.');
  lines.push('- Red: values above yellow thresholds.');
  lines.push('');

  return `${lines.join('\n')}\n`;
}
