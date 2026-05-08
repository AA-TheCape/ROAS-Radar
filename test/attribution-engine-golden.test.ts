import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ATTRIBUTION_MODELS,
  executeAttributionModels,
  type AttributionCredit,
  type AttributionModel
} from '../src/modules/attribution/engine.js';
import { ATTRIBUTION_ENGINE_GOLDEN_FIXTURES } from './fixtures/attribution-engine-golden.fixtures.js';

function revenueToCents(value: string): number {
  return Math.round(Number.parseFloat(value) * 100);
}

function creditMap(credits: AttributionCredit[]): Record<string, string> {
  return Object.fromEntries(credits.map((credit) => [credit.touchpointId ?? 'null', credit.revenueCredit]));
}

function assertModelInvariants(
  model: AttributionModel,
  orderRevenue: string,
  credits: AttributionCredit[],
  summary: ReturnType<typeof executeAttributionModels>['summariesByModel'][AttributionModel]
) {
  const totalRevenueFromCredits = credits.reduce((sum, credit) => sum + revenueToCents(credit.revenueCredit), 0);
  const totalWeightFromCredits = credits.reduce((sum, credit) => sum + credit.creditWeight, 0);
  const positiveCredits = credits.filter((credit) => revenueToCents(credit.revenueCredit) > 0);
  const primaryCredits = credits.filter((credit) => credit.isPrimary);

  assert.equal(totalRevenueFromCredits, revenueToCents(summary.totalRevenueCredited), `${model} summary revenue mismatch`);
  assert.equal(Number(totalWeightFromCredits.toFixed(8)), summary.totalCreditWeight, `${model} summary weight mismatch`);

  if (summary.allocationStatus === 'attributed') {
    assert.equal(summary.totalRevenueCredited, orderRevenue, `${model} attributed summary must conserve order revenue`);
    assert.equal(summary.totalCreditWeight, 1, `${model} attributed summary must allocate total weight 1`);
    assert.equal(primaryCredits.length, 1, `${model} attributed result must expose exactly one primary credit`);
    assert.equal(primaryCredits[0].touchpointId, summary.winnerTouchpointId, `${model} winner must match primary credit`);

    if (model === 'linear') {
      assert.equal(positiveCredits.length, credits.length, 'linear should assign positive credit to every considered touch');
    } else {
      assert.equal(positiveCredits.length, 1, `${model} winner-take-all model must have one positive credit`);
    }
  } else {
    assert.equal(summary.totalRevenueCredited, '0.00', `${model} non-attributed summary must not credit revenue`);
    assert.equal(summary.totalCreditWeight, 0, `${model} non-attributed summary must not allocate weight`);
    assert.equal(positiveCredits.length, 0, `${model} non-attributed result must not include positive credits`);
    assert.equal(primaryCredits.length, 0, `${model} non-attributed result must not include a primary credit`);
    assert.equal(summary.winnerTouchpointId, null, `${model} non-attributed result must not expose a winner`);
  }
}

for (const fixture of ATTRIBUTION_ENGINE_GOLDEN_FIXTURES) {
  test(`golden attribution dataset: ${fixture.name}`, () => {
    const result = executeAttributionModels(fixture.touchpoints, {
      orderOccurredAt: new Date(fixture.orderOccurredAt),
      orderRevenue: fixture.orderRevenue
    });

    assert.deepEqual(result.models, [...ATTRIBUTION_MODELS]);

    for (const model of ATTRIBUTION_MODELS) {
      const expectation = fixture.expectedByModel[model];
      assert.ok(expectation, `missing expectation for ${model} in fixture ${fixture.name}`);

      const credits = result.creditsByModel[model];
      const summary = result.summariesByModel[model];

      assert.equal(summary.attributionModel, model);
      assert.equal(summary.winnerSelectionRule, model);
      assert.equal(summary.allocationStatus, expectation.allocationStatus);
      assert.equal(summary.winnerTouchpointId, expectation.winnerTouchpointId);

      if (expectation.totalRevenueCredited) {
        assert.equal(summary.totalRevenueCredited, expectation.totalRevenueCredited);
      }

      if (expectation.touchpointCountConsidered !== undefined) {
        assert.equal(summary.touchpointCountConsidered, expectation.touchpointCountConsidered);
      }

      if (expectation.eligibleClickCount !== undefined) {
        assert.equal(summary.eligibleClickCount, expectation.eligibleClickCount);
      }

      if (expectation.eligibleViewCount !== undefined) {
        assert.equal(summary.eligibleViewCount, expectation.eligibleViewCount);
      }

      if (expectation.lookbackRuleApplied) {
        assert.equal(summary.lookbackRuleApplied, expectation.lookbackRuleApplied);
      }

      if (expectation.directSuppressionApplied !== undefined) {
        assert.equal(summary.directSuppressionApplied, expectation.directSuppressionApplied);
      }

      if (expectation.deterministicBlockApplied !== undefined) {
        assert.equal(summary.deterministicBlockApplied, expectation.deterministicBlockApplied);
      }

      if (expectation.revenueCredits) {
        assert.deepEqual(creditMap(credits), expectation.revenueCredits);
      }

      assertModelInvariants(model, fixture.orderRevenue, credits, summary);
    }
  });
}
