export const ATTRIBUTION_MODELS = [
  'first_touch',
  'last_touch',
  'linear',
  'time_decay',
  'position_based',
  'rule_based_weighted'
] as const;

export type AttributionModel = (typeof ATTRIBUTION_MODELS)[number];

export type AttributionTouchpoint = {
  sessionId: string | null;
  occurredAt: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  isDirect: boolean;
  isForced: boolean;
};

export type RuleBasedWeightConfig = {
  firstTouchWeight?: number;
  middleTouchWeight?: number;
  lastTouchWeight?: number;
  clickIdBonusMultiplier?: number;
  directDiscountMultiplier?: number;
  sourceWeights?: Record<string, number>;
  mediumWeights?: Record<string, number>;
};

export type AttributionEngineOptions = {
  orderOccurredAt: Date;
  orderRevenue: number | string;
  timeDecayHalfLifeDays?: number;
  positionBasedFirstWeight?: number;
  positionBasedLastWeight?: number;
  ruleBasedWeightConfig?: RuleBasedWeightConfig;
};

export type AttributionCredit = {
  attributionModel: AttributionModel;
  touchpointPosition: number;
  sessionId: string | null;
  touchpointOccurredAt: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  creditWeight: number;
  revenueCredit: string;
  isPrimary: boolean;
};

export type AttributionModelOutputs = Record<AttributionModel, AttributionCredit[]>;

const DEFAULT_TIME_DECAY_HALF_LIFE_DAYS = 7;
const DEFAULT_POSITION_BASED_FIRST_WEIGHT = 0.4;
const DEFAULT_POSITION_BASED_LAST_WEIGHT = 0.4;
const DEFAULT_RULE_BASED_WEIGHTS: Required<
  Pick<
    RuleBasedWeightConfig,
    'firstTouchWeight' | 'middleTouchWeight' | 'lastTouchWeight' | 'clickIdBonusMultiplier' | 'directDiscountMultiplier'
  >
> = {
  firstTouchWeight: 0.3,
  middleTouchWeight: 0.2,
  lastTouchWeight: 0.5,
  clickIdBonusMultiplier: 1.25,
  directDiscountMultiplier: 0.5
};

function toPositiveNumber(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return value;
}

function revenueToCents(value: number | string): number {
  const numericValue = typeof value === 'number' ? value : Number.parseFloat(value);

  if (!Number.isFinite(numericValue) || numericValue < 0) {
    throw new Error(`orderRevenue must be a finite non-negative number, received ${String(value)}`);
  }

  return Math.round(numericValue * 100);
}

function centsToRevenue(cents: number): string {
  return (cents / 100).toFixed(2);
}

function normalizeWeights(rawWeights: number[]): number[] {
  const positiveWeights = rawWeights.map((value) => (Number.isFinite(value) && value > 0 ? value : 0));
  const totalWeight = positiveWeights.reduce((sum, value) => sum + value, 0);

  if (totalWeight <= 0) {
    return positiveWeights.length === 0 ? [] : positiveWeights.map(() => 1 / positiveWeights.length);
  }

  return positiveWeights.map((value) => value / totalWeight);
}

function allocateRevenueAcrossWeights(totalCents: number, normalizedWeights: number[]): number[] {
  if (normalizedWeights.length === 0) {
    return [];
  }

  const provisional = normalizedWeights.map((weight, index) => {
    const exactCents = totalCents * weight;
    const wholeCents = Math.floor(exactCents);

    return {
      index,
      wholeCents,
      remainder: exactCents - wholeCents
    };
  });

  let distributedCents = provisional.reduce((sum, entry) => sum + entry.wholeCents, 0);
  const remainingCents = totalCents - distributedCents;

  provisional
    .slice()
    .sort((left, right) => {
      if (right.remainder !== left.remainder) {
        return right.remainder - left.remainder;
      }

      return left.index - right.index;
    })
    .slice(0, remainingCents)
    .forEach((entry) => {
      provisional[entry.index].wholeCents += 1;
      distributedCents += 1;
    });

  if (distributedCents !== totalCents) {
    throw new Error('Revenue allocation failed to conserve cents');
  }

  return provisional.map((entry) => entry.wholeCents);
}

function buildCredits(
  attributionModel: AttributionModel,
  touchpoints: AttributionTouchpoint[],
  normalizedWeights: number[],
  totalRevenue: number | string
): AttributionCredit[] {
  const totalCents = revenueToCents(totalRevenue);
  const creditedCents = allocateRevenueAcrossWeights(totalCents, normalizedWeights);
  const highestCreditCents = creditedCents.reduce((max, value) => Math.max(max, value), 0);
  const primaryTouchpointIndex = creditedCents.findIndex((value) => value === highestCreditCents);

  return touchpoints.map((touchpoint, index) => ({
    attributionModel,
    touchpointPosition: index,
    sessionId: touchpoint.sessionId,
    touchpointOccurredAt: touchpoint.occurredAt,
    source: touchpoint.source,
    medium: touchpoint.medium,
    campaign: touchpoint.campaign,
    content: touchpoint.content,
    term: touchpoint.term,
    clickIdType: touchpoint.clickIdType,
    clickIdValue: touchpoint.clickIdValue,
    attributionReason: touchpoint.attributionReason,
    creditWeight: normalizedWeights[index] ?? 0,
    revenueCredit: centsToRevenue(creditedCents[index] ?? 0),
    isPrimary: index === primaryTouchpointIndex
  }));
}

function firstTouchWeights(touchpoints: AttributionTouchpoint[]): number[] {
  return touchpoints.map((_touchpoint, index) => (index === 0 ? 1 : 0));
}

function lastTouchWeights(touchpoints: AttributionTouchpoint[]): number[] {
  const lastIndex = Math.max(touchpoints.length - 1, 0);
  return touchpoints.map((_touchpoint, index) => (index === lastIndex ? 1 : 0));
}

function linearWeights(touchpoints: AttributionTouchpoint[]): number[] {
  return touchpoints.map(() => 1);
}

function timeDecayWeights(touchpoints: AttributionTouchpoint[], orderOccurredAt: Date, halfLifeDays: number): number[] {
  const halfLifeMs = toPositiveNumber(halfLifeDays, DEFAULT_TIME_DECAY_HALF_LIFE_DAYS) * 24 * 60 * 60 * 1000;

  return touchpoints.map((touchpoint) => {
    const deltaMs = Math.max(orderOccurredAt.getTime() - touchpoint.occurredAt.getTime(), 0);
    return Math.pow(0.5, deltaMs / halfLifeMs);
  });
}

function positionBasedWeights(
  touchpoints: AttributionTouchpoint[],
  firstWeight: number,
  lastWeight: number
): number[] {
  if (touchpoints.length <= 1) {
    return [1];
  }

  if (touchpoints.length === 2) {
    return [1, 1];
  }

  const normalizedFirstWeight = Math.min(Math.max(firstWeight, 0), 1);
  const normalizedLastWeight = Math.min(Math.max(lastWeight, 0), 1);
  const remainingWeight = Math.max(1 - normalizedFirstWeight - normalizedLastWeight, 0);
  const middleWeight = remainingWeight / Math.max(touchpoints.length - 2, 1);

  return touchpoints.map((_touchpoint, index) => {
    if (index === 0) {
      return normalizedFirstWeight;
    }

    if (index === touchpoints.length - 1) {
      return normalizedLastWeight;
    }

    return middleWeight;
  });
}

function ruleBasedWeights(touchpoints: AttributionTouchpoint[], config: RuleBasedWeightConfig | undefined): number[] {
  const mergedConfig = {
    ...DEFAULT_RULE_BASED_WEIGHTS,
    ...config
  };
  const lastIndex = Math.max(touchpoints.length - 1, 0);

  return touchpoints.map((touchpoint, index) => {
    let positionWeight = mergedConfig.middleTouchWeight;

    if (touchpoints.length === 1) {
      positionWeight = 1;
    } else if (index === 0) {
      positionWeight = mergedConfig.firstTouchWeight;
    } else if (index === lastIndex) {
      positionWeight = mergedConfig.lastTouchWeight;
    }

    let multiplier = 1;

    if (touchpoint.clickIdValue) {
      multiplier *= mergedConfig.clickIdBonusMultiplier;
    }

    if (touchpoint.isDirect) {
      multiplier *= mergedConfig.directDiscountMultiplier;
    }

    const normalizedSource = touchpoint.source?.trim().toLowerCase() ?? null;
    const normalizedMedium = touchpoint.medium?.trim().toLowerCase() ?? null;

    if (normalizedSource && config?.sourceWeights?.[normalizedSource]) {
      multiplier *= config.sourceWeights[normalizedSource];
    }

    if (normalizedMedium && config?.mediumWeights?.[normalizedMedium]) {
      multiplier *= config.mediumWeights[normalizedMedium];
    }

    return positionWeight * multiplier;
  });
}

export function buildUnattributedTouchpoint(orderOccurredAt: Date): AttributionTouchpoint {
  return {
    sessionId: null,
    occurredAt: orderOccurredAt,
    source: null,
    medium: null,
    campaign: null,
    content: null,
    term: null,
    clickIdType: null,
    clickIdValue: null,
    attributionReason: 'unattributed',
    isDirect: true,
    isForced: true
  };
}

export function computeAttributionOutputs(
  rawTouchpoints: AttributionTouchpoint[],
  options: AttributionEngineOptions
): AttributionModelOutputs {
  const touchpoints = rawTouchpoints.length > 0 ? rawTouchpoints : [buildUnattributedTouchpoint(options.orderOccurredAt)];
  const positionFirstWeight = toPositiveNumber(
    options.positionBasedFirstWeight,
    DEFAULT_POSITION_BASED_FIRST_WEIGHT
  );
  const positionLastWeight = toPositiveNumber(
    options.positionBasedLastWeight,
    DEFAULT_POSITION_BASED_LAST_WEIGHT
  );
  const modelWeights: Record<AttributionModel, number[]> = {
    first_touch: normalizeWeights(firstTouchWeights(touchpoints)),
    last_touch: normalizeWeights(lastTouchWeights(touchpoints)),
    linear: normalizeWeights(linearWeights(touchpoints)),
    time_decay: normalizeWeights(
      timeDecayWeights(
        touchpoints,
        options.orderOccurredAt,
        toPositiveNumber(options.timeDecayHalfLifeDays, DEFAULT_TIME_DECAY_HALF_LIFE_DAYS)
      )
    ),
    position_based: normalizeWeights(positionBasedWeights(touchpoints, positionFirstWeight, positionLastWeight)),
    rule_based_weighted: normalizeWeights(ruleBasedWeights(touchpoints, options.ruleBasedWeightConfig))
  };

  return ATTRIBUTION_MODELS.reduce<AttributionModelOutputs>((outputs, attributionModel) => {
    outputs[attributionModel] = buildCredits(
      attributionModel,
      touchpoints,
      modelWeights[attributionModel],
      options.orderRevenue
    );
    return outputs;
  }, {} as AttributionModelOutputs);
}
