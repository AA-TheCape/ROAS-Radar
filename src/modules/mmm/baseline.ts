import { createHash } from 'node:crypto';

import type { PoolClient } from 'pg';

import { withTransaction } from '../../db/pool.js';

export const MMM_BASELINE_MODEL_VERSION = 'baseline_linear_mmm_v1';
export const MMM_BASELINE_MODEL_TYPE = 'baseline_linear_mmm';
export const MMM_BASELINE_MART_VERSION = 'mmm_daily_input_mart_v1';

const DEFAULT_MAX_SEGMENTS = 8;
const DEFAULT_ADSTOCK_DECAY = 0.5;
const DEFAULT_RIDGE_LAMBDA = 1;
const MIN_OBSERVATIONS = 3;

export type MmmBaselineTrainingInput = {
  startDate: string;
  endDate: string;
  attributionModel?: string;
  maxSegments?: number;
  adstockDecay?: number;
  ridgeLambda?: number;
  holdoutRatio?: number;
  submittedBy?: string;
};

type MmmBaselineMartRow = {
  metric_date: string;
  mart_row_type: 'paid_media' | 'attribution';
  attribution_model: string;
  platform: string;
  source: string;
  medium: string;
  campaign: string;
  spend: string | number;
  impressions: string | number;
  clicks: string | number;
  shopify_revenue: string | number;
  attribution_credit_revenue: string | number;
  attribution_credit_orders: string | number;
  match_source_coverage: unknown;
  confidence_label_coverage: unknown;
};

type SegmentStats = {
  key: string;
  source: string;
  medium: string;
  campaign: string;
  spend: number;
  impressions: number;
  clicks: number;
  attributedRevenue: number;
  attributedOrders: number;
};

type DailyObservation = {
  date: string;
  revenue: number;
  features: Record<string, number>;
};

export type MmmBaselineModelRun = {
  id: string | null;
  modelType: typeof MMM_BASELINE_MODEL_TYPE;
  modelVersion: typeof MMM_BASELINE_MODEL_VERSION;
  martVersion: typeof MMM_BASELINE_MART_VERSION;
  attributionModel: string;
  trainingStartDate: string;
  trainingEndDate: string;
  holdoutStartDate: string | null;
  holdoutEndDate: string | null;
  runConfig: Record<string, unknown>;
  inputSummary: Record<string, unknown>;
  modelArtifact: Record<string, unknown>;
  calibrationReport: Record<string, unknown>;
  validationReport: Record<string, unknown>;
};

function toNumber(value: string | number | null | undefined): number {
  if (value === null || value === undefined) {
    return 0;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function normalizeDate(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new Error(`${fieldName} must use YYYY-MM-DD format`);
  }

  return trimmed;
}

function clampInteger(value: number | undefined, fallback: number, min: number, max: number): number {
  const numeric = Number(value ?? fallback);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.min(max, Math.max(min, Math.trunc(numeric)));
}

function clampNumber(value: number | undefined, fallback: number, min: number, max: number): number {
  const numeric = Number(value ?? fallback);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }

  return Math.min(max, Math.max(min, numeric));
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`;
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(',')}}`;
  }

  return JSON.stringify(value);
}

function hashJson(value: unknown): string {
  return createHash('sha256').update(stableStringify(value)).digest('hex');
}

function segmentKey(row: Pick<MmmBaselineMartRow, 'source' | 'medium' | 'campaign'>): string {
  return [row.source || 'unknown', row.medium || 'unknown', row.campaign || 'unknown'].join('|');
}

function addSegmentMetric(target: Map<string, SegmentStats>, key: string, row: MmmBaselineMartRow): SegmentStats {
  const existing = target.get(key);
  if (existing) {
    return existing;
  }

  const created = {
    key,
    source: row.source || 'unknown',
    medium: row.medium || 'unknown',
    campaign: row.campaign || 'unknown',
    spend: 0,
    impressions: 0,
    clicks: 0,
    attributedRevenue: 0,
    attributedOrders: 0
  };
  target.set(key, created);
  return created;
}

function applyAdstock(values: number[], decay: number): number[] {
  const transformed: number[] = [];
  let carry = 0;

  for (const value of values) {
    carry = value + carry * decay;
    transformed.push(Math.log1p(carry));
  }

  return transformed;
}

function solveLinearSystem(matrix: number[][], vector: number[]): number[] {
  const size = vector.length;
  const augmented = matrix.map((row, index) => [...row, vector[index] ?? 0]);

  for (let column = 0; column < size; column += 1) {
    let pivotRow = column;
    for (let row = column + 1; row < size; row += 1) {
      if (Math.abs(augmented[row][column] ?? 0) > Math.abs(augmented[pivotRow][column] ?? 0)) {
        pivotRow = row;
      }
    }

    [augmented[column], augmented[pivotRow]] = [augmented[pivotRow], augmented[column]];

    const pivot = augmented[column][column] ?? 0;
    if (Math.abs(pivot) < 1e-12) {
      continue;
    }

    for (let entry = column; entry <= size; entry += 1) {
      augmented[column][entry] = (augmented[column][entry] ?? 0) / pivot;
    }

    for (let row = 0; row < size; row += 1) {
      if (row === column) {
        continue;
      }

      const factor = augmented[row][column] ?? 0;
      for (let entry = column; entry <= size; entry += 1) {
        augmented[row][entry] = (augmented[row][entry] ?? 0) - factor * (augmented[column][entry] ?? 0);
      }
    }
  }

  return augmented.map((row) => row[size] ?? 0);
}

function fitRidgeRegression(observations: DailyObservation[], segmentKeys: string[], lambda: number) {
  const featureCount = segmentKeys.length + 1;
  const matrix = Array.from({ length: featureCount }, () => Array.from({ length: featureCount }, () => 0));
  const vector = Array.from({ length: featureCount }, () => 0);

  for (const observation of observations) {
    const row = [1, ...segmentKeys.map((key) => observation.features[key] ?? 0)];
    for (let left = 0; left < featureCount; left += 1) {
      vector[left] += row[left] * observation.revenue;
      for (let right = 0; right < featureCount; right += 1) {
        matrix[left][right] += row[left] * row[right];
      }
    }
  }

  for (let index = 1; index < featureCount; index += 1) {
    matrix[index][index] += lambda;
  }

  const coefficients = solveLinearSystem(matrix, vector);

  return {
    intercept: coefficients[0] ?? 0,
    coefficients: Object.fromEntries(segmentKeys.map((key, index) => [key, coefficients[index + 1] ?? 0]))
  };
}

function predict(observation: DailyObservation, intercept: number, coefficients: Record<string, number>): number {
  return Object.entries(coefficients).reduce(
    (sum, [key, coefficient]) => sum + (observation.features[key] ?? 0) * coefficient,
    intercept
  );
}

function validationMetrics(observations: DailyObservation[], intercept: number, coefficients: Record<string, number>) {
  if (observations.length === 0) {
    return {
      observationCount: 0,
      mape: null,
      rmse: null,
      meanActualRevenue: null,
      meanPredictedRevenue: null
    };
  }

  const scored = observations.map((observation) => ({
    actual: observation.revenue,
    predicted: Math.max(0, predict(observation, intercept, coefficients))
  }));
  const squaredError = scored.reduce((sum, row) => sum + (row.actual - row.predicted) ** 2, 0);
  const absolutePercentageError = scored.reduce(
    (sum, row) => sum + (row.actual > 0 ? Math.abs(row.actual - row.predicted) / row.actual : 0),
    0
  );

  return {
    observationCount: observations.length,
    mape: absolutePercentageError / observations.length,
    rmse: Math.sqrt(squaredError / observations.length),
    meanActualRevenue: scored.reduce((sum, row) => sum + row.actual, 0) / observations.length,
    meanPredictedRevenue: scored.reduce((sum, row) => sum + row.predicted, 0) / observations.length
  };
}

export function buildBaselineMmmArtifact(rows: MmmBaselineMartRow[], input: MmmBaselineTrainingInput): MmmBaselineModelRun {
  const startDate = normalizeDate(input.startDate, 'startDate');
  const endDate = normalizeDate(input.endDate, 'endDate');
  if (startDate > endDate) {
    throw new Error('startDate must be on or before endDate');
  }

  const attributionModel = input.attributionModel?.trim() || 'last_touch';
  const maxSegments = clampInteger(input.maxSegments, DEFAULT_MAX_SEGMENTS, 1, 25);
  const adstockDecay = clampNumber(input.adstockDecay, DEFAULT_ADSTOCK_DECAY, 0, 0.95);
  const ridgeLambda = clampNumber(input.ridgeLambda, DEFAULT_RIDGE_LAMBDA, 0, 10_000);
  const holdoutRatio = clampNumber(input.holdoutRatio, 0.2, 0, 0.5);

  const dates = Array.from(new Set(rows.map((row) => row.metric_date))).sort();
  const paidRows = rows.filter((row) => row.mart_row_type === 'paid_media');
  const attributionRows = rows.filter(
    (row) => row.mart_row_type === 'attribution' && row.attribution_model === attributionModel
  );

  const segmentStats = new Map<string, SegmentStats>();
  const dailySpendBySegment = new Map<string, Map<string, number>>();
  for (const row of paidRows) {
    const key = segmentKey(row);
    const stats = addSegmentMetric(segmentStats, key, row);
    stats.spend += toNumber(row.spend);
    stats.impressions += toNumber(row.impressions);
    stats.clicks += toNumber(row.clicks);

    const daily = dailySpendBySegment.get(row.metric_date) ?? new Map<string, number>();
    daily.set(key, (daily.get(key) ?? 0) + toNumber(row.spend));
    dailySpendBySegment.set(row.metric_date, daily);
  }

  const dailyRevenue = new Map<string, number>();
  const attributionSegments = new Map<string, SegmentStats>();
  for (const row of attributionRows) {
    const key = segmentKey(row);
    const stats = addSegmentMetric(attributionSegments, key, row);
    stats.attributedRevenue += toNumber(row.attribution_credit_revenue);
    stats.attributedOrders += toNumber(row.attribution_credit_orders);
    dailyRevenue.set(row.metric_date, (dailyRevenue.get(row.metric_date) ?? 0) + toNumber(row.shopify_revenue));
  }

  for (const [key, attributionStats] of attributionSegments) {
    const stats = segmentStats.get(key) ?? attributionStats;
    stats.attributedRevenue = attributionStats.attributedRevenue;
    stats.attributedOrders = attributionStats.attributedOrders;
    segmentStats.set(key, stats);
  }

  const selectedSegments = [...segmentStats.values()]
    .filter((segment) => segment.spend > 0)
    .sort((left, right) => right.spend - left.spend || left.key.localeCompare(right.key))
    .slice(0, maxSegments)
    .map((segment) => segment.key);
  const selectedSegmentSet = new Set(selectedSegments);
  const modelSegments = [...selectedSegments, '__other_paid__'];
  const spendSeries = Object.fromEntries(modelSegments.map((key) => [key, dates.map(() => 0)]));

  dates.forEach((date, dateIndex) => {
    const daily = dailySpendBySegment.get(date);
    if (!daily) {
      return;
    }

    for (const [key, spend] of daily) {
      const featureKey = selectedSegmentSet.has(key) ? key : '__other_paid__';
      spendSeries[featureKey][dateIndex] += spend;
    }
  });

  const transformedSeries = Object.fromEntries(
    Object.entries(spendSeries).map(([key, values]) => [key, applyAdstock(values, adstockDecay)])
  );
  const observations = dates
    .filter((date) => dailyRevenue.has(date))
    .map((date) => {
      const dateIndex = dates.indexOf(date);
      return {
        date,
        revenue: dailyRevenue.get(date) ?? 0,
        features: Object.fromEntries(modelSegments.map((key) => [key, transformedSeries[key][dateIndex] ?? 0]))
      };
    });

  if (observations.length < MIN_OBSERVATIONS) {
    throw new Error(`MMM baseline requires at least ${MIN_OBSERVATIONS} daily observations from the approved mart`);
  }

  const holdoutCount = observations.length >= 8 ? Math.max(1, Math.floor(observations.length * holdoutRatio)) : 0;
  const trainingObservations = holdoutCount > 0 ? observations.slice(0, -holdoutCount) : observations;
  const holdoutObservations = holdoutCount > 0 ? observations.slice(-holdoutCount) : [];
  const fitted = fitRidgeRegression(trainingObservations, modelSegments, ridgeLambda);
  const trainMetrics = validationMetrics(trainingObservations, fitted.intercept, fitted.coefficients);
  const holdoutMetrics = validationMetrics(holdoutObservations, fitted.intercept, fitted.coefficients);
  const totalAttributedRevenue = [...segmentStats.values()].reduce((sum, segment) => sum + segment.attributedRevenue, 0);
  const coefficientRevenue = Object.fromEntries(
    modelSegments.map((key) => {
      const totalFeature = observations.reduce((sum, observation) => sum + (observation.features[key] ?? 0), 0);
      return [key, Math.max(0, (fitted.coefficients[key] ?? 0) * totalFeature)];
    })
  );
  const totalCoefficientRevenue = Object.values(coefficientRevenue).reduce((sum, value) => sum + value, 0);
  const calibrationSegments = [...segmentStats.values()]
    .filter((segment) => selectedSegmentSet.has(segment.key))
    .map((segment) => {
      const modeledRevenue = coefficientRevenue[segment.key] ?? 0;
      return {
        key: segment.key,
        source: segment.source,
        medium: segment.medium,
        campaign: segment.campaign,
        spend: segment.spend,
        attributedRevenue: segment.attributedRevenue,
        attributedRevenueShare: totalAttributedRevenue > 0 ? segment.attributedRevenue / totalAttributedRevenue : null,
        modeledRevenue,
        modeledRevenueShare: totalCoefficientRevenue > 0 ? modeledRevenue / totalCoefficientRevenue : null,
        calibrationRatio: segment.attributedRevenue > 0 && modeledRevenue > 0 ? modeledRevenue / segment.attributedRevenue : null
      };
    });
  const config = {
    attributionModel,
    maxSegments,
    adstockDecay,
    ridgeLambda,
    holdoutRatio,
    responseVariable: 'daily_total_shopify_revenue_from_mart_outcomes',
    calibrationUse: 'segment attribution credit metrics are validation/calibration diagnostics, not per-segment training labels'
  };
  const inputSummary = {
    rowCount: rows.length,
    paidMediaRowCount: paidRows.length,
    attributionRowCount: attributionRows.length,
    observationCount: observations.length,
    trainingObservationCount: trainingObservations.length,
    holdoutObservationCount: holdoutObservations.length,
    selectedSegments,
    martInputHash: hashJson(rows)
  };

  return {
    id: null,
    modelType: MMM_BASELINE_MODEL_TYPE,
    modelVersion: MMM_BASELINE_MODEL_VERSION,
    martVersion: MMM_BASELINE_MART_VERSION,
    attributionModel,
    trainingStartDate: startDate,
    trainingEndDate: endDate,
    holdoutStartDate: holdoutObservations[0]?.date ?? null,
    holdoutEndDate: holdoutObservations.at(-1)?.date ?? null,
    runConfig: config,
    inputSummary,
    modelArtifact: {
      intercept: fitted.intercept,
      coefficients: fitted.coefficients,
      featureTransform: {
        spend: 'log1p(adstock(spend))',
        adstockDecay
      },
      segments: calibrationSegments.map(({ key, source, medium, campaign, spend }) => ({
        key,
        source,
        medium,
        campaign,
        spend
      }))
    },
    calibrationReport: {
      attributionModel,
      deterministicAttributionUsage: 'calibration_and_validation_segments_only',
      totalAttributedRevenue,
      totalModeledMediaRevenue: totalCoefficientRevenue,
      segments: calibrationSegments
    },
    validationReport: {
      train: trainMetrics,
      holdout: holdoutMetrics
    }
  };
}

export async function trainBaselineMmmModelWithClient(
  client: PoolClient,
  input: MmmBaselineTrainingInput
): Promise<MmmBaselineModelRun> {
  const startDate = normalizeDate(input.startDate, 'startDate');
  const endDate = normalizeDate(input.endDate, 'endDate');
  const attributionModel = input.attributionModel?.trim() || 'last_touch';
  const result = await client.query<MmmBaselineMartRow>(
    `
      SELECT
        metric_date::text,
        mart_row_type,
        attribution_model,
        platform,
        source,
        medium,
        campaign,
        spend,
        impressions,
        clicks,
        shopify_revenue,
        attribution_credit_revenue,
        attribution_credit_orders,
        match_source_coverage,
        confidence_label_coverage
      FROM mmm_daily_input_mart_v1
      WHERE metric_date BETWEEN $1::date AND $2::date
        AND (
          mart_row_type = 'paid_media'
          OR (mart_row_type = 'attribution' AND attribution_model = $3)
        )
      ORDER BY metric_date ASC, mart_row_type ASC, platform ASC, source ASC, medium ASC, campaign ASC
    `,
    [startDate, endDate, attributionModel]
  );
  const run = buildBaselineMmmArtifact(result.rows, { ...input, startDate, endDate, attributionModel });
  const insertResult = await client.query<{ id: string }>(
    `
      INSERT INTO mmm_model_runs (
        model_type,
        model_version,
        mart_version,
        attribution_model,
        run_status,
        training_start_date,
        training_end_date,
        holdout_start_date,
        holdout_end_date,
        run_config,
        input_summary,
        model_artifact,
        calibration_report,
        validation_report,
        completed_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'completed',
        $5::date,
        $6::date,
        $7::date,
        $8::date,
        $9::jsonb,
        $10::jsonb,
        $11::jsonb,
        $12::jsonb,
        $13::jsonb,
        now()
      )
      RETURNING id
    `,
    [
      run.modelType,
      run.modelVersion,
      run.martVersion,
      run.attributionModel,
      run.trainingStartDate,
      run.trainingEndDate,
      run.holdoutStartDate,
      run.holdoutEndDate,
      JSON.stringify(run.runConfig),
      JSON.stringify(run.inputSummary),
      JSON.stringify(run.modelArtifact),
      JSON.stringify(run.calibrationReport),
      JSON.stringify(run.validationReport)
    ]
  );

  return {
    ...run,
    id: insertResult.rows[0]?.id ?? null
  };
}

export async function trainBaselineMmmModel(input: MmmBaselineTrainingInput): Promise<MmmBaselineModelRun> {
  return withTransaction((client) => trainBaselineMmmModelWithClient(client, input));
}
