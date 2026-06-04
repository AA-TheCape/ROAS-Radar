import { createHash } from 'node:crypto';

import { Router } from 'express';
import { z } from 'zod';

import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated, type AuthContext } from '../auth/index.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';
import {
  backfillMmmCampaignMetadata,
  campaignResolverRequestSchema,
  resolveCampaignMetadata
} from '../campaign-resolver/index.js';

class MmmHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'MmmHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

const MMM_SCHEMA_VERSION = 'mmm_daily_input_mart_v1';

const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const mmmQueryBaseSchema = z.object({
  startDate: dateStringSchema,
  endDate: dateStringSchema,
  martRowType: z.enum(['paid_media', 'attribution']).optional(),
  attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
  platform: z.enum(['meta', 'google', 'taxonomy']).optional(),
  source: z.string().trim().min(1).max(200).optional(),
  campaign: z.string().trim().min(1).max(500).optional(),
  format: z.enum(['json', 'csv']).optional().default('json'),
  limit: z.coerce.number().int().positive().max(10000).optional().default(1000),
  offset: z.coerce.number().int().min(0).optional().default(0)
});

const mmmQuerySchema = mmmQueryBaseSchema
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const taxonomyDriftQuerySchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    martRowType: z.enum(['paid_media', 'attribution']).optional(),
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
    platform: z.enum(['meta', 'google', 'taxonomy']).optional(),
    source: z.string().trim().min(1).max(200).optional(),
    campaign: z.string().trim().min(1).max(500).optional(),
    staleAfterDays: z.coerce.number().int().positive().max(365).optional().default(14),
    sampleLimit: z.coerce.number().int().positive().max(50).optional().default(10)
  })
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const campaignResolverBackfillSchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    resolverVersion: z.string().trim().min(1).max(200).optional(),
    limit: z.coerce.number().int().positive().max(50000).optional()
  })
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const modelRunsQuerySchema = z
  .object({
    startDate: dateStringSchema.optional(),
    endDate: dateStringSchema.optional(),
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
    limit: z.coerce.number().int().positive().max(100).optional().default(10)
  })
  .superRefine((value, ctx) => {
    if (value.startDate && value.endDate && value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const readinessGateBaseSchema = mmmQueryBaseSchema.omit({ format: true, limit: true, offset: true });

const readinessGateQuerySchema = readinessGateBaseSchema.superRefine((value, ctx) => {
  if (value.startDate > value.endDate) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'startDate must be on or before endDate',
      path: ['startDate']
    });
  }
});

const readinessGateMutationSchema = z.object({
  owner: z.string().trim().min(1).max(120).optional(),
  reason: z.string().trim().min(1).max(1000).optional(),
  waiver: z
    .object({
      checklistKey: z.string().trim().min(1).max(120),
      reason: z.string().trim().min(1).max(1000),
      expiresAt: z.string().datetime().optional()
    })
    .optional()
});

const readinessGateDecisionSchema = readinessGateBaseSchema.merge(readinessGateMutationSchema).superRefine((value, ctx) => {
  if (value.startDate > value.endDate) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'startDate must be on or before endDate',
      path: ['startDate']
    });
  }
});

const readinessGateWaiverSchema = readinessGateBaseSchema
  .merge(readinessGateMutationSchema.required({ waiver: true }))
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

type MmmQueryInput = z.infer<typeof mmmQuerySchema>;
type TaxonomyDriftQueryInput = z.infer<typeof taxonomyDriftQuerySchema>;

type MmmReadinessStatus = 'ready' | 'partial' | 'not_ready';
type GateChecklistStatus = 'pass' | 'warn' | 'fail' | 'pending' | 'waived';

type MmmReadinessRow = {
  metric_date: string;
  matching_row_count: string | number;
  mart_row_count: string | number;
  generation_timestamp: Date | null;
};

type MmmExportRow = {
  metric_date: string;
  mart_version: string;
  mart_row_type: string;
  attribution_model: string;
  platform: string;
  platform_connection_id: string | number | null;
  granularity: string;
  entity_key: string;
  account_id: string | null;
  account_name: string | null;
  campaign_id: string | null;
  campaign_name: string | null;
  adset_id: string | null;
  adset_name: string | null;
  ad_id: string | null;
  ad_name: string | null;
  creative_id: string | null;
  creative_name: string | null;
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
  currency: string | null;
  spend: string | number;
  impressions: string | number;
  clicks: string | number;
  shopify_orders: string | number;
  shopify_revenue: string | number;
  attribution_credit_orders: string | number;
  attribution_credit_revenue: string | number;
  new_customer_credit_orders: string | number;
  returning_customer_credit_orders: string | number;
  new_customer_credit_revenue: string | number;
  returning_customer_credit_revenue: string | number;
  match_source_coverage: unknown;
  confidence_label_coverage: unknown;
  spend_last_synced_at: Date | null;
  shopify_last_ingested_at: Date | null;
  attribution_last_computed_at: Date | null;
  last_computed_at: Date;
  resolver_version: string | null;
  resolver_source: string | null;
  resolver_confidence: string | number | null;
  resolved_canonical_campaign_id: string | null;
  resolved_canonical_campaign_name: string | null;
  resolved_canonical_source: string | null;
  resolved_canonical_medium: string | null;
  resolved_canonical_channel: string | null;
  resolved_canonical_channel_group: string | null;
  resolved_hierarchy_metadata: unknown;
  needs_metadata_qa: boolean;
};

type MmmModelRunRow = {
  id: string;
  model_type: string;
  model_version: string;
  mart_version: string;
  attribution_model: string;
  run_status: string;
  training_start_date: string;
  training_end_date: string;
  holdout_start_date: string | null;
  holdout_end_date: string | null;
  run_config: unknown;
  input_summary: unknown;
  model_artifact: unknown;
  calibration_report: unknown;
  validation_report: unknown;
  error_code: string | null;
  error_message: string | null;
  created_at: Date | string;
  started_at: Date | string;
  completed_at: Date | string | null;
};

type ExposureCoverageRow = {
  metric_date: string;
  source_platform: string;
  exposure_type: string;
  total_exposures: string | number;
  valid_exposures: string | number;
  invalid_exposures: string | number;
  identity_resolved_exposures: string | number;
  identity_unresolved_exposures: string | number;
  campaign_joinable_exposures: string | number;
  campaign_metadata_resolved_exposures: string | number;
  latest_exposure_at: Date | null;
};

type TaxonomyDriftSummaryRow = {
  metric_date: string | null;
  total_rows: string | number;
  unknown_source_rows: string | number;
  unmapped_source_rows: string | number;
  unknown_or_unmapped_source_rows: string | number;
  unknown_medium_rows: string | number;
  unmapped_medium_rows: string | number;
  unknown_or_unmapped_medium_rows: string | number;
  unresolved_campaign_metadata_rows: string | number;
  stale_campaign_metadata_rows: string | number;
  native_id_eligible_rows: string | number;
  account_id_rows: string | number;
  campaign_id_rows: string | number;
  adset_id_rows: string | number;
  ad_id_rows: string | number;
  creative_id_rows: string | number;
  platform_native_id_rows: string | number;
};

type TaxonomyDriftSampleRow = {
  sample_type: string;
  row_count: string | number;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  platform: string | null;
  mart_row_type: string | null;
  attribution_model: string | null;
  account_id: string | null;
  campaign_id: string | null;
  metadata_last_seen_at: Date | string | null;
  metadata_updated_at: Date | string | null;
};

type DataQualityBlockerRow = {
  check_key: string;
  status: string;
  severity: string;
  discrepancy_count: string | number;
  summary: string;
  checked_at: Date | string;
};

type MmmReadinessGateRow = {
  id: string;
  gate_version: string;
  start_date: string;
  end_date: string;
  mart_row_type: string | null;
  attribution_model: string | null;
  platform: string | null;
  source: string | null;
  campaign: string | null;
  evidence_payload: unknown;
  checklist_statuses: unknown;
  owner_approvals: unknown;
  waivers: unknown;
  unresolved_critical_issue_count: string | number;
  evidence_hash: string;
  gate_status: string;
  final_state: string;
  decision_reason: string | null;
  decided_by: string | null;
  decided_at: Date | string | null;
  created_by: string;
  updated_by: string;
  created_at: Date | string;
  updated_at: Date | string;
};

type JsonRecord = Record<string, unknown>;

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new MmmHttpError(400, 'invalid_request', 'Invalid MMM export query parameters', error.flatten());
    }

    throw error;
  }
}

function buildMmmFilters(input: MmmQueryInput): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.startDate, input.endDate];
  const filters = ['metric_date BETWEEN $1::date AND $2::date'];

  if (input.martRowType) {
    params.push(input.martRowType);
    filters.push(`mart_row_type = $${params.length}`);
  }

  if (input.attributionModel) {
    params.push(input.attributionModel);
    filters.push(`attribution_model = $${params.length}`);
  }

  if (input.platform) {
    params.push(input.platform);
    filters.push(`platform = $${params.length}`);
  }

  if (input.source) {
    params.push(input.source);
    filters.push(`source = $${params.length}`);
  }

  if (input.campaign) {
    params.push(input.campaign);
    filters.push(`campaign = $${params.length}`);
  }

  return {
    sql: filters.join('\n        AND '),
    params
  };
}

function buildTaxonomyDriftFilters(input: TaxonomyDriftQueryInput): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.startDate, input.endDate];
  const filters = ['mart.metric_date BETWEEN $1::date AND $2::date'];

  if (input.martRowType) {
    params.push(input.martRowType);
    filters.push(`mart.mart_row_type = $${params.length}`);
  }

  if (input.attributionModel) {
    params.push(input.attributionModel);
    filters.push(`mart.attribution_model = $${params.length}`);
  }

  if (input.platform) {
    params.push(input.platform);
    filters.push(`mart.platform = $${params.length}`);
  }

  if (input.source) {
    params.push(input.source);
    filters.push(`mart.source = $${params.length}`);
  }

  if (input.campaign) {
    params.push(input.campaign);
    filters.push(`mart.campaign = $${params.length}`);
  }

  return {
    sql: filters.join('\n            AND '),
    params
  };
}

function toIsoString(value: Date | string | null | undefined): string | null {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return typeof value === 'string' && value.length > 0 ? new Date(value).toISOString() : null;
}

function toNumber(value: string | number | null | undefined): number {
  if (value === null || value === undefined) {
    return 0;
  }

  return Number(value);
}

function toNullableRate(numerator: number, denominator: number): number | null {
  return denominator > 0 ? numerator / denominator : null;
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as JsonRecord) : {};
}

function asRecordArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.filter((entry): entry is JsonRecord => entry !== null && typeof entry === 'object' && !Array.isArray(entry)) : [];
}

function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function toNumberOrZero(value: unknown): number {
  return toNullableNumber(value) ?? 0;
}

function getString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function pickIntervalSummary(value: unknown) {
  const summary = asRecord(value);
  return {
    mean: toNullableNumber(summary.mean),
    credibleInterval80: asRecord(summary.credibleInterval80),
    credibleInterval95: asRecord(summary.credibleInterval95)
  };
}

function summarizeBayesianRun(row: MmmModelRunRow) {
  const inputSummary = asRecord(row.input_summary);
  const runConfig = asRecord(row.run_config);

  return {
    runId: row.id,
    modelType: row.model_type,
    modelVersion: row.model_version,
    martVersion: row.mart_version,
    attributionModel: row.attribution_model,
    runStatus: row.run_status,
    trainingStartDate: row.training_start_date,
    trainingEndDate: row.training_end_date,
    holdoutStartDate: row.holdout_start_date,
    holdoutEndDate: row.holdout_end_date,
    completedAt: toIsoString(row.completed_at),
    rowCount: toNullableNumber(inputSummary.rowCount),
    observationCount: toNullableNumber(inputSummary.observationCount),
    trainingObservationCount: toNullableNumber(inputSummary.trainingObservationCount),
    holdoutObservationCount: toNullableNumber(inputSummary.holdoutObservationCount),
    selectedChannels: Array.isArray(inputSummary.selectedChannels) ? inputSummary.selectedChannels : [],
    otherPaidChannelCount: toNullableNumber(inputSummary.otherPaidChannelCount),
    posteriorEngine: getString(runConfig.posteriorEngine),
    responseVariable: getString(runConfig.responseVariable),
    calibrationUse: getString(runConfig.calibrationUse),
    inputContractVersion: getString(inputSummary.inputContractVersion) ?? getString(runConfig.inputContractVersion),
    snapshotVersion: getString(inputSummary.snapshotVersion),
    snapshotRowCount: toNullableNumber(inputSummary.snapshotRowCount),
    snapshotHash: getString(inputSummary.snapshotHash)
  };
}

function mapPosteriorContributionIntervals(modelArtifact: unknown) {
  const contributionOutputs = asRecord(asRecord(modelArtifact).contributionOutputs);
  const channels = asRecordArray(contributionOutputs.channels).map((channel) => ({
    key: getString(channel.key),
    source: getString(channel.source) ?? 'unknown',
    medium: getString(channel.medium) ?? 'unknown',
    campaign: getString(channel.campaign) ?? 'unknown',
    channel: getString(channel.channel),
    channelGroup: getString(channel.channelGroup),
    spend: toNullableNumber(channel.spend),
    impressions: toNullableNumber(channel.impressions),
    clicks: toNullableNumber(channel.clicks),
    attributedOrders: toNullableNumber(channel.attributedOrders),
    attributedRevenue: toNullableNumber(channel.attributedRevenue),
    contribution: pickIntervalSummary(channel.contribution),
    contributionShare: pickIntervalSummary(channel.contributionShare),
    posteriorProbabilityPositive: toNullableNumber(channel.posteriorProbabilityPositive)
  }));

  return {
    totalMediaContribution: pickIntervalSummary(contributionOutputs.totalMediaContribution),
    totalMediaContributionShare: pickIntervalSummary(contributionOutputs.totalMediaContributionShare),
    channels
  };
}

function mapCalibrationDeltas(calibrationReport: unknown) {
  const report = asRecord(calibrationReport);
  const totalDeterministicRevenue = toNumberOrZero(report.totalDeterministicRevenue);
  const totalPosteriorMediaContribution = toNumberOrZero(report.totalPosteriorMediaContribution);
  const segments = asRecordArray(report.segments).map((segment) => {
    const deterministicRevenue = toNumberOrZero(segment.deterministicRevenue);
    const posteriorContributionMean = toNumberOrZero(segment.posteriorContributionMean);

    return {
      key: getString(segment.key),
      source: getString(segment.source) ?? 'unknown',
      medium: getString(segment.medium) ?? 'unknown',
      campaign: getString(segment.campaign) ?? 'unknown',
      channel: getString(segment.channel),
      channelGroup: getString(segment.channelGroup),
      deterministicRevenue,
      posteriorContributionMean,
      deltaRevenue: posteriorContributionMean - deterministicRevenue,
      deltaPct: deterministicRevenue > 0 ? (posteriorContributionMean - deterministicRevenue) / deterministicRevenue : null,
      deterministicContributionShare: toNullableNumber(segment.deterministicContributionShare),
      posteriorContributionShareMean: toNullableNumber(segment.posteriorContributionShareMean),
      productionContributionShare: toNullableNumber(segment.productionContributionShare),
      posteriorProbabilityPositive: toNullableNumber(segment.posteriorProbabilityPositive),
      trustWeights: asRecord(segment.trustWeights)
    };
  });

  return {
    reportVersion: getString(report.reportVersion),
    governanceStatus: getString(report.governanceStatus),
    deterministicAttributionUsage: getString(report.deterministicAttributionUsage),
    deterministicBaseline: asRecord(report.deterministicBaseline),
    totalDeterministicRevenue,
    totalPosteriorMediaContribution,
    deltaRevenue: totalPosteriorMediaContribution - totalDeterministicRevenue,
    deltaPct:
      totalDeterministicRevenue > 0
        ? (totalPosteriorMediaContribution - totalDeterministicRevenue) / totalDeterministicRevenue
        : null,
    divergenceAlerts: report.divergenceAlerts ?? [],
    thresholds: report.thresholds ?? null,
    segments
  };
}

function mapValidationDiagnostics(validationReport: unknown) {
  const report = asRecord(validationReport);
  const posteriorDiagnostics = asRecord(report.posteriorDiagnostics);
  const posteriorSanityChecks = asRecord(report.posteriorSanityChecks);

  return {
    train: asRecord(report.train),
    holdout: asRecord(report.holdout),
    posteriorDiagnostics: {
      maxRhat: toNullableNumber(posteriorDiagnostics.maxRhat),
      minEffectiveSampleSize: toNullableNumber(posteriorDiagnostics.minEffectiveSampleSize),
      totalDraws: toNullableNumber(posteriorDiagnostics.totalDraws),
      byParameter: asRecord(posteriorDiagnostics.byParameter)
    },
    posteriorSanityChecks: {
      status: getString(posteriorSanityChecks.status),
      maxRhat: toNullableNumber(posteriorSanityChecks.maxRhat),
      maxAllowedRhat: toNullableNumber(posteriorSanityChecks.maxAllowedRhat),
      minEffectiveSampleSize: toNullableNumber(posteriorSanityChecks.minEffectiveSampleSize),
      minRequiredEffectiveSampleSize: toNullableNumber(posteriorSanityChecks.minRequiredEffectiveSampleSize)
    }
  };
}

function deriveModelRunReadiness(row: MmmModelRunRow, validationDiagnostics: ReturnType<typeof mapValidationDiagnostics>, calibrationDeltas: ReturnType<typeof mapCalibrationDeltas>) {
  const blockers: string[] = [];
  const warnings: string[] = [];
  const inputSummary = asRecord(row.input_summary);

  if (row.run_status !== 'completed') {
    blockers.push(`run_status_${row.run_status}`);
  }

  if (toNumberOrZero(inputSummary.failCount) > 0) {
    blockers.push('failed_input_rows');
  }

  if (validationDiagnostics.posteriorSanityChecks.status === 'fail') {
    blockers.push('posterior_sanity_checks_failed');
  }

  if (calibrationDeltas.governanceStatus && calibrationDeltas.governanceStatus !== 'passed') {
    warnings.push(`calibration_governance_${calibrationDeltas.governanceStatus}`);
  }

  if (toNumberOrZero(inputSummary.warnCount) > 0) {
    warnings.push('warning_input_rows');
  }

  return {
    status: blockers.length > 0 ? 'not_ready' : warnings.length > 0 ? 'partial' : 'ready',
    blockers,
    warnings,
    completedAt: toIsoString(row.completed_at)
  };
}

function mapMmmRow(row: MmmExportRow) {
  const mapped = {
    date: row.metric_date,
    martVersion: row.mart_version,
    martRowType: row.mart_row_type,
    attributionModel: row.attribution_model,
    platform: row.platform,
    platformConnectionId: row.platform_connection_id === null ? null : Number(row.platform_connection_id),
    granularity: row.granularity,
    entityKey: row.entity_key,
    accountId: row.account_id,
    accountName: row.account_name,
    campaignId: row.campaign_id,
    campaignName: row.campaign_name,
    adsetId: row.adset_id,
    adsetName: row.adset_name,
    adId: row.ad_id,
    adName: row.ad_name,
    creativeId: row.creative_id,
    creativeName: row.creative_name,
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    content: row.content,
    term: row.term,
    currency: row.currency,
    spend: toNumber(row.spend),
    impressions: toNumber(row.impressions),
    clicks: toNumber(row.clicks),
    shopifyOrders: toNumber(row.shopify_orders),
    shopifyRevenue: toNumber(row.shopify_revenue),
    attributionCreditOrders: toNumber(row.attribution_credit_orders),
    attributionCreditRevenue: toNumber(row.attribution_credit_revenue),
    newCustomerCreditOrders: toNumber(row.new_customer_credit_orders),
    returningCustomerCreditOrders: toNumber(row.returning_customer_credit_orders),
    newCustomerCreditRevenue: toNumber(row.new_customer_credit_revenue),
    returningCustomerCreditRevenue: toNumber(row.returning_customer_credit_revenue),
    matchSourceCoverage: row.match_source_coverage,
    confidenceLabelCoverage: row.confidence_label_coverage,
    spendLastSyncedAt: toIsoString(row.spend_last_synced_at),
    shopifyLastIngestedAt: toIsoString(row.shopify_last_ingested_at),
    attributionLastComputedAt: toIsoString(row.attribution_last_computed_at),
    lastComputedAt: toIsoString(row.last_computed_at)
  };

  if (!Object.hasOwn(row, 'resolver_version')) {
    return mapped;
  }

  return {
    ...mapped,
    resolverVersion: row.resolver_version,
    resolverSource: row.resolver_source,
    resolverConfidence: row.resolver_confidence === null ? null : Number(row.resolver_confidence),
    resolvedCanonicalCampaignId: row.resolved_canonical_campaign_id,
    resolvedCanonicalCampaignName: row.resolved_canonical_campaign_name,
    resolvedCanonicalSource: row.resolved_canonical_source,
    resolvedCanonicalMedium: row.resolved_canonical_medium,
    resolvedCanonicalChannel: row.resolved_canonical_channel,
    resolvedCanonicalChannelGroup: row.resolved_canonical_channel_group,
    resolvedHierarchyMetadata: row.resolved_hierarchy_metadata,
    needsMetadataQa: row.needs_metadata_qa
  };
}

function escapeCsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }

  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function renderCsv(rows: ReturnType<typeof mapMmmRow>[], generationTimestamp: string | null, readinessStatus: MmmReadinessStatus): string {
  const headers = [
    'schemaVersion',
    'generationTimestamp',
    'readinessStatus',
    'date',
    'martVersion',
    'martRowType',
    'attributionModel',
    'platform',
    'platformConnectionId',
    'granularity',
    'entityKey',
    'accountId',
    'accountName',
    'campaignId',
    'campaignName',
    'adsetId',
    'adsetName',
    'adId',
    'adName',
    'creativeId',
    'creativeName',
    'source',
    'medium',
    'campaign',
    'content',
    'term',
    'currency',
    'spend',
    'impressions',
    'clicks',
    'shopifyOrders',
    'shopifyRevenue',
    'attributionCreditOrders',
    'attributionCreditRevenue',
    'newCustomerCreditOrders',
    'returningCustomerCreditOrders',
    'newCustomerCreditRevenue',
    'returningCustomerCreditRevenue',
    'matchSourceCoverage',
    'confidenceLabelCoverage',
    'spendLastSyncedAt',
    'shopifyLastIngestedAt',
    'attributionLastComputedAt',
    'lastComputedAt',
    'resolverVersion',
    'resolverSource',
    'resolverConfidence',
    'resolvedCanonicalCampaignId',
    'resolvedCanonicalCampaignName',
    'resolvedCanonicalSource',
    'resolvedCanonicalMedium',
    'resolvedCanonicalChannel',
    'resolvedCanonicalChannelGroup',
    'resolvedHierarchyMetadata',
    'needsMetadataQa'
  ];

  const lines = [headers.join(',')];
  for (const row of rows) {
    const record = {
      schemaVersion: MMM_SCHEMA_VERSION,
      generationTimestamp,
      readinessStatus,
      ...row
    };
    lines.push(headers.map((header) => escapeCsvValue(record[header as keyof typeof record])).join(','));
  }

  return `${lines.join('\n')}\n`;
}

function mapExposureCoverageRow(row: ExposureCoverageRow) {
  const totalExposures = toNumber(row.total_exposures);
  const validExposures = toNumber(row.valid_exposures);
  const identityResolvedExposures = toNumber(row.identity_resolved_exposures);
  const campaignJoinableExposures = toNumber(row.campaign_joinable_exposures);
  const campaignMetadataResolvedExposures = toNumber(row.campaign_metadata_resolved_exposures);

  return {
    date: row.metric_date,
    sourcePlatform: row.source_platform,
    exposureType: row.exposure_type,
    totalExposures,
    validExposures,
    invalidExposures: toNumber(row.invalid_exposures),
    identityResolvedExposures,
    identityUnresolvedExposures: toNumber(row.identity_unresolved_exposures),
    identityResolutionRate: totalExposures > 0 ? identityResolvedExposures / totalExposures : null,
    campaignJoinableExposures,
    campaignMetadataResolvedExposures,
    campaignMetadataResolutionRate: campaignJoinableExposures > 0 ? campaignMetadataResolvedExposures / campaignJoinableExposures : null,
    latestExposureAt: toIsoString(row.latest_exposure_at)
  };
}

function mapTaxonomyDriftSummaryRow(row: TaxonomyDriftSummaryRow) {
  const totalRows = toNumber(row.total_rows);
  const nativeIdEligibleRows = toNumber(row.native_id_eligible_rows);
  const unknownSourceRows = toNumber(row.unknown_source_rows);
  const unmappedSourceRows = toNumber(row.unmapped_source_rows);
  const unknownOrUnmappedSourceRows = toNumber(row.unknown_or_unmapped_source_rows);
  const unknownMediumRows = toNumber(row.unknown_medium_rows);
  const unmappedMediumRows = toNumber(row.unmapped_medium_rows);
  const unknownOrUnmappedMediumRows = toNumber(row.unknown_or_unmapped_medium_rows);
  const unresolvedCampaignMetadataRows = toNumber(row.unresolved_campaign_metadata_rows);
  const staleCampaignMetadataRows = toNumber(row.stale_campaign_metadata_rows);
  const accountIdRows = toNumber(row.account_id_rows);
  const campaignIdRows = toNumber(row.campaign_id_rows);
  const adsetIdRows = toNumber(row.adset_id_rows);
  const adIdRows = toNumber(row.ad_id_rows);
  const creativeIdRows = toNumber(row.creative_id_rows);
  const platformNativeIdRows = toNumber(row.platform_native_id_rows);

  return {
    date: row.metric_date,
    totalRows,
    unknownSourceRows,
    unknownSourceRate: toNullableRate(unknownSourceRows, totalRows),
    unmappedSourceRows,
    unmappedSourceRate: toNullableRate(unmappedSourceRows, totalRows),
    unknownOrUnmappedSourceRows,
    unknownOrUnmappedSourceRate: toNullableRate(unknownOrUnmappedSourceRows, totalRows),
    unknownMediumRows,
    unknownMediumRate: toNullableRate(unknownMediumRows, totalRows),
    unmappedMediumRows,
    unmappedMediumRate: toNullableRate(unmappedMediumRows, totalRows),
    unknownOrUnmappedMediumRows,
    unknownOrUnmappedMediumRate: toNullableRate(unknownOrUnmappedMediumRows, totalRows),
    unresolvedCampaignMetadataRows,
    unresolvedCampaignMetadataRate: toNullableRate(unresolvedCampaignMetadataRows, totalRows),
    staleCampaignMetadataRows,
    staleCampaignMetadataRate: toNullableRate(staleCampaignMetadataRows, totalRows),
    nativeIdEligibleRows,
    nativeIdCoverage: {
      accountIdRows,
      accountIdRate: toNullableRate(accountIdRows, nativeIdEligibleRows),
      campaignIdRows,
      campaignIdRate: toNullableRate(campaignIdRows, nativeIdEligibleRows),
      adsetIdRows,
      adsetIdRate: toNullableRate(adsetIdRows, nativeIdEligibleRows),
      adIdRows,
      adIdRate: toNullableRate(adIdRows, nativeIdEligibleRows),
      creativeIdRows,
      creativeIdRate: toNullableRate(creativeIdRows, nativeIdEligibleRows),
      platformNativeIdRows,
      platformNativeIdRate: toNullableRate(platformNativeIdRows, nativeIdEligibleRows)
    }
  };
}

function mapTaxonomyDriftSampleRow(row: TaxonomyDriftSampleRow) {
  return {
    sampleType: row.sample_type,
    rowCount: toNumber(row.row_count),
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    platform: row.platform,
    martRowType: row.mart_row_type,
    attributionModel: row.attribution_model,
    accountId: row.account_id,
    campaignId: row.campaign_id,
    metadataLastSeenAt: toIsoString(row.metadata_last_seen_at),
    metadataUpdatedAt: toIsoString(row.metadata_updated_at)
  };
}

function deriveReadiness(rows: MmmReadinessRow[]) {
  const excludedDateWindows = rows
    .filter((row) => Number(row.matching_row_count) === 0)
    .map((row) => ({
      startDate: row.metric_date,
      endDate: row.metric_date,
      reason: Number(row.mart_row_count) === 0 ? 'no_mmm_mart_rows' : 'no_rows_matching_filters'
    }));
  const includedDateCount = rows.length - excludedDateWindows.length;
  const status: MmmReadinessStatus =
    excludedDateWindows.length === 0 ? 'ready' : includedDateCount > 0 ? 'partial' : 'not_ready';
  const generationTimestamp = rows.reduce<string | null>((latest, row) => {
    const candidate = toIsoString(row.generation_timestamp);
    if (!candidate) {
      return latest;
    }

    return latest === null || candidate > latest ? candidate : latest;
  }, null);

  return {
    status,
    generationTimestamp,
    includedDateCount,
    excludedDateWindows
  };
}

function mapMmmModelRun(row: MmmModelRunRow) {
  const mapped = {
    id: row.id,
    modelType: row.model_type,
    modelVersion: row.model_version,
    martVersion: row.mart_version,
    attributionModel: row.attribution_model,
    runStatus: row.run_status,
    trainingStartDate: row.training_start_date,
    trainingEndDate: row.training_end_date,
    holdoutStartDate: row.holdout_start_date,
    holdoutEndDate: row.holdout_end_date,
    runConfig: row.run_config ?? {},
    inputSummary: row.input_summary ?? {},
    modelArtifact: row.model_artifact ?? {},
    calibrationReport: row.calibration_report ?? {},
    validationReport: row.validation_report ?? {},
    errorCode: row.error_code,
    errorMessage: row.error_message,
    createdAt: toIsoString(row.created_at),
    startedAt: toIsoString(row.started_at),
    completedAt: toIsoString(row.completed_at)
  };

  if (row.model_type !== 'bayesian_hierarchical_mmm') {
    return mapped;
  }

  const posteriorContributionIntervals = mapPosteriorContributionIntervals(row.model_artifact);
  const calibrationDeltas = mapCalibrationDeltas(row.calibration_report);
  const validationDiagnostics = mapValidationDiagnostics(row.validation_report);

  return {
    ...mapped,
    runSummary: summarizeBayesianRun(row),
    posteriorContributionIntervals,
    calibrationDeltas,
    validationDiagnostics,
    readiness: deriveModelRunReadiness(row, validationDiagnostics, calibrationDeltas)
  };
}

function getActorLabel(auth: AuthContext | null | undefined): string {
  if (auth?.kind === 'user') {
    return auth.user.email;
  }

  return 'internal';
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableJson).join(',')}]`;
  }

  if (value && typeof value === 'object') {
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableJson(entry)}`)
      .join(',')}}`;
  }

  return JSON.stringify(value);
}

function evidenceHash(value: unknown): string {
  return createHash('sha256').update(stableJson(value)).digest('hex');
}

function buildGateFilters(input: z.infer<typeof readinessGateQuerySchema>): MmmQueryInput {
  return {
    ...input,
    format: 'json',
    limit: 1000,
    offset: 0
  };
}

function mapGateRow(row: MmmReadinessGateRow) {
  return {
    id: row.id,
    gateVersion: row.gate_version,
    range: {
      startDate: row.start_date,
      endDate: row.end_date
    },
    filters: {
      martRowType: row.mart_row_type,
      attributionModel: row.attribution_model,
      platform: row.platform,
      source: row.source,
      campaign: row.campaign
    },
    evidencePayload: row.evidence_payload ?? {},
    checklistStatuses: row.checklist_statuses ?? [],
    ownerApprovals: row.owner_approvals ?? [],
    waivers: row.waivers ?? [],
    unresolvedCriticalIssueCount: toNumber(row.unresolved_critical_issue_count),
    evidenceHash: row.evidence_hash,
    gateStatus: row.gate_status,
    finalState: row.final_state,
    decisionReason: row.decision_reason,
    decidedBy: row.decided_by,
    decidedAt: toIsoString(row.decided_at),
    createdBy: row.created_by,
    updatedBy: row.updated_by,
    createdAt: toIsoString(row.created_at),
    updatedAt: toIsoString(row.updated_at)
  };
}

async function fetchPersistedGate(input: z.infer<typeof readinessGateQuerySchema>): Promise<MmmReadinessGateRow | null> {
  const result = await query<MmmReadinessGateRow>(
    `
      SELECT
        id::text,
        gate_version,
        start_date::text,
        end_date::text,
        mart_row_type,
        attribution_model,
        platform,
        source,
        campaign,
        evidence_payload,
        checklist_statuses,
        owner_approvals,
        waivers,
        unresolved_critical_issue_count,
        evidence_hash,
        gate_status,
        final_state,
        decision_reason,
        decided_by,
        decided_at,
        created_by,
        updated_by,
        created_at,
        updated_at
      FROM mmm_readiness_gates
      WHERE start_date = $1::date
        AND end_date = $2::date
        AND COALESCE(mart_row_type, '') = COALESCE($3::text, '')
        AND COALESCE(attribution_model, '') = COALESCE($4::text, '')
        AND COALESCE(platform, '') = COALESCE($5::text, '')
        AND COALESCE(source, '') = COALESCE($6::text, '')
        AND COALESCE(campaign, '') = COALESCE($7::text, '')
      LIMIT 1
    `,
    [
      input.startDate,
      input.endDate,
      input.martRowType ?? null,
      input.attributionModel ?? null,
      input.platform ?? null,
      input.source ?? null,
      input.campaign ?? null
    ]
  );

  return result.rows[0] ?? null;
}

async function buildReadinessEvidence(input: z.infer<typeof readinessGateQuerySchema>) {
  const gateInput = buildGateFilters(input);
  const filters = buildMmmFilters(gateInput);
  const driftFilters = buildTaxonomyDriftFilters({
    ...input,
    staleAfterDays: 14,
    sampleLimit: 5
  });

  const [readinessResult, summaryResult, exposureResult, driftResult, blockerResult, modelRunsResult] = await Promise.all([
    query<MmmReadinessRow>(
      `
        WITH requested_dates AS (
          SELECT generate_series($1::date, $2::date, interval '1 day')::date AS metric_date
        ),
        filtered_rows AS (
          SELECT
            metric_date,
            COUNT(*) AS row_count,
            MAX(last_computed_at) AS generation_timestamp
          FROM mmm_daily_input_mart_v1
          WHERE ${filters.sql}
          GROUP BY metric_date
        ),
        mart_rows AS (
          SELECT
            metric_date,
            COUNT(*) AS row_count
          FROM mmm_daily_input_mart_v1
          WHERE metric_date BETWEEN $1::date AND $2::date
          GROUP BY metric_date
        )
        SELECT
          requested_dates.metric_date::text,
          COALESCE(filtered_rows.row_count, 0) AS matching_row_count,
          COALESCE(mart_rows.row_count, 0) AS mart_row_count,
          filtered_rows.generation_timestamp
        FROM requested_dates
        LEFT JOIN filtered_rows ON filtered_rows.metric_date = requested_dates.metric_date
        LEFT JOIN mart_rows ON mart_rows.metric_date = requested_dates.metric_date
        ORDER BY requested_dates.metric_date ASC
      `,
      filters.params
    ),
    query<{
      total_rows: string | number;
      paid_media_rows: string | number;
      attribution_rows: string | number;
      total_spend: string | number;
      total_impressions: string | number;
      total_clicks: string | number;
      total_shopify_orders: string | number;
      total_shopify_revenue: string | number;
      total_attribution_credit_orders: string | number;
      total_attribution_credit_revenue: string | number;
      latest_spend_last_synced_at: Date | null;
      latest_shopify_last_ingested_at: Date | null;
      latest_attribution_last_computed_at: Date | null;
      latest_last_computed_at: Date | null;
      unresolved_metadata_rows: string | number;
    }>(
      `
        SELECT
          COUNT(*)::bigint AS total_rows,
          COUNT(*) FILTER (WHERE mart_row_type = 'paid_media')::bigint AS paid_media_rows,
          COUNT(*) FILTER (WHERE mart_row_type = 'attribution')::bigint AS attribution_rows,
          COALESCE(SUM(spend), 0) AS total_spend,
          COALESCE(SUM(impressions), 0) AS total_impressions,
          COALESCE(SUM(clicks), 0) AS total_clicks,
          COALESCE(SUM(shopify_orders), 0) AS total_shopify_orders,
          COALESCE(SUM(shopify_revenue), 0) AS total_shopify_revenue,
          COALESCE(SUM(attribution_credit_orders), 0) AS total_attribution_credit_orders,
          COALESCE(SUM(attribution_credit_revenue), 0) AS total_attribution_credit_revenue,
          MAX(spend_last_synced_at) AS latest_spend_last_synced_at,
          MAX(shopify_last_ingested_at) AS latest_shopify_last_ingested_at,
          MAX(attribution_last_computed_at) AS latest_attribution_last_computed_at,
          MAX(last_computed_at) AS latest_last_computed_at,
          COUNT(*) FILTER (WHERE needs_metadata_qa OR resolved_canonical_campaign_name IS NULL)::bigint AS unresolved_metadata_rows
        FROM mmm_daily_input_mart_v1
        WHERE ${filters.sql}
      `,
      filters.params
    ),
    query<{
      total_exposures: string | number;
      valid_exposures: string | number;
      identity_resolved_exposures: string | number;
      campaign_joinable_exposures: string | number;
      campaign_metadata_resolved_exposures: string | number;
      latest_exposure_at: Date | null;
    }>(
      `
        SELECT
          COUNT(*)::bigint AS total_exposures,
          COUNT(*) FILTER (WHERE e.validity_status = 'valid')::bigint AS valid_exposures,
          COUNT(*) FILTER (WHERE e.identity_journey_id IS NOT NULL)::bigint AS identity_resolved_exposures,
          COUNT(*) FILTER (
            WHERE e.validity_status = 'valid'
              AND e.account_id IS NOT NULL
              AND e.campaign_id IS NOT NULL
          )::bigint AS campaign_joinable_exposures,
          COUNT(*) FILTER (WHERE campaign_meta.id IS NOT NULL)::bigint AS campaign_metadata_resolved_exposures,
          MAX(e.occurred_at) AS latest_exposure_at
        FROM ad_exposure_events e
        LEFT JOIN ad_platform_entity_metadata campaign_meta
          ON campaign_meta.platform = e.source_platform
         AND campaign_meta.entity_type = 'campaign'
         AND campaign_meta.account_id = e.account_id
         AND campaign_meta.entity_id = e.campaign_id
         AND COALESCE(campaign_meta.tenant_id, '') = COALESCE(e.tenant_id, '')
         AND COALESCE(campaign_meta.workspace_id, '') = COALESCE(e.workspace_id, '')
        WHERE e.occurred_at >= $1::date
          AND e.occurred_at < ($2::date + interval '1 day')
      `,
      [input.startDate, input.endDate]
    ),
    query<TaxonomyDriftSummaryRow>(
      `
        WITH filtered_mart AS (
          SELECT
            mart.metric_date,
            mart.mart_row_type,
            mart.attribution_model,
            mart.platform,
            mart.source,
            mart.medium,
            mart.campaign,
            mart.account_id,
            mart.campaign_id,
            mart.adset_id,
            mart.ad_id,
            mart.creative_id,
            mart.resolved_canonical_source,
            mart.resolved_canonical_medium,
            mart.resolved_canonical_campaign_name,
            mart.needs_metadata_qa,
            campaign_meta.last_seen_at AS metadata_last_seen_at,
            lower(btrim(COALESCE(mart.source, ''))) AS normalized_source,
            lower(btrim(COALESCE(mart.medium, ''))) AS normalized_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_source, mart.source, ''))) AS normalized_effective_source,
            lower(btrim(COALESCE(mart.resolved_canonical_medium, mart.medium, ''))) AS normalized_effective_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_campaign_name, mart.campaign, ''))) AS normalized_effective_campaign,
            mart.platform IN ('meta', 'google') AS native_id_eligible,
            campaign_meta.id IS NOT NULL
              AND campaign_meta.last_seen_at < ($2::date - (14::int * interval '1 day')) AS stale_campaign_metadata
          FROM mmm_daily_input_mart_v1 mart
          LEFT JOIN ad_platform_entity_metadata campaign_meta
            ON campaign_meta.platform = CASE
                WHEN mart.platform = 'meta' THEN 'meta_ads'
                WHEN mart.platform = 'google' THEN 'google_ads'
                ELSE NULL
              END
           AND campaign_meta.entity_type = 'campaign'
           AND campaign_meta.account_id = mart.account_id
           AND campaign_meta.entity_id = mart.campaign_id
          WHERE ${driftFilters.sql}
        ),
        classified_mart AS (
          SELECT
            *,
            normalized_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_source,
            resolved_canonical_source IS NULL AS has_unmapped_source,
            normalized_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_medium,
            resolved_canonical_medium IS NULL AS has_unmapped_medium,
            needs_metadata_qa
              OR resolved_canonical_campaign_name IS NULL
              OR normalized_effective_campaign IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unresolved_campaign_metadata,
            account_id IS NOT NULL AND campaign_id IS NOT NULL AS has_platform_native_campaign_key
          FROM filtered_mart
        )
        SELECT
          NULL::text AS metric_date,
          COUNT(*)::bigint AS total_rows,
          COUNT(*) FILTER (WHERE has_unknown_source)::bigint AS unknown_source_rows,
          COUNT(*) FILTER (WHERE has_unmapped_source)::bigint AS unmapped_source_rows,
          COUNT(*) FILTER (WHERE has_unknown_source OR has_unmapped_source)::bigint AS unknown_or_unmapped_source_rows,
          COUNT(*) FILTER (WHERE has_unknown_medium)::bigint AS unknown_medium_rows,
          COUNT(*) FILTER (WHERE has_unmapped_medium)::bigint AS unmapped_medium_rows,
          COUNT(*) FILTER (WHERE has_unknown_medium OR has_unmapped_medium)::bigint AS unknown_or_unmapped_medium_rows,
          COUNT(*) FILTER (WHERE has_unresolved_campaign_metadata)::bigint AS unresolved_campaign_metadata_rows,
          COUNT(*) FILTER (WHERE stale_campaign_metadata)::bigint AS stale_campaign_metadata_rows,
          COUNT(*) FILTER (WHERE native_id_eligible)::bigint AS native_id_eligible_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND account_id IS NOT NULL)::bigint AS account_id_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND campaign_id IS NOT NULL)::bigint AS campaign_id_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND adset_id IS NOT NULL)::bigint AS adset_id_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND ad_id IS NOT NULL)::bigint AS ad_id_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND creative_id IS NOT NULL)::bigint AS creative_id_rows,
          COUNT(*) FILTER (WHERE native_id_eligible AND has_platform_native_campaign_key)::bigint AS platform_native_id_rows
        FROM classified_mart
      `,
      driftFilters.params
    ),
    query<DataQualityBlockerRow>(
      `
        SELECT DISTINCT ON (check_key)
          check_key,
          status,
          severity,
          discrepancy_count,
          summary,
          checked_at
        FROM data_quality_check_runs
        WHERE run_date BETWEEN $1::date AND $2::date
          AND (
            check_key LIKE 'mmm_readiness_%'
            OR severity = 'critical'
          )
          AND status <> 'healthy'
        ORDER BY check_key, checked_at DESC
      `,
      [input.startDate, input.endDate]
    ),
    query<MmmModelRunRow>(
      `
        SELECT
          id::text,
          model_type,
          model_version,
          mart_version,
          attribution_model,
          run_status,
          training_start_date::text,
          training_end_date::text,
          holdout_start_date::text,
          holdout_end_date::text,
          run_config,
          input_summary,
          model_artifact,
          calibration_report,
          validation_report,
          error_code,
          error_message,
          created_at,
          started_at,
          completed_at
        FROM mmm_model_runs
        WHERE training_end_date >= $1::date
          AND training_start_date <= $2::date
          ${input.attributionModel ? 'AND attribution_model = $3' : ''}
        ORDER BY created_at DESC
        LIMIT 1
      `,
      input.attributionModel ? [input.startDate, input.endDate, input.attributionModel] : [input.startDate, input.endDate]
    )
  ]);

  const readiness = deriveReadiness(readinessResult.rows);
  const summary = summaryResult.rows[0];
  const exposure = exposureResult.rows[0];
  const drift = driftResult.rows[0] ? mapTaxonomyDriftSummaryRow(driftResult.rows[0]) : null;
  const latestModelRun = modelRunsResult.rows[0] ? mapMmmModelRun(modelRunsResult.rows[0]) : null;
  const exposureTotals = {
    totalExposures: toNumber(exposure?.total_exposures),
    validExposures: toNumber(exposure?.valid_exposures),
    identityResolvedExposures: toNumber(exposure?.identity_resolved_exposures),
    campaignJoinableExposures: toNumber(exposure?.campaign_joinable_exposures),
    campaignMetadataResolvedExposures: toNumber(exposure?.campaign_metadata_resolved_exposures),
    identityResolutionRate: toNullableRate(toNumber(exposure?.identity_resolved_exposures), toNumber(exposure?.total_exposures)),
    campaignMetadataResolutionRate: toNullableRate(
      toNumber(exposure?.campaign_metadata_resolved_exposures),
      toNumber(exposure?.campaign_joinable_exposures)
    ),
    latestExposureAt: toIsoString(exposure?.latest_exposure_at)
  };
  const dataQualityBlockers = blockerResult.rows.map((row) => ({
    checkKey: row.check_key,
    status: row.status,
    severity: row.severity,
    discrepancyCount: toNumber(row.discrepancy_count),
    summary: row.summary,
    checkedAt: toIsoString(row.checked_at)
  }));
  const criticalDataQualityCount = dataQualityBlockers.filter(
    (row) => row.severity === 'critical' || row.checkKey.startsWith('mmm_readiness_')
  ).length;

  const evidencePayload = {
    schemaVersion: 'mmm_readiness_gate_evidence_v1',
    range: {
      startDate: input.startDate,
      endDate: input.endDate
    },
    filters: {
      martRowType: input.martRowType ?? null,
      attributionModel: input.attributionModel ?? null,
      platform: input.platform ?? null,
      source: input.source ?? null,
      campaign: input.campaign ?? null
    },
    exportReadiness: readiness,
    exportSummary: {
      totalRows: toNumber(summary?.total_rows),
      paidMediaRows: toNumber(summary?.paid_media_rows),
      attributionRows: toNumber(summary?.attribution_rows),
      totalSpend: toNumber(summary?.total_spend),
      totalImpressions: toNumber(summary?.total_impressions),
      totalClicks: toNumber(summary?.total_clicks),
      totalShopifyOrders: toNumber(summary?.total_shopify_orders),
      totalShopifyRevenue: toNumber(summary?.total_shopify_revenue),
      totalAttributionCreditOrders: toNumber(summary?.total_attribution_credit_orders),
      totalAttributionCreditRevenue: toNumber(summary?.total_attribution_credit_revenue),
      latestSpendLastSyncedAt: toIsoString(summary?.latest_spend_last_synced_at),
      latestShopifyLastIngestedAt: toIsoString(summary?.latest_shopify_last_ingested_at),
      latestAttributionLastComputedAt: toIsoString(summary?.latest_attribution_last_computed_at),
      latestLastComputedAt: toIsoString(summary?.latest_last_computed_at),
      unresolvedMetadataRows: toNumber(summary?.unresolved_metadata_rows)
    },
    exposureCoverage: exposureTotals,
    taxonomyDrift: drift,
    dataQualityBlockers,
    latestModelRun
  };

  const checklistStatuses = [
    {
      key: 'mmm_export_readiness',
      label: 'MMM export window coverage',
      owner: 'Product',
      status: readiness.status === 'ready' ? 'pass' : readiness.status === 'partial' ? 'warn' : 'fail',
      detail: `${readiness.includedDateCount} included days, ${readiness.excludedDateWindows.length} excluded windows.`
    },
    {
      key: 'mmm_input_rows',
      label: 'MMM input rows',
      owner: 'Analytics',
      status: toNumber(summary?.paid_media_rows) > 0 && toNumber(summary?.attribution_rows) > 0 ? 'pass' : 'fail',
      detail: `${toNumber(summary?.paid_media_rows)} paid media rows and ${toNumber(summary?.attribution_rows)} attribution rows.`
    },
    {
      key: 'campaign_resolver_coverage',
      label: 'Campaign resolver coverage',
      owner: 'Analytics',
      status: toNumber(summary?.unresolved_metadata_rows) === 0 ? 'pass' : 'fail',
      detail: `${toNumber(summary?.unresolved_metadata_rows)} rows still need metadata QA.`
    },
    {
      key: 'exposure_coverage',
      label: 'Exposure coverage',
      owner: 'Frontend',
      status:
        exposureTotals.totalExposures === 0
          ? 'warn'
          : (exposureTotals.identityResolutionRate ?? 0) >= 0.8 &&
              (exposureTotals.campaignMetadataResolutionRate ?? 0) >= 0.8
            ? 'pass'
            : 'fail',
      detail: `${exposureTotals.totalExposures} exposures, identity ${exposureTotals.identityResolutionRate ?? 0}, metadata ${
        exposureTotals.campaignMetadataResolutionRate ?? 0
      }.`
    },
    {
      key: 'taxonomy_drift',
      label: 'Taxonomy drift',
      owner: 'Analytics',
      status: drift && drift.unresolvedCampaignMetadataRows === 0 && drift.unknownOrUnmappedSourceRows === 0 ? 'pass' : 'fail',
      detail: `${drift?.unresolvedCampaignMetadataRows ?? 0} unresolved campaign metadata rows, ${
        drift?.unknownOrUnmappedSourceRows ?? 0
      } unknown or unmapped source rows.`
    },
    {
      key: 'data_quality_blockers',
      label: 'Data-quality blockers',
      owner: 'Data Platform',
      status: criticalDataQualityCount === 0 ? 'pass' : 'fail',
      detail: `${criticalDataQualityCount} unresolved critical data-quality checks.`
    },
    {
      key: 'model_run_governance',
      label: 'Baseline model governance',
      owner: 'Modeling',
      status: latestModelRun?.runStatus === 'completed' ? 'pass' : 'pending',
      detail: latestModelRun ? `${latestModelRun.modelVersion} ${latestModelRun.runStatus}.` : 'No model run overlaps this window.'
    }
  ] satisfies Array<{
    key: string;
    label: string;
    owner: string;
    status: GateChecklistStatus;
    detail: string;
  }>;
  const ownerApprovals = ['Product', 'Analytics', 'Frontend', 'Data Platform', 'Modeling'].map((owner) => {
    const ownerItems = checklistStatuses.filter((item) => item.owner === owner);
    const failedCount = ownerItems.filter((item) => item.status === 'fail').length;
    const pendingCount = ownerItems.filter((item) => item.status === 'pending').length;

    return {
      owner,
      status: failedCount > 0 ? 'pending' : pendingCount > 0 ? 'pending' : 'pass',
      approvedBy: null,
      approvedAt: null,
      detail:
        failedCount > 0
          ? `${failedCount} gate ${failedCount === 1 ? 'item requires' : 'items require'} approval or waiver.`
          : pendingCount > 0
            ? `${pendingCount} gate ${pendingCount === 1 ? 'item is' : 'items are'} pending.`
            : 'Owner evidence is ready for sign-off.'
    };
  });

  return {
    evidencePayload,
    checklistStatuses,
    ownerApprovals,
    unresolvedCriticalIssueCount:
      checklistStatuses.filter((item) => item.status === 'fail' && item.key !== 'data_quality_blockers').length +
      criticalDataQualityCount,
    evidenceHash: evidenceHash(evidencePayload)
  };
}

async function upsertReadinessGate(input: z.infer<typeof readinessGateQuerySchema>, actor: string) {
  const evidence = await buildReadinessEvidence(input);
  const finalState = evidence.unresolvedCriticalIssueCount === 0 ? 'approved' : 'blocked';
  const gateStatus = finalState === 'approved' ? 'approved' : 'pending';
  const result = await query<MmmReadinessGateRow>(
    `
      INSERT INTO mmm_readiness_gates (
        start_date,
        end_date,
        mart_row_type,
        attribution_model,
        platform,
        source,
        campaign,
        evidence_payload,
        checklist_statuses,
        owner_approvals,
        unresolved_critical_issue_count,
        evidence_hash,
        gate_status,
        final_state,
        decision_reason,
        decided_by,
        decided_at,
        created_by,
        updated_by
      )
      VALUES (
        $1::date,
        $2::date,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb,
        $9::jsonb,
        $10::jsonb,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        CASE WHEN $13 = 'approved' THEN now() ELSE NULL END,
        $17,
        $17
      )
      ON CONFLICT (
        start_date,
        end_date,
        COALESCE(mart_row_type, ''),
        COALESCE(attribution_model, ''),
        COALESCE(platform, ''),
        COALESCE(source, ''),
        COALESCE(campaign, '')
      )
      DO UPDATE SET
        evidence_payload = EXCLUDED.evidence_payload,
        checklist_statuses = EXCLUDED.checklist_statuses,
        owner_approvals = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.owner_approvals
          ELSE EXCLUDED.owner_approvals
        END,
        waivers = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.waivers
          ELSE '[]'::jsonb
        END,
        unresolved_critical_issue_count = EXCLUDED.unresolved_critical_issue_count,
        evidence_hash = EXCLUDED.evidence_hash,
        gate_status = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.gate_status
          ELSE EXCLUDED.gate_status
        END,
        final_state = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.final_state
          ELSE EXCLUDED.final_state
        END,
        decision_reason = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.decision_reason
          ELSE EXCLUDED.decision_reason
        END,
        decided_by = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.decided_by
          ELSE EXCLUDED.decided_by
        END,
        decided_at = CASE
          WHEN mmm_readiness_gates.evidence_hash = EXCLUDED.evidence_hash THEN mmm_readiness_gates.decided_at
          ELSE EXCLUDED.decided_at
        END,
        updated_by = EXCLUDED.updated_by,
        updated_at = now()
      RETURNING
        id::text,
        gate_version,
        start_date::text,
        end_date::text,
        mart_row_type,
        attribution_model,
        platform,
        source,
        campaign,
        evidence_payload,
        checklist_statuses,
        owner_approvals,
        waivers,
        unresolved_critical_issue_count,
        evidence_hash,
        gate_status,
        final_state,
        decision_reason,
        decided_by,
        decided_at,
        created_by,
        updated_by,
        created_at,
        updated_at
    `,
    [
      input.startDate,
      input.endDate,
      input.martRowType ?? null,
      input.attributionModel ?? null,
      input.platform ?? null,
      input.source ?? null,
      input.campaign ?? null,
      JSON.stringify(evidence.evidencePayload),
      JSON.stringify(evidence.checklistStatuses),
      JSON.stringify(evidence.ownerApprovals),
      evidence.unresolvedCriticalIssueCount,
      evidence.evidenceHash,
      gateStatus,
      finalState,
      gateStatus === 'approved' ? 'All readiness gates passed during evidence refresh.' : null,
      gateStatus === 'approved' ? actor : null,
      actor
    ]
  );

  return result.rows[0];
}

export function createMmmRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);
  router.use((_req, res, next) => {
    res.setHeader('X-ROAS-Radar-MMM-Schema', MMM_SCHEMA_VERSION);
    next();
  });

  router.get('/readiness-gate', async (req, res, next) => {
    try {
      const input = parseInput(readinessGateQuerySchema, req.query);
      const existing = await fetchPersistedGate(input);

      if (existing) {
        res.status(200).json({
          schemaVersion: 'mmm_readiness_gate_v1',
          gate: mapGateRow(existing)
        });
        return;
      }

      const actor = getActorLabel(res.locals.auth as AuthContext | null | undefined);
      const gate = await upsertReadinessGate(input, actor);
      res.status(201).json({
        schemaVersion: 'mmm_readiness_gate_v1',
        gate: mapGateRow(gate)
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/readiness-gate/refresh', async (req, res, next) => {
    try {
      const input = parseInput(readinessGateQuerySchema, req.body);
      const actor = getActorLabel(res.locals.auth as AuthContext | null | undefined);
      const gate = await upsertReadinessGate(input, actor);

      res.status(200).json({
        schemaVersion: 'mmm_readiness_gate_v1',
        gate: mapGateRow(gate)
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/readiness-gate/approve', async (req, res, next) => {
    try {
      const input = parseInput(readinessGateDecisionSchema, req.body);
      const actor = getActorLabel(res.locals.auth as AuthContext | null | undefined);
      const gate = (await fetchPersistedGate(input)) ?? (await upsertReadinessGate(input, actor));
      const mapped = mapGateRow(gate);
      const owner = input.owner ?? actor;
      const ownerApprovals = Array.isArray(mapped.ownerApprovals) ? [...mapped.ownerApprovals] : [];
      const ownerIndex = ownerApprovals.findIndex(
        (approval) =>
          approval &&
          typeof approval === 'object' &&
          'owner' in approval &&
          (approval as { owner?: unknown }).owner === owner
      );
      const approvalRecord = {
        owner,
        status: 'pass',
        approvedBy: actor,
        approvedAt: new Date().toISOString(),
        detail: input.reason ?? 'Owner approved the persisted MMM readiness evidence.'
      };

      if (ownerIndex >= 0) {
        ownerApprovals[ownerIndex] = approvalRecord;
      } else {
        ownerApprovals.push(approvalRecord);
      }

      const checklistStatuses = Array.isArray(mapped.checklistStatuses) ? mapped.checklistStatuses : [];
      const waivers = Array.isArray(mapped.waivers) ? mapped.waivers : [];
      const waivedKeys = new Set(
        waivers
          .filter((waiver) => waiver && typeof waiver === 'object' && 'checklistKey' in waiver)
          .map((waiver) => (waiver as { checklistKey?: unknown }).checklistKey)
          .filter((key): key is string => typeof key === 'string')
      );
      const unresolvedFailures = checklistStatuses.filter(
        (item) =>
          item &&
          typeof item === 'object' &&
          (item as { status?: unknown }).status === 'fail' &&
          !waivedKeys.has(String((item as { key?: unknown }).key))
      ).length;
      const finalState = unresolvedFailures === 0 ? 'approved' : 'blocked';
      const gateStatus = finalState === 'approved' ? 'approved' : 'pending';
      const result = await query<MmmReadinessGateRow>(
        `
          UPDATE mmm_readiness_gates
          SET
            owner_approvals = $2::jsonb,
            gate_status = $3,
            final_state = $4,
            decision_reason = $5,
            decided_by = CASE WHEN $3 = 'approved' THEN $6 ELSE decided_by END,
            decided_at = CASE WHEN $3 = 'approved' THEN now() ELSE decided_at END,
            updated_by = $6,
            updated_at = now()
          WHERE id = $1::uuid
          RETURNING
            id::text,
            gate_version,
            start_date::text,
            end_date::text,
            mart_row_type,
            attribution_model,
            platform,
            source,
            campaign,
            evidence_payload,
            checklist_statuses,
            owner_approvals,
            waivers,
            unresolved_critical_issue_count,
            evidence_hash,
            gate_status,
            final_state,
            decision_reason,
            decided_by,
            decided_at,
            created_by,
            updated_by,
            created_at,
            updated_at
        `,
        [
          mapped.id,
          JSON.stringify(ownerApprovals),
          gateStatus,
          finalState,
          input.reason ?? (finalState === 'approved' ? 'Owner approvals completed.' : 'Approval recorded with unresolved gate failures.'),
          actor
        ]
      );

      res.status(200).json({
        schemaVersion: 'mmm_readiness_gate_v1',
        gate: mapGateRow(result.rows[0])
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/readiness-gate/waive', async (req, res, next) => {
    try {
      const input = parseInput(readinessGateWaiverSchema, req.body);
      const actor = getActorLabel(res.locals.auth as AuthContext | null | undefined);
      const gate = (await fetchPersistedGate(input)) ?? (await upsertReadinessGate(input, actor));
      const mapped = mapGateRow(gate);
      const waivers = Array.isArray(mapped.waivers) ? [...mapped.waivers] : [];
      waivers.push({
        ...input.waiver,
        waivedBy: actor,
        waivedAt: new Date().toISOString(),
        evidenceHash: mapped.evidenceHash
      });
      const checklistStatuses = (Array.isArray(mapped.checklistStatuses) ? mapped.checklistStatuses : []).map((item) => {
        if (
          item &&
          typeof item === 'object' &&
          (item as { key?: unknown }).key === input.waiver.checklistKey &&
          (item as { status?: unknown }).status === 'fail'
        ) {
          return {
            ...item,
            status: 'waived',
            waiverReason: input.waiver.reason
          };
        }

        return item;
      });
      const unresolvedFailures = checklistStatuses.filter(
        (item) => item && typeof item === 'object' && (item as { status?: unknown }).status === 'fail'
      ).length;
      const result = await query<MmmReadinessGateRow>(
        `
          UPDATE mmm_readiness_gates
          SET
            waivers = $2::jsonb,
            checklist_statuses = $3::jsonb,
            unresolved_critical_issue_count = $4,
            final_state = CASE WHEN $4 = 0 AND gate_status = 'approved' THEN 'approved' ELSE 'blocked' END,
            updated_by = $5,
            updated_at = now()
          WHERE id = $1::uuid
          RETURNING
            id::text,
            gate_version,
            start_date::text,
            end_date::text,
            mart_row_type,
            attribution_model,
            platform,
            source,
            campaign,
            evidence_payload,
            checklist_statuses,
            owner_approvals,
            waivers,
            unresolved_critical_issue_count,
            evidence_hash,
            gate_status,
            final_state,
            decision_reason,
            decided_by,
            decided_at,
            created_by,
            updated_by,
            created_at,
            updated_at
        `,
        [mapped.id, JSON.stringify(waivers), JSON.stringify(checklistStatuses), unresolvedFailures, actor]
      );

      res.status(200).json({
        schemaVersion: 'mmm_readiness_gate_v1',
        gate: mapGateRow(result.rows[0])
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/readiness-gate/block', async (req, res, next) => {
    try {
      const input = parseInput(readinessGateDecisionSchema, req.body);
      const actor = getActorLabel(res.locals.auth as AuthContext | null | undefined);
      const gate = (await fetchPersistedGate(input)) ?? (await upsertReadinessGate(input, actor));
      const mapped = mapGateRow(gate);
      const result = await query<MmmReadinessGateRow>(
        `
          UPDATE mmm_readiness_gates
          SET
            gate_status = 'blocked',
            final_state = 'blocked',
            decision_reason = $2,
            decided_by = $3,
            decided_at = now(),
            updated_by = $3,
            updated_at = now()
          WHERE id = $1::uuid
          RETURNING
            id::text,
            gate_version,
            start_date::text,
            end_date::text,
            mart_row_type,
            attribution_model,
            platform,
            source,
            campaign,
            evidence_payload,
            checklist_statuses,
            owner_approvals,
            waivers,
            unresolved_critical_issue_count,
            evidence_hash,
            gate_status,
            final_state,
            decision_reason,
            decided_by,
            decided_at,
            created_by,
            updated_by,
            created_at,
            updated_at
        `,
        [mapped.id, input.reason ?? 'Readiness gate blocked by owner review.', actor]
      );

      res.status(200).json({
        schemaVersion: 'mmm_readiness_gate_v1',
        gate: mapGateRow(result.rows[0])
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/model-runs', async (req, res, next) => {
    try {
      const input = parseInput(modelRunsQuerySchema, req.query);
      const params: unknown[] = [];
      const filters: string[] = [];

      if (input.startDate) {
        params.push(input.startDate);
        filters.push(`training_end_date >= $${params.length}::date`);
      }

      if (input.endDate) {
        params.push(input.endDate);
        filters.push(`training_start_date <= $${params.length}::date`);
      }

      if (input.attributionModel) {
        params.push(input.attributionModel);
        filters.push(`attribution_model = $${params.length}`);
      }

      params.push(input.limit);
      const result = await query<MmmModelRunRow>(
        `
          SELECT
            id::text,
            model_type,
            model_version,
            mart_version,
            attribution_model,
            run_status,
            training_start_date::text,
            training_end_date::text,
            holdout_start_date::text,
            holdout_end_date::text,
            run_config,
            input_summary,
            model_artifact,
            calibration_report,
            validation_report,
            error_code,
            error_message,
            created_at,
            started_at,
            completed_at
          FROM mmm_model_runs
          ${filters.length > 0 ? `WHERE ${filters.join('\n            AND ')}` : ''}
          ORDER BY created_at DESC
          LIMIT $${params.length}
        `,
        params
      );

      res.status(200).json({
        schemaVersion: 'mmm_model_runs_v1',
        rows: result.rows.map(mapMmmModelRun)
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/campaign-resolver/resolve', async (req, res, next) => {
    try {
      const input = parseInput(campaignResolverRequestSchema, req.body);
      const resolution = await resolveCampaignMetadata(input);
      res.status(200).json({
        schemaVersion: 'campaign_metadata_resolver_v1',
        resolution
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/campaign-resolver/backfill', async (req, res, next) => {
    try {
      const input = parseInput(campaignResolverBackfillSchema, req.body);
      const report = await backfillMmmCampaignMetadata(input);
      res.status(202).json({
        schemaVersion: 'campaign_metadata_resolver_v1',
        report
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/taxonomy-drift', async (req, res, next) => {
    try {
      const input = parseInput(taxonomyDriftQuerySchema, req.query);
      const filters = buildTaxonomyDriftFilters(input);
      const staleAfterDaysParam = filters.params.length + 1;
      const sampleLimitParam = filters.params.length + 2;
      const driftCte = `
        WITH filtered_mart AS (
          SELECT
            mart.metric_date,
            mart.mart_row_type,
            mart.attribution_model,
            mart.platform,
            mart.source,
            mart.medium,
            mart.campaign,
            mart.account_id,
            mart.campaign_id,
            mart.adset_id,
            mart.ad_id,
            mart.creative_id,
            mart.resolved_canonical_source,
            mart.resolved_canonical_medium,
            mart.resolved_canonical_campaign_name,
            mart.needs_metadata_qa,
            campaign_meta.last_seen_at AS metadata_last_seen_at,
            campaign_meta.updated_at AS metadata_updated_at,
            lower(btrim(COALESCE(mart.source, ''))) AS normalized_source,
            lower(btrim(COALESCE(mart.medium, ''))) AS normalized_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_source, mart.source, ''))) AS normalized_effective_source,
            lower(btrim(COALESCE(mart.resolved_canonical_medium, mart.medium, ''))) AS normalized_effective_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_campaign_name, mart.campaign, ''))) AS normalized_effective_campaign,
            mart.platform IN ('meta', 'google') AS native_id_eligible,
            campaign_meta.id IS NOT NULL
              AND campaign_meta.last_seen_at < ($2::date - ($${staleAfterDaysParam}::int * interval '1 day')) AS stale_campaign_metadata
          FROM mmm_daily_input_mart_v1 mart
          LEFT JOIN ad_platform_entity_metadata campaign_meta
            ON campaign_meta.platform = CASE
                WHEN mart.platform = 'meta' THEN 'meta_ads'
                WHEN mart.platform = 'google' THEN 'google_ads'
                ELSE NULL
              END
           AND campaign_meta.entity_type = 'campaign'
           AND campaign_meta.account_id = mart.account_id
           AND campaign_meta.entity_id = mart.campaign_id
          WHERE ${filters.sql}
        ),
        classified_mart AS (
          SELECT
            *,
            normalized_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_source,
            resolved_canonical_source IS NULL AS has_unmapped_source,
            normalized_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_medium,
            resolved_canonical_medium IS NULL AS has_unmapped_medium,
            needs_metadata_qa
              OR resolved_canonical_campaign_name IS NULL
              OR normalized_effective_campaign IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unresolved_campaign_metadata,
            account_id IS NOT NULL AND campaign_id IS NOT NULL AS has_platform_native_campaign_key
          FROM filtered_mart
        )
      `;

      const summaryResult = await query<TaxonomyDriftSummaryRow>(
        `
          ${driftCte},
          daily_summary AS (
            SELECT
              metric_date::text,
              COUNT(*)::bigint AS total_rows,
              COUNT(*) FILTER (WHERE has_unknown_source)::bigint AS unknown_source_rows,
              COUNT(*) FILTER (WHERE has_unmapped_source)::bigint AS unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_source OR has_unmapped_source)::bigint AS unknown_or_unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium)::bigint AS unknown_medium_rows,
              COUNT(*) FILTER (WHERE has_unmapped_medium)::bigint AS unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium OR has_unmapped_medium)::bigint AS unknown_or_unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unresolved_campaign_metadata)::bigint AS unresolved_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE stale_campaign_metadata)::bigint AS stale_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE native_id_eligible)::bigint AS native_id_eligible_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND account_id IS NOT NULL)::bigint AS account_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND campaign_id IS NOT NULL)::bigint AS campaign_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND adset_id IS NOT NULL)::bigint AS adset_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND ad_id IS NOT NULL)::bigint AS ad_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND creative_id IS NOT NULL)::bigint AS creative_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND has_platform_native_campaign_key)::bigint AS platform_native_id_rows
            FROM classified_mart
            GROUP BY metric_date
          ),
          overall_summary AS (
            SELECT
              NULL::text AS metric_date,
              COUNT(*)::bigint AS total_rows,
              COUNT(*) FILTER (WHERE has_unknown_source)::bigint AS unknown_source_rows,
              COUNT(*) FILTER (WHERE has_unmapped_source)::bigint AS unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_source OR has_unmapped_source)::bigint AS unknown_or_unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium)::bigint AS unknown_medium_rows,
              COUNT(*) FILTER (WHERE has_unmapped_medium)::bigint AS unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium OR has_unmapped_medium)::bigint AS unknown_or_unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unresolved_campaign_metadata)::bigint AS unresolved_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE stale_campaign_metadata)::bigint AS stale_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE native_id_eligible)::bigint AS native_id_eligible_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND account_id IS NOT NULL)::bigint AS account_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND campaign_id IS NOT NULL)::bigint AS campaign_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND adset_id IS NOT NULL)::bigint AS adset_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND ad_id IS NOT NULL)::bigint AS ad_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND creative_id IS NOT NULL)::bigint AS creative_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND has_platform_native_campaign_key)::bigint AS platform_native_id_rows
            FROM classified_mart
          )
          SELECT * FROM overall_summary
          UNION ALL
          SELECT * FROM daily_summary
          ORDER BY metric_date NULLS FIRST
        `,
        [...filters.params, input.staleAfterDays]
      );

      const samplesResult = await query<TaxonomyDriftSampleRow>(
        `
          ${driftCte},
          sample_candidates AS (
            SELECT
              'unknown_or_unmapped_source'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unknown_source OR has_unmapped_source
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'unknown_or_unmapped_medium'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unknown_medium OR has_unmapped_medium
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'unresolved_campaign_metadata'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unresolved_campaign_metadata
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'stale_campaign_metadata'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE stale_campaign_metadata
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'missing_platform_native_campaign_key'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE native_id_eligible AND NOT has_platform_native_campaign_key
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id
          ),
          ranked_samples AS (
            SELECT
              *,
              row_number() OVER (
                PARTITION BY sample_type
                ORDER BY row_count DESC, source ASC, medium ASC, campaign ASC, platform ASC
              ) AS sample_rank
            FROM sample_candidates
          )
          SELECT
            sample_type,
            row_count,
            source,
            medium,
            campaign,
            platform,
            mart_row_type,
            attribution_model,
            account_id,
            campaign_id,
            metadata_last_seen_at,
            metadata_updated_at
          FROM ranked_samples
          WHERE sample_rank <= $${sampleLimitParam}
          ORDER BY sample_type ASC, row_count DESC, source ASC, medium ASC, campaign ASC
        `,
        [...filters.params, input.staleAfterDays, input.sampleLimit]
      );

      const [overallRow, ...dailyRows] = summaryResult.rows.map(mapTaxonomyDriftSummaryRow);

      res.status(200).json({
        schemaVersion: 'mmm_taxonomy_drift_report_v1',
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          martRowType: input.martRowType ?? null,
          attributionModel: input.attributionModel ?? null,
          platform: input.platform ?? null,
          source: input.source ?? null,
          campaign: input.campaign ?? null,
          staleAfterDays: input.staleAfterDays,
          sampleLimit: input.sampleLimit
        },
        overall: overallRow,
        daily: dailyRows,
        samples: samplesResult.rows.map(mapTaxonomyDriftSampleRow)
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/exposure-coverage', async (req, res, next) => {
    try {
      const input = parseInput(
        z
          .object({
            startDate: dateStringSchema,
            endDate: dateStringSchema,
            sourcePlatform: z
              .enum(['meta_ads', 'google_ads', 'tiktok_ads', 'pinterest_ads', 'snapchat_ads', 'unknown'])
              .optional(),
            exposureType: z.enum(['impression', 'view']).optional()
          })
          .superRefine((value, ctx) => {
            if (value.startDate > value.endDate) {
              ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'startDate must be on or before endDate',
                path: ['startDate']
              });
            }
          }),
        req.query
      );
      const params: unknown[] = [input.startDate, input.endDate];
      const filters = ['e.occurred_at >= $1::date', "e.occurred_at < ($2::date + interval '1 day')"];

      if (input.sourcePlatform) {
        params.push(input.sourcePlatform);
        filters.push(`e.source_platform = $${params.length}`);
      }

      if (input.exposureType) {
        params.push(input.exposureType);
        filters.push(`e.exposure_type = $${params.length}`);
      }

      const result = await query<ExposureCoverageRow>(
        `
          SELECT
            e.occurred_at::date::text AS metric_date,
            e.source_platform,
            e.exposure_type,
            COUNT(*)::bigint AS total_exposures,
            COUNT(*) FILTER (WHERE e.validity_status = 'valid')::bigint AS valid_exposures,
            COUNT(*) FILTER (WHERE e.validity_status = 'invalid')::bigint AS invalid_exposures,
            COUNT(*) FILTER (WHERE e.identity_journey_id IS NOT NULL)::bigint AS identity_resolved_exposures,
            COUNT(*) FILTER (
              WHERE e.validity_status = 'valid'
                AND e.identity_journey_id IS NULL
            )::bigint AS identity_unresolved_exposures,
            COUNT(*) FILTER (
              WHERE e.validity_status = 'valid'
                AND e.account_id IS NOT NULL
                AND e.campaign_id IS NOT NULL
            )::bigint AS campaign_joinable_exposures,
            COUNT(*) FILTER (WHERE campaign_meta.id IS NOT NULL)::bigint AS campaign_metadata_resolved_exposures,
            MAX(e.occurred_at) AS latest_exposure_at
          FROM ad_exposure_events e
          LEFT JOIN ad_platform_entity_metadata campaign_meta
            ON campaign_meta.platform = e.source_platform
           AND campaign_meta.entity_type = 'campaign'
           AND campaign_meta.account_id = e.account_id
           AND campaign_meta.entity_id = e.campaign_id
           AND COALESCE(campaign_meta.tenant_id, '') = COALESCE(e.tenant_id, '')
           AND COALESCE(campaign_meta.workspace_id, '') = COALESCE(e.workspace_id, '')
          WHERE ${filters.join('\n            AND ')}
          GROUP BY e.occurred_at::date, e.source_platform, e.exposure_type
          ORDER BY e.occurred_at::date ASC, e.source_platform ASC, e.exposure_type ASC
        `,
        params
      );
      const rows = result.rows.map(mapExposureCoverageRow);
      const totals = rows.reduce(
        (current, row) => ({
          totalExposures: current.totalExposures + row.totalExposures,
          validExposures: current.validExposures + row.validExposures,
          invalidExposures: current.invalidExposures + row.invalidExposures,
          identityResolvedExposures: current.identityResolvedExposures + row.identityResolvedExposures,
          identityUnresolvedExposures: current.identityUnresolvedExposures + row.identityUnresolvedExposures,
          campaignJoinableExposures: current.campaignJoinableExposures + row.campaignJoinableExposures,
          campaignMetadataResolvedExposures:
            current.campaignMetadataResolvedExposures + row.campaignMetadataResolvedExposures
        }),
        {
          totalExposures: 0,
          validExposures: 0,
          invalidExposures: 0,
          identityResolvedExposures: 0,
          identityUnresolvedExposures: 0,
          campaignJoinableExposures: 0,
          campaignMetadataResolvedExposures: 0
        }
      );

      res.status(200).json({
        schemaVersion: 'ad_exposure_coverage_v1',
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          sourcePlatform: input.sourcePlatform ?? null,
          exposureType: input.exposureType ?? null
        },
        totals: {
          ...totals,
          identityResolutionRate:
            totals.totalExposures > 0 ? totals.identityResolvedExposures / totals.totalExposures : null,
          campaignMetadataResolutionRate:
            totals.campaignJoinableExposures > 0
              ? totals.campaignMetadataResolvedExposures / totals.campaignJoinableExposures
              : null
        },
        rows
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/', async (req, res, next) => {
    try {
      const input = parseInput(mmmQuerySchema, req.query);
      const filters = buildMmmFilters(input);
      const readinessResult = await query<MmmReadinessRow>(
        `
          WITH requested_dates AS (
            SELECT generate_series($1::date, $2::date, interval '1 day')::date AS metric_date
          ),
          filtered_rows AS (
            SELECT
              metric_date,
              COUNT(*) AS row_count,
              MAX(last_computed_at) AS generation_timestamp
            FROM mmm_daily_input_mart_v1
            WHERE ${filters.sql}
            GROUP BY metric_date
          ),
          mart_rows AS (
            SELECT
              metric_date,
              COUNT(*) AS row_count
            FROM mmm_daily_input_mart_v1
            WHERE metric_date BETWEEN $1::date AND $2::date
            GROUP BY metric_date
          )
          SELECT
            requested_dates.metric_date::text,
            COALESCE(filtered_rows.row_count, 0) AS matching_row_count,
            COALESCE(mart_rows.row_count, 0) AS mart_row_count,
            filtered_rows.generation_timestamp
          FROM requested_dates
          LEFT JOIN filtered_rows ON filtered_rows.metric_date = requested_dates.metric_date
          LEFT JOIN mart_rows ON mart_rows.metric_date = requested_dates.metric_date
          ORDER BY requested_dates.metric_date ASC
        `,
        filters.params
      );
      const readiness = deriveReadiness(readinessResult.rows);
      const totalRows = readinessResult.rows.reduce((sum, row) => sum + Number(row.matching_row_count), 0);

      const rowsResult = await query<MmmExportRow>(
        `
          SELECT
            metric_date::text,
            mart_version,
            mart_row_type,
            attribution_model,
            platform,
            platform_connection_id,
            granularity,
            entity_key,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            adset_id,
            adset_name,
            ad_id,
            ad_name,
            creative_id,
            creative_name,
            source,
            medium,
            campaign,
            content,
            term,
            currency,
            spend,
            impressions,
            clicks,
            shopify_orders,
            shopify_revenue,
            attribution_credit_orders,
            attribution_credit_revenue,
            new_customer_credit_orders,
            returning_customer_credit_orders,
            new_customer_credit_revenue,
            returning_customer_credit_revenue,
            match_source_coverage,
            confidence_label_coverage,
            spend_last_synced_at,
            shopify_last_ingested_at,
            attribution_last_computed_at,
            last_computed_at,
            resolver_version,
            resolver_source,
            resolver_confidence,
            resolved_canonical_campaign_id,
            resolved_canonical_campaign_name,
            resolved_canonical_source,
            resolved_canonical_medium,
            resolved_canonical_channel,
            resolved_canonical_channel_group,
            resolved_hierarchy_metadata,
            needs_metadata_qa
          FROM mmm_daily_input_mart_v1
          WHERE ${filters.sql}
          ORDER BY metric_date ASC, mart_row_type ASC, attribution_model ASC, platform ASC, entity_key ASC
          LIMIT $${filters.params.length + 1}
          OFFSET $${filters.params.length + 2}
        `,
        [...filters.params, input.limit, input.offset]
      );
      const rows = rowsResult.rows.map(mapMmmRow);

      if (input.format === 'csv') {
        res.status(200);
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="mmm-${input.startDate}-${input.endDate}.csv"`);
        res.send(renderCsv(rows, readiness.generationTimestamp, readiness.status));
        return;
      }

      res.status(200).json({
        schemaVersion: MMM_SCHEMA_VERSION,
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          martRowType: input.martRowType ?? null,
          attributionModel: input.attributionModel ?? null,
          platform: input.platform ?? null,
          source: input.source ?? null,
          campaign: input.campaign ?? null
        },
        readiness: {
          status: readiness.status,
          generationTimestamp: readiness.generationTimestamp,
          includedDateCount: readiness.includedDateCount,
          excludedDateWindows: readiness.excludedDateWindows
        },
        pagination: {
          limit: input.limit,
          offset: input.offset,
          returned: rows.length,
          totalRows,
          hasMore: input.offset + rows.length < totalRows
        },
        rows
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
