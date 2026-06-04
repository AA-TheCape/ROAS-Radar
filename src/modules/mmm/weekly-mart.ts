import { createHash } from 'node:crypto';

import type { PoolClient } from 'pg';

import { ATTRIBUTION_MODELS } from '../attribution/engine.js';

export const MMM_WEEKLY_CHANNEL_MART_VERSION = 'mmm_weekly_channel_input_mart_v1';
export const MMM_WEEKLY_CHANNEL_SNAPSHOT_VERSION = 'mmm_weekly_channel_snapshot_v1';
export const BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION = 'bayesian_hierarchical_mmm_v1';

function normalizeDate(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new Error(`${fieldName} must use YYYY-MM-DD format`);
  }

  return trimmed;
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

export type WeeklyMmmRefreshInput = {
  startDate: string;
  endDate: string;
  attributionModels?: string[];
};

export type WeeklyMmmQualitySummary = {
  rowCount: number;
  failCount: number;
  warnCount: number;
  unknownDimensionRowCount: number;
  futureDatedSourceRowCount: number;
};

export type WeeklyMmmSnapshotRow = {
  week_start_date: string;
  week_end_date: string;
  mart_version: string;
  source_mart_version: string;
  attribution_model: string;
  channel_key: string;
  source: string;
  medium: string;
  campaign: string;
  channel: string;
  channel_group: string;
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
  controls: unknown;
  deterministic_anchors: unknown;
  missingness_report: unknown;
  leakage_report: unknown;
  dq_status: string;
  source_row_count: string | number;
  generated_at: Date | string;
};

export type BayesianHierarchicalMmmV1FeatureRow = WeeklyMmmSnapshotRow & {
  input_contract_version: typeof BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION;
};

function normalizeAttributionModels(values: string[] | undefined): string[] {
  const allowed = new Set(ATTRIBUTION_MODELS);
  const normalized = [...new Set((values?.length ? values : ['last_touch']).map((value) => value.trim()).filter(Boolean))];

  for (const value of normalized) {
    if (!allowed.has(value as (typeof ATTRIBUTION_MODELS)[number])) {
      throw new Error(`Unsupported attribution model for weekly MMM mart: ${value}`);
    }
  }

  return normalized;
}

export async function refreshWeeklyMmmChannelInputMartWithClient(
  client: PoolClient,
  input: WeeklyMmmRefreshInput
): Promise<WeeklyMmmQualitySummary> {
  const startDate = normalizeDate(input.startDate, 'startDate');
  const endDate = normalizeDate(input.endDate, 'endDate');
  if (startDate > endDate) {
    throw new Error('startDate must be on or before endDate');
  }

  const attributionModels = normalizeAttributionModels(input.attributionModels);

  await client.query('SELECT pg_advisory_xact_lock($1)', [82134724]);
  await client.query(
    `
      DELETE FROM mmm_weekly_channel_input_mart_v1
      WHERE week_start_date <= date_trunc('week', $2::date)::date
        AND week_end_date >= date_trunc('week', $1::date)::date
        AND attribution_model = ANY($3::text[])
    `,
    [startDate, endDate, attributionModels]
  );

  await client.query(
    `
      WITH requested_models AS (
        SELECT unnest($3::text[]) AS attribution_model
      ),
      eligible_weeks AS (
        SELECT
          week_start_date,
          (week_start_date::date + 6) AS week_end_date
        FROM generate_series(
          date_trunc('week', $1::date)::date,
          date_trunc('week', $2::date)::date,
          interval '1 week'
        ) AS generated(week_start_date)
        WHERE week_start_date >= $1::date
          AND (week_start_date::date + 6) <= $2::date
      ),
      daily_paid AS (
        SELECT
          date_trunc('week', metric_date)::date AS week_start_date,
          (date_trunc('week', metric_date)::date + 6) AS week_end_date,
          COALESCE(resolved_canonical_source, source, 'unknown') AS source,
          COALESCE(resolved_canonical_medium, medium, 'unknown') AS medium,
          COALESCE(resolved_canonical_campaign_name, campaign, 'unknown') AS campaign,
          COALESCE(resolved_canonical_channel, medium, 'unknown') AS channel,
          COALESCE(resolved_canonical_channel_group, resolved_canonical_channel, medium, 'unknown') AS channel_group,
          SUM(spend)::numeric(14, 2) AS spend,
          SUM(impressions)::bigint AS impressions,
          SUM(clicks)::bigint AS clicks,
          COUNT(*)::bigint AS source_row_count,
          MAX(metric_date)::date AS max_source_metric_date,
          MIN(spend_last_synced_at) AS min_spend_last_synced_at,
          MAX(spend_last_synced_at) AS max_spend_last_synced_at,
          MIN(last_computed_at) AS min_last_computed_at,
          MAX(last_computed_at) AS max_last_computed_at
        FROM mmm_daily_input_mart_v1
        JOIN eligible_weeks
          ON metric_date BETWEEN eligible_weeks.week_start_date AND eligible_weeks.week_end_date
        WHERE metric_date BETWEEN $1::date AND $2::date
          AND mart_row_type = 'paid_media'
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      daily_attribution AS (
        SELECT
          date_trunc('week', metric_date)::date AS week_start_date,
          (date_trunc('week', metric_date)::date + 6) AS week_end_date,
          attribution_model,
          COALESCE(resolved_canonical_source, source, 'unknown') AS source,
          COALESCE(resolved_canonical_medium, medium, 'unknown') AS medium,
          COALESCE(resolved_canonical_campaign_name, campaign, 'unknown') AS campaign,
          COALESCE(resolved_canonical_channel, medium, 'unknown') AS channel,
          COALESCE(resolved_canonical_channel_group, resolved_canonical_channel, medium, 'unknown') AS channel_group,
          SUM(shopify_orders)::bigint AS shopify_orders,
          SUM(shopify_revenue)::numeric(14, 2) AS shopify_revenue,
          SUM(attribution_credit_orders)::numeric(14, 8) AS attribution_credit_orders,
          SUM(attribution_credit_revenue)::numeric(14, 2) AS attribution_credit_revenue,
          SUM(new_customer_credit_orders)::numeric(14, 8) AS new_customer_credit_orders,
          SUM(returning_customer_credit_orders)::numeric(14, 8) AS returning_customer_credit_orders,
          SUM(new_customer_credit_revenue)::numeric(14, 2) AS new_customer_credit_revenue,
          SUM(returning_customer_credit_revenue)::numeric(14, 2) AS returning_customer_credit_revenue,
          COUNT(*)::bigint AS source_row_count,
          MAX(metric_date)::date AS max_source_metric_date,
          MIN(shopify_last_ingested_at) AS min_shopify_last_ingested_at,
          MAX(shopify_last_ingested_at) AS max_shopify_last_ingested_at,
          MIN(attribution_last_computed_at) AS min_attribution_last_computed_at,
          MAX(attribution_last_computed_at) AS max_attribution_last_computed_at,
          MIN(last_computed_at) AS min_last_computed_at,
          MAX(last_computed_at) AS max_last_computed_at
        FROM mmm_daily_input_mart_v1
        JOIN eligible_weeks
          ON metric_date BETWEEN eligible_weeks.week_start_date AND eligible_weeks.week_end_date
        WHERE metric_date BETWEEN $1::date AND $2::date
          AND mart_row_type = 'attribution'
          AND attribution_model = ANY($3::text[])
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
      ),
      weekly_match_source_coverage AS (
        SELECT
          week_start_date,
          attribution_model,
          source,
          medium,
          campaign,
          channel,
          channel_group,
          jsonb_object_agg(match_key, credited_orders ORDER BY match_key) AS coverage
        FROM (
          SELECT
            date_trunc('week', mart.metric_date)::date AS week_start_date,
            mart.attribution_model,
            COALESCE(mart.resolved_canonical_source, mart.source, 'unknown') AS source,
            COALESCE(mart.resolved_canonical_medium, mart.medium, 'unknown') AS medium,
            COALESCE(mart.resolved_canonical_campaign_name, mart.campaign, 'unknown') AS campaign,
            COALESCE(mart.resolved_canonical_channel, mart.medium, 'unknown') AS channel,
            COALESCE(mart.resolved_canonical_channel_group, mart.resolved_canonical_channel, mart.medium, 'unknown') AS channel_group,
            entry.key AS match_key,
            SUM((entry.value #>> '{}')::numeric)::numeric(14, 8) AS credited_orders
          FROM mmm_daily_input_mart_v1 mart
          JOIN eligible_weeks
            ON mart.metric_date BETWEEN eligible_weeks.week_start_date AND eligible_weeks.week_end_date
          CROSS JOIN LATERAL jsonb_each(mart.match_source_coverage) entry
          WHERE mart.metric_date BETWEEN $1::date AND $2::date
            AND mart.mart_row_type = 'attribution'
            AND mart.attribution_model = ANY($3::text[])
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ) grouped
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      weekly_confidence_label_coverage AS (
        SELECT
          week_start_date,
          attribution_model,
          source,
          medium,
          campaign,
          channel,
          channel_group,
          jsonb_object_agg(confidence_key, credited_orders ORDER BY confidence_key) AS coverage
        FROM (
          SELECT
            date_trunc('week', mart.metric_date)::date AS week_start_date,
            mart.attribution_model,
            COALESCE(mart.resolved_canonical_source, mart.source, 'unknown') AS source,
            COALESCE(mart.resolved_canonical_medium, mart.medium, 'unknown') AS medium,
            COALESCE(mart.resolved_canonical_campaign_name, mart.campaign, 'unknown') AS campaign,
            COALESCE(mart.resolved_canonical_channel, mart.medium, 'unknown') AS channel,
            COALESCE(mart.resolved_canonical_channel_group, mart.resolved_canonical_channel, mart.medium, 'unknown') AS channel_group,
            entry.key AS confidence_key,
            SUM((entry.value #>> '{}')::numeric)::numeric(14, 8) AS credited_orders
          FROM mmm_daily_input_mart_v1 mart
          JOIN eligible_weeks
            ON mart.metric_date BETWEEN eligible_weeks.week_start_date AND eligible_weeks.week_end_date
          CROSS JOIN LATERAL jsonb_each(mart.confidence_label_coverage) entry
          WHERE mart.metric_date BETWEEN $1::date AND $2::date
            AND mart.mart_row_type = 'attribution'
            AND mart.attribution_model = ANY($3::text[])
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ) grouped
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      weekly_keys AS (
        SELECT
          paid.week_start_date,
          paid.week_end_date,
          model.attribution_model,
          paid.source,
          paid.medium,
          paid.campaign,
          paid.channel,
          paid.channel_group
        FROM daily_paid paid
        CROSS JOIN requested_models model

        UNION

        SELECT
          week_start_date,
          week_end_date,
          attribution_model,
          source,
          medium,
          campaign,
          channel,
          channel_group
        FROM daily_attribution
      ),
      combined AS (
        SELECT
          keys.week_start_date,
          keys.week_end_date,
          keys.attribution_model,
          keys.source,
          keys.medium,
          keys.campaign,
          keys.channel,
          keys.channel_group,
          COALESCE(paid.spend, 0)::numeric(14, 2) AS spend,
          COALESCE(paid.impressions, 0)::bigint AS impressions,
          COALESCE(paid.clicks, 0)::bigint AS clicks,
          COALESCE(attr.shopify_orders, 0)::bigint AS shopify_orders,
          COALESCE(attr.shopify_revenue, 0)::numeric(14, 2) AS shopify_revenue,
          COALESCE(attr.attribution_credit_orders, 0)::numeric(14, 8) AS attribution_credit_orders,
          COALESCE(attr.attribution_credit_revenue, 0)::numeric(14, 2) AS attribution_credit_revenue,
          COALESCE(attr.new_customer_credit_orders, 0)::numeric(14, 8) AS new_customer_credit_orders,
          COALESCE(attr.returning_customer_credit_orders, 0)::numeric(14, 8) AS returning_customer_credit_orders,
          COALESCE(attr.new_customer_credit_revenue, 0)::numeric(14, 2) AS new_customer_credit_revenue,
          COALESCE(attr.returning_customer_credit_revenue, 0)::numeric(14, 2) AS returning_customer_credit_revenue,
          COALESCE(match_coverage.coverage, '{}'::jsonb) AS match_source_coverage,
          COALESCE(confidence_coverage.coverage, '{}'::jsonb) AS confidence_label_coverage,
          COALESCE(paid.source_row_count, 0) + COALESCE(attr.source_row_count, 0) AS source_row_count,
          paid.min_spend_last_synced_at,
          paid.max_spend_last_synced_at,
          attr.min_shopify_last_ingested_at,
          attr.max_shopify_last_ingested_at,
          attr.min_attribution_last_computed_at,
          attr.max_attribution_last_computed_at,
          LEAST(
            COALESCE(paid.min_last_computed_at, attr.min_last_computed_at),
            COALESCE(attr.min_last_computed_at, paid.min_last_computed_at)
          ) AS min_source_last_computed_at,
          GREATEST(
            COALESCE(paid.max_last_computed_at, attr.max_last_computed_at),
            COALESCE(attr.max_last_computed_at, paid.max_last_computed_at)
          ) AS max_source_last_computed_at,
          GREATEST(
            COALESCE(paid.max_source_metric_date, attr.max_source_metric_date),
            COALESCE(attr.max_source_metric_date, paid.max_source_metric_date)
          ) AS max_source_metric_date
        FROM weekly_keys keys
        LEFT JOIN daily_paid paid
          ON paid.week_start_date = keys.week_start_date
         AND paid.source = keys.source
         AND paid.medium = keys.medium
         AND paid.campaign = keys.campaign
         AND paid.channel = keys.channel
         AND paid.channel_group = keys.channel_group
        LEFT JOIN daily_attribution attr
          ON attr.week_start_date = keys.week_start_date
         AND attr.attribution_model = keys.attribution_model
         AND attr.source = keys.source
         AND attr.medium = keys.medium
         AND attr.campaign = keys.campaign
         AND attr.channel = keys.channel
         AND attr.channel_group = keys.channel_group
        LEFT JOIN weekly_match_source_coverage match_coverage
          ON match_coverage.week_start_date = keys.week_start_date
         AND match_coverage.attribution_model = keys.attribution_model
         AND match_coverage.source = keys.source
         AND match_coverage.medium = keys.medium
         AND match_coverage.campaign = keys.campaign
         AND match_coverage.channel = keys.channel
         AND match_coverage.channel_group = keys.channel_group
        LEFT JOIN weekly_confidence_label_coverage confidence_coverage
          ON confidence_coverage.week_start_date = keys.week_start_date
         AND confidence_coverage.attribution_model = keys.attribution_model
         AND confidence_coverage.source = keys.source
         AND confidence_coverage.medium = keys.medium
         AND confidence_coverage.campaign = keys.campaign
         AND confidence_coverage.channel = keys.channel
         AND confidence_coverage.channel_group = keys.channel_group
      )
      INSERT INTO mmm_weekly_channel_input_mart_v1 (
        week_start_date,
        week_end_date,
        mart_version,
        source_mart_version,
        attribution_model,
        channel_key,
        source,
        medium,
        campaign,
        channel,
        channel_group,
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
        controls,
        deterministic_anchors,
        missingness_report,
        leakage_report,
        dq_status,
        source_row_count,
        generated_at
      )
      SELECT
        week_start_date,
        week_end_date,
        'mmm_weekly_channel_input_mart_v1',
        'mmm_daily_input_mart_v1',
        attribution_model,
        concat_ws('|', source, medium, campaign, channel, channel_group),
        source,
        medium,
        campaign,
        channel,
        channel_group,
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
        jsonb_build_object(
          'inputContractVersion', 'bayesian_hierarchical_mmm_v1',
          'weekOfYear', EXTRACT(WEEK FROM week_start_date)::int,
          'month', EXTRACT(MONTH FROM week_start_date)::int,
          'quarter', EXTRACT(QUARTER FROM week_start_date)::int,
          'isYearEndHolidayWindow', EXTRACT(MONTH FROM week_start_date)::int = 12,
          'isNewYearHolidayWindow', EXTRACT(MONTH FROM week_start_date)::int = 1
        ),
        jsonb_build_object(
          'inputContractVersion', 'bayesian_hierarchical_mmm_v1',
          'attributionModel', attribution_model,
          'clickLookbackWindowDays', 30,
          'viewLookbackWindowDays', 7,
          'lookbackWindows', jsonb_build_object(
            'click', jsonb_build_object('days', 30, 'rule', '30d_click'),
            'view', jsonb_build_object('days', 7, 'rule', '7d_view')
          ),
          'attributionCreditOrders', attribution_credit_orders,
          'attributionCreditRevenue', attribution_credit_revenue,
          'newCustomerCreditRevenue', new_customer_credit_revenue,
          'returningCustomerCreditRevenue', returning_customer_credit_revenue,
          'matchSourceCoverage', match_source_coverage,
          'confidenceLabelCoverage', confidence_label_coverage
        ),
        jsonb_build_object(
          'missingDimensions',
          array_remove(ARRAY[
            CASE WHEN source = 'unknown' THEN 'source' END,
            CASE WHEN medium = 'unknown' THEN 'medium' END,
            CASE WHEN campaign = 'unknown' THEN 'campaign' END,
            CASE WHEN channel = 'unknown' THEN 'channel' END
          ], NULL),
          'hasSpendWithoutDelivery', spend > 0 AND impressions = 0 AND clicks = 0,
          'hasOutcomeWithoutAttributionCredit', shopify_orders > 0 AND attribution_credit_orders = 0
        ),
        jsonb_build_object(
          'inputContractVersion', 'bayesian_hierarchical_mmm_v1',
          'isCompleteWeek', true,
          'freshness', jsonb_build_object(
            'minSpendLastSyncedAt', min_spend_last_synced_at,
            'maxSpendLastSyncedAt', max_spend_last_synced_at,
            'minShopifyLastIngestedAt', min_shopify_last_ingested_at,
            'maxShopifyLastIngestedAt', max_shopify_last_ingested_at,
            'minAttributionLastComputedAt', min_attribution_last_computed_at,
            'maxAttributionLastComputedAt', max_attribution_last_computed_at,
            'minSourceLastComputedAt', min_source_last_computed_at,
            'maxSourceLastComputedAt', max_source_last_computed_at
          ),
          'calibrationMetadata', jsonb_build_object(
            'contractVersion', 'bayesian_hierarchical_mmm_v1',
            'clickLookbackWindowDays', 30,
            'viewLookbackWindowDays', 7,
            'attributionLookbackRules', jsonb_build_array('30d_click', '7d_view')
          ),
          'maxSourceMetricDate', max_source_metric_date,
          'latestAllowedMetricDate', week_end_date,
          'hasFutureDatedSourceRows', COALESCE(max_source_metric_date > week_end_date, false)
        ),
        CASE
          WHEN COALESCE(max_source_metric_date > week_end_date, false) THEN 'fail'
          WHEN source = 'unknown' OR medium = 'unknown' OR campaign = 'unknown' OR channel = 'unknown' THEN 'warn'
          WHEN spend > 0 AND impressions = 0 AND clicks = 0 THEN 'warn'
          WHEN shopify_orders > 0 AND attribution_credit_orders = 0 THEN 'warn'
          ELSE 'pass'
        END,
        source_row_count,
        now()
      FROM combined
      ORDER BY week_start_date ASC, attribution_model ASC, source ASC, medium ASC, campaign ASC
    `,
    [startDate, endDate, attributionModels]
  );

  return getWeeklyMmmQualitySummaryWithClient(client, startDate, endDate, attributionModels);
}

export async function getWeeklyMmmQualitySummaryWithClient(
  client: PoolClient,
  startDate: string,
  endDate: string,
  attributionModels: string[]
): Promise<WeeklyMmmQualitySummary> {
  const result = await client.query<{
    row_count: string;
    fail_count: string;
    warn_count: string;
    unknown_dimension_row_count: string;
    future_dated_source_row_count: string;
  }>(
    `
      SELECT
        COUNT(*)::text AS row_count,
        COUNT(*) FILTER (WHERE dq_status = 'fail')::text AS fail_count,
        COUNT(*) FILTER (WHERE dq_status = 'warn')::text AS warn_count,
        COUNT(*) FILTER (WHERE jsonb_array_length(missingness_report->'missingDimensions') > 0)::text AS unknown_dimension_row_count,
        COUNT(*) FILTER (WHERE leakage_report->>'hasFutureDatedSourceRows' = 'true')::text AS future_dated_source_row_count
      FROM mmm_weekly_channel_input_mart_v1
      WHERE week_start_date <= date_trunc('week', $2::date)::date
        AND week_end_date >= date_trunc('week', $1::date)::date
        AND attribution_model = ANY($3::text[])
    `,
    [startDate, endDate, attributionModels]
  );
  const row = result.rows[0];

  return {
    rowCount: Number(row?.row_count ?? 0),
    failCount: Number(row?.fail_count ?? 0),
    warnCount: Number(row?.warn_count ?? 0),
    unknownDimensionRowCount: Number(row?.unknown_dimension_row_count ?? 0),
    futureDatedSourceRowCount: Number(row?.future_dated_source_row_count ?? 0)
  };
}

export async function fetchWeeklyMmmSnapshotRowsWithClient(
  client: PoolClient,
  input: WeeklyMmmRefreshInput
): Promise<WeeklyMmmSnapshotRow[]> {
  const startDate = normalizeDate(input.startDate, 'startDate');
  const endDate = normalizeDate(input.endDate, 'endDate');
  const attributionModels = normalizeAttributionModels(input.attributionModels);
  const result = await client.query<WeeklyMmmSnapshotRow>(
    `
      SELECT
        week_start_date::text,
        week_end_date::text,
        mart_version,
        source_mart_version,
        attribution_model,
        channel_key,
        source,
        medium,
        campaign,
        channel,
        channel_group,
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
        controls,
        deterministic_anchors,
        missingness_report,
        leakage_report,
        dq_status,
        source_row_count,
        generated_at
      FROM mmm_weekly_channel_input_mart_v1
      WHERE week_start_date <= date_trunc('week', $2::date)::date
        AND week_end_date >= date_trunc('week', $1::date)::date
        AND attribution_model = ANY($3::text[])
      ORDER BY week_start_date ASC, attribution_model ASC, channel_key ASC
    `,
    [startDate, endDate, attributionModels]
  );

  return result.rows;
}

export async function fetchBayesianHierarchicalMmmV1FeatureRowsWithClient(
  client: PoolClient,
  input: WeeklyMmmRefreshInput
): Promise<BayesianHierarchicalMmmV1FeatureRow[]> {
  const rows = await fetchWeeklyMmmSnapshotRowsWithClient(client, input);

  return rows.map((row) => ({
    input_contract_version: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
    ...row
  }));
}

export async function snapshotWeeklyMmmInputRowsWithClient(
  client: PoolClient,
  modelRunId: string,
  rows: WeeklyMmmSnapshotRow[]
): Promise<{ snapshotRowCount: number; snapshotHash: string }> {
  await client.query('DELETE FROM mmm_model_run_input_snapshots WHERE model_run_id = $1', [modelRunId]);

  let rowNumber = 0;
  for (const row of rows) {
    rowNumber += 1;
    await client.query(
      `
        INSERT INTO mmm_model_run_input_snapshots (
          model_run_id,
          snapshot_version,
          mart_version,
          row_number,
          row_hash,
          input_row
        )
        VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb)
      `,
      [
        modelRunId,
        MMM_WEEKLY_CHANNEL_SNAPSHOT_VERSION,
        MMM_WEEKLY_CHANNEL_MART_VERSION,
        rowNumber,
        hashJson(row),
        JSON.stringify(row)
      ]
    );
  }

  return {
    snapshotRowCount: rows.length,
    snapshotHash: hashJson(rows)
  };
}
