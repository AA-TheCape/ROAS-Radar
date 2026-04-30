// Key additions in the Meta order-value sync module.

import { logError, logInfo, logWarning } from '../../observability/index.js';

type MetaAdsApiRequestMetricsAccumulator = {
  requestCount: number;
  errorCount: number;
  retryCount: number;
  latencyMsTotal: number;
  latencyMsMax: number;
};

type MetaAdsOrderValueBaselineStats = {
  totalRows: number;
  nullAttributedRevenueCount: number;
  nullPurchaseCountCount: number;
  nullActionTypeCount: number;
};

type MetaAdsOrderValueRecordSummary = MetaAdsOrderValueBaselineStats & {
  fallbackSelectionCount: number;
  prioritySelectionCount: number;
  noSelectionCount: number;
};

type MetaAdsOrderValueSyncAnomaly = {
  type:
    | 'zero_rows_pulled'
    | 'null_attributed_revenue_spike'
    | 'null_purchase_count_spike'
    | 'null_action_type_spike';
  severity: 'warning';
  summary: string;
  details: Record<string, unknown>;
};

type MetaAdsSyncAuditContext = {
  connectionId: number;
  syncJobId: number;
  transactionSource: string;
  sourceMetadata?: Record<string, unknown>;
  metrics?: MetaAdsApiRequestMetricsAccumulator;
};

function safeRatio(numerator: number, denominator: number): number | null {
  if (denominator <= 0) {
    return null;
  }

  return Number((numerator / denominator).toFixed(4));
}

function buildMetaAdsApiRequestMetricsAccumulator(): MetaAdsApiRequestMetricsAccumulator {
  return {
    requestCount: 0,
    errorCount: 0,
    retryCount: 0,
    latencyMsTotal: 0,
    latencyMsMax: 0
  };
}

function summarizeOrderValueRecords(records: MetaAdsCampaignDailyRevenueRecord[]): MetaAdsOrderValueRecordSummary {
  return records.reduce<MetaAdsOrderValueRecordSummary>(
    (summary, record) => {
      summary.totalRows += 1;

      if (record.attributedRevenue === null) {
        summary.nullAttributedRevenueCount += 1;
      }

      if (record.purchaseCount === null) {
        summary.nullPurchaseCountCount += 1;
      }

      if (record.actionTypeUsed === null) {
        summary.nullActionTypeCount += 1;
      }

      if (record.canonicalSelectionMode === 'fallback') {
        summary.fallbackSelectionCount += 1;
      } else if (record.canonicalSelectionMode === 'priority') {
        summary.prioritySelectionCount += 1;
      } else {
        summary.noSelectionCount += 1;
      }

      return summary;
    },
    {
      totalRows: 0,
      nullAttributedRevenueCount: 0,
      nullPurchaseCountCount: 0,
      nullActionTypeCount: 0,
      fallbackSelectionCount: 0,
      prioritySelectionCount: 0,
      noSelectionCount: 0
    }
  );
}

function buildOrderValueSyncAnomalies(input: {
  rawRowsFetched: number;
  records: MetaAdsCampaignDailyRevenueRecord[];
  summary: MetaAdsOrderValueRecordSummary;
  baseline: MetaAdsOrderValueBaselineStats;
}): MetaAdsOrderValueSyncAnomaly[] {
  const anomalies: MetaAdsOrderValueSyncAnomaly[] = [];

  if (input.rawRowsFetched === 0 || input.summary.totalRows === 0) {
    anomalies.push({
      type: 'zero_rows_pulled',
      severity: 'warning',
      summary: 'Meta order value sync completed with zero upstream rows or zero normalized records.',
      details: {
        rawRowsFetched: input.rawRowsFetched,
        normalizedRecords: input.summary.totalRows
      }
    });
  }

  // Current-vs-baseline null spike checks for revenue, purchase count, and canonical action type.
  // Uses configured min rows, current null ratio floor, and ratio delta thresholds.
  // ...
  return anomalies;
}

async function loadOrderValueBaselineStats(
  connectionId: number,
  windowStartDate: string,
  windowEndDate: string
): Promise<MetaAdsOrderValueBaselineStats> {
  const result = await query<{
    total_rows: string;
    null_attributed_revenue_count: string;
    null_purchase_count_count: string;
    null_action_type_count: string;
  }>(
    `
      SELECT
        COUNT(*)::text AS total_rows,
        COUNT(*) FILTER (WHERE attributed_revenue IS NULL)::text AS null_attributed_revenue_count,
        COUNT(*) FILTER (WHERE purchase_count IS NULL)::text AS null_purchase_count_count,
        COUNT(*) FILTER (WHERE canonical_action_type IS NULL)::text AS null_action_type_count
      FROM meta_ads_order_value_aggregates
      WHERE meta_connection_id = $1
        AND report_date BETWEEN $2::date AND $3::date
        AND action_report_time = 'conversion'
        AND use_account_attribution_setting = true
    `,
    [connectionId, windowStartDate, windowEndDate]
  );

  const row = result.rows[0];

  return {
    totalRows: Number(row?.total_rows ?? '0'),
    nullAttributedRevenueCount: Number(row?.null_attributed_revenue_count ?? '0'),
    nullPurchaseCountCount: Number(row?.null_purchase_count_count ?? '0'),
    nullActionTypeCount: Number(row?.null_action_type_count ?? '0')
  };
}

async function metaFetchJson<T>(url: URL, retryCount = 2, audit?: MetaAdsSyncAuditContext): Promise<T> {
  // Added per-attempt latency/error telemetry.
  // Success emits `meta_ads_api_request_completed`.
  // Retryable or terminal failure emits `meta_ads_api_request_failed`.
  // Metrics accumulator tracks request count, retries, errors, and latency totals/max.
}

function emitOrderValueSyncAnomalies(params: {
  runId: number;
  connectionId: number;
  adAccountId: string;
  triggerSource: string;
  windowStartDate: string;
  windowEndDate: string;
  anomalies: MetaAdsOrderValueSyncAnomaly[];
}): void {
  for (const anomaly of params.anomalies) {
    logWarning('meta_ads_order_value_sync_anomaly', {
      service: process.env.K_SERVICE ?? 'roas-radar',
      runId: params.runId,
      connectionId: params.connectionId,
      adAccountId: params.adAccountId,
      triggerSource: params.triggerSource,
      windowStartDate: params.windowStartDate,
      windowEndDate: params.windowEndDate,
      anomalyType: anomaly.type,
      severity: anomaly.severity,
      summary: anomaly.summary,
      details: anomaly.details,
      alertable: true
    });
  }
}

function emitOrderValueSyncConnectionCompleted(/* ... */): void {
  logInfo('meta_ads_order_value_sync_connection_completed', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    rawRowsFetched: params.rawRowsFetched,
    normalizedRecordsReceived: params.accumulator.recordsReceived,
    rawRowsPersisted: params.accumulator.rawRowsPersisted,
    aggregateRowsUpserted: params.accumulator.aggregateRowsUpserted,
    apiRequestCount: params.apiMetrics.requestCount,
    apiRequestErrorCount: params.apiMetrics.errorCount,
    apiRequestRetryCount: params.apiMetrics.retryCount,
    apiLatencyMsTotal: params.apiMetrics.latencyMsTotal,
    apiLatencyMsMax: params.apiMetrics.latencyMsMax,
    apiLatencyMsAvg:
      params.apiMetrics.requestCount > 0 ? Number((params.apiMetrics.latencyMsTotal / params.apiMetrics.requestCount).toFixed(2)) : 0,
    nullAttributedRevenueCount: params.summary.nullAttributedRevenueCount,
    nullAttributedRevenueRate: safeRatio(params.summary.nullAttributedRevenueCount, currentTotalRows),
    nullPurchaseCountCount: params.summary.nullPurchaseCountCount,
    nullPurchaseCountRate: safeRatio(params.summary.nullPurchaseCountCount, currentTotalRows),
    nullActionTypeCount: params.summary.nullActionTypeCount,
    nullActionTypeRate: safeRatio(params.summary.nullActionTypeCount, currentTotalRows),
    anomalyCount: params.anomalies.length,
    anomalyTypes: params.anomalies.map((anomaly) => anomaly.type),
    zeroRowsPulled: params.rawRowsFetched === 0 || params.summary.totalRows === 0
  });
}

export async function runMetaAdsOrderValueSync(options: {
  now?: Date;
  triggerSource?: string;
} = {}): Promise<MetaAdsOrderValueSyncResult> {
  // Added per-connection API metrics accumulator and baseline query.
  // On success:
  // - compute record summary
  // - evaluate anomalies before overwrite
  // - emit connection completion log
  // - emit anomaly logs
  // - roll totals into the final sync summary event
  //
  // On failure:
  // - emit enriched `meta_ads_order_value_sync_connection_failed`
  //   with code, alertable, api counts, and latency totals.
}

export const __metaAdsTestUtils = {
  // ...
  summarizeOrderValueRecords,
  buildOrderValueSyncAnomalies,
  // ...
};
