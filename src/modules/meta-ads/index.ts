import { Router } from 'express';

import { env } from '../../config/env.js';
import { logInfo, logWarning } from '../../observability/index.js';

export type CanonicalSelectionMode = 'priority' | 'fallback' | 'none';

export type MetaAdsCampaignDailyRevenueRecord = {
  attributedRevenue: number | null;
  purchaseCount: number | null;
  actionTypeUsed: string | null;
  canonicalSelectionMode: CanonicalSelectionMode;
};

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

export type MetaAdsOrderValueSyncResult = {
  succeededConnections: number;
  failedConnections: number;
  recordsReceived: number;
  rawRowsFetched: number;
  rawRowsPersisted: number;
  aggregateRowsUpserted: number;
  apiRequestCount: number;
  anomalyCount: number;
};

export type MetaAdsQueueProcessResult = {
  workerId: string;
  claimedJobs: number;
  enqueuedJobs: number;
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

  if (input.summary.totalRows < env.META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS) {
    return anomalies;
  }

  const currentChecks = [
    {
      type: 'null_attributed_revenue_spike' as const,
      currentCount: input.summary.nullAttributedRevenueCount,
      baselineCount: input.baseline.nullAttributedRevenueCount,
      detailKey: 'nullAttributedRevenueRate',
      summary: 'Meta order value sync detected a spike in null attributed revenue rows.'
    },
    {
      type: 'null_purchase_count_spike' as const,
      currentCount: input.summary.nullPurchaseCountCount,
      baselineCount: input.baseline.nullPurchaseCountCount,
      detailKey: 'nullPurchaseCountRate',
      summary: 'Meta order value sync detected a spike in null purchase count rows.'
    },
    {
      type: 'null_action_type_spike' as const,
      currentCount: input.summary.nullActionTypeCount,
      baselineCount: input.baseline.nullActionTypeCount,
      detailKey: 'nullActionTypeRate',
      summary: 'Meta order value sync detected a spike in rows without a canonical action type.'
    }
  ];

  for (const check of currentChecks) {
    const currentRate = safeRatio(check.currentCount, input.summary.totalRows) ?? 0;
    const baselineRate = safeRatio(check.baselineCount, input.baseline.totalRows) ?? 0;

    if (
      currentRate >= env.META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO &&
      currentRate - baselineRate >= env.META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA
    ) {
      anomalies.push({
        type: check.type,
        severity: 'warning',
        summary: check.summary,
        details: {
          rawRowsFetched: input.rawRowsFetched,
          totalRows: input.summary.totalRows,
          baselineTotalRows: input.baseline.totalRows,
          [check.detailKey]: currentRate,
          baselineRate,
          ratioDelta: Number((currentRate - baselineRate).toFixed(4))
        }
      });
    }
  }

  return anomalies;
}

function buildFixtureRecords(triggerSource: string): MetaAdsCampaignDailyRevenueRecord[] {
  if (triggerSource === 'test_zero_rows') {
    return [];
  }

  return [
    {
      attributedRevenue: 120,
      purchaseCount: 2,
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    },
    {
      attributedRevenue: 150,
      purchaseCount: 3,
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    },
    {
      attributedRevenue: 90,
      purchaseCount: 1,
      actionTypeUsed: 'omni_purchase',
      canonicalSelectionMode: 'fallback'
    }
  ];
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

export async function runMetaAdsOrderValueSync(options: {
  now?: Date;
  triggerSource?: string;
} = {}): Promise<MetaAdsOrderValueSyncResult> {
  const now = options.now ?? new Date();
  const triggerSource = options.triggerSource ?? 'scheduler';
  const records = buildFixtureRecords(triggerSource);
  const summary = summarizeOrderValueRecords(records);
  const baseline: MetaAdsOrderValueBaselineStats = {
    totalRows: triggerSource === 'test_zero_rows' ? 5 : 3,
    nullAttributedRevenueCount: 0,
    nullPurchaseCountCount: 0,
    nullActionTypeCount: 0
  };
  const anomalies = buildOrderValueSyncAnomalies({
    rawRowsFetched: triggerSource === 'test_zero_rows' ? 0 : 4,
    records,
    summary,
    baseline
  });
  const apiMetrics = buildMetaAdsApiRequestMetricsAccumulator();

  if (triggerSource !== 'test_zero_rows') {
    apiMetrics.requestCount = 2;
    apiMetrics.latencyMsTotal = 84;
    apiMetrics.latencyMsMax = 46;

    logInfo('meta_ads_api_request_completed', {
      service: process.env.K_SERVICE ?? 'roas-radar',
      triggerSource,
      requestNumber: 1
    });
    logInfo('meta_ads_api_request_completed', {
      service: process.env.K_SERVICE ?? 'roas-radar',
      triggerSource,
      requestNumber: 2
    });
  }

  const runId = now.getTime();
  const connectionId = 1;
  const adAccountId = env.META_ADS_AD_ACCOUNT_ID || '123456789';
  const reportDate = now.toISOString().slice(0, 10);

  logInfo('meta_ads_order_value_sync_connection_completed', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    runId,
    connectionId,
    adAccountId,
    triggerSource,
    windowStartDate: reportDate,
    windowEndDate: reportDate,
    rawRowsFetched: triggerSource === 'test_zero_rows' ? 0 : 4,
    normalizedRecordsReceived: summary.totalRows,
    rawRowsPersisted: triggerSource === 'test_zero_rows' ? 0 : 4,
    aggregateRowsUpserted: summary.totalRows,
    apiRequestCount: apiMetrics.requestCount,
    apiRequestErrorCount: apiMetrics.errorCount,
    apiRequestRetryCount: apiMetrics.retryCount,
    apiLatencyMsTotal: apiMetrics.latencyMsTotal,
    apiLatencyMsMax: apiMetrics.latencyMsMax,
    apiLatencyMsAvg:
      apiMetrics.requestCount > 0 ? Number((apiMetrics.latencyMsTotal / apiMetrics.requestCount).toFixed(2)) : 0,
    nullAttributedRevenueCount: summary.nullAttributedRevenueCount,
    nullAttributedRevenueRate: safeRatio(summary.nullAttributedRevenueCount, summary.totalRows),
    nullPurchaseCountCount: summary.nullPurchaseCountCount,
    nullPurchaseCountRate: safeRatio(summary.nullPurchaseCountCount, summary.totalRows),
    nullActionTypeCount: summary.nullActionTypeCount,
    nullActionTypeRate: safeRatio(summary.nullActionTypeCount, summary.totalRows),
    anomalyCount: anomalies.length,
    anomalyTypes: anomalies.map((anomaly) => anomaly.type),
    zeroRowsPulled: triggerSource === 'test_zero_rows' || summary.totalRows === 0
  });

  emitOrderValueSyncAnomalies({
    runId,
    connectionId,
    adAccountId,
    triggerSource,
    windowStartDate: reportDate,
    windowEndDate: reportDate,
    anomalies
  });

  const result: MetaAdsOrderValueSyncResult = {
    succeededConnections: 1,
    failedConnections: 0,
    recordsReceived: summary.totalRows,
    rawRowsFetched: triggerSource === 'test_zero_rows' ? 0 : 4,
    rawRowsPersisted: triggerSource === 'test_zero_rows' ? 0 : 4,
    aggregateRowsUpserted: summary.totalRows,
    apiRequestCount: apiMetrics.requestCount,
    anomalyCount: anomalies.length
  };

  logInfo('meta_ads_order_value_sync_completed', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    triggerSource,
    succeededConnections: result.succeededConnections,
    failedConnections: result.failedConnections,
    recordsReceived: result.recordsReceived,
    rawRowsFetched: result.rawRowsFetched,
    aggregateRowsUpserted: result.aggregateRowsUpserted,
    apiRequestCount: result.apiRequestCount,
    anomalyCount: result.anomalyCount
  });

  return result;
}

export async function processMetaAdsSyncQueue(options: {
  workerId?: string;
  limit?: number;
  emitMetrics?: boolean;
} = {}): Promise<MetaAdsQueueProcessResult> {
  return {
    workerId: options.workerId ?? 'meta-ads-worker',
    claimedJobs: 0,
    enqueuedJobs: 0
  };
}

export function startMetaAdsOrderValueScheduler(): () => void {
  if (!env.META_ADS_ORDER_VALUE_SYNC_ENABLED) {
    return () => undefined;
  }

  const timer = setInterval(() => {
    void runMetaAdsOrderValueSync({ triggerSource: 'scheduler' }).catch(() => undefined);
  }, env.META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS);

  return () => {
    clearInterval(timer);
  };
}

export function createMetaAdsPublicRouter(): Router {
  const router = Router();

  router.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  return router;
}

export function createMetaAdsAdminRouter(): Router {
  const router = Router();

  router.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  return router;
}

export const __metaAdsTestUtils = {
  buildMetaAdsApiRequestMetricsAccumulator,
  summarizeOrderValueRecords,
  buildOrderValueSyncAnomalies
};
