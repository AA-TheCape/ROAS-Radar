export type OrderAttributionBackfillCursor = {
  lastOrderOccurredAt: string | null;
  lastOrderRowId: string | null;
  completed: boolean;
  batchesProcessed: number;
};

export type OrderAttributionBackfillProgress = {
  beforeMetrics: {
    totalOrdersInScope: number;
    ordersMissingAttribution: number;
    ordersWithAttribution: number;
    completenessRate: number;
  } | null;
  scannedOrders: number;
  recoverableOrders: number;
  recoveredOrders: number;
  unrecoverableOrders: number;
  failedOrders: number;
  shopifyWritebackCompleted: number;
  shopifyWritebackSkipped: number;
  shopifyWritebackFailed: number;
  failures: Array<{
    orderId: string | null;
    code: string;
    message: string;
  }>;
  preview: Array<{
    shopifyOrderId: string;
    orderOccurredAt: string;
    recoverable: boolean;
    touchpointCount: number;
    winnerSessionId: string | null;
    attributionReason: string;
  }>;
  cursor: OrderAttributionBackfillCursor;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeNonNegativeNumber(value: unknown): number {
  const normalized = Number(value ?? 0);
  return Number.isFinite(normalized) && normalized >= 0 ? normalized : 0;
}

export function buildEmptyOrderAttributionBackfillProgress(): OrderAttributionBackfillProgress {
  return {
    beforeMetrics: null,
    scannedOrders: 0,
    recoverableOrders: 0,
    recoveredOrders: 0,
    unrecoverableOrders: 0,
    failedOrders: 0,
    shopifyWritebackCompleted: 0,
    shopifyWritebackSkipped: 0,
    shopifyWritebackFailed: 0,
    failures: [],
    preview: [],
    cursor: {
      lastOrderOccurredAt: null,
      lastOrderRowId: null,
      completed: false,
      batchesProcessed: 0
    }
  };
}

export function parseOrderAttributionBackfillProgress(value: unknown): OrderAttributionBackfillProgress {
  const defaults = buildEmptyOrderAttributionBackfillProgress();
  const record = isRecord(value) ? value : {};
  const beforeMetrics = isRecord(record.beforeMetrics) ? record.beforeMetrics : null;
  const cursor = isRecord(record.cursor) ? record.cursor : {};

  return {
    beforeMetrics: beforeMetrics
      ? {
          totalOrdersInScope: normalizeNonNegativeNumber(beforeMetrics.totalOrdersInScope),
          ordersMissingAttribution: normalizeNonNegativeNumber(beforeMetrics.ordersMissingAttribution),
          ordersWithAttribution: normalizeNonNegativeNumber(beforeMetrics.ordersWithAttribution),
          completenessRate: Number(beforeMetrics.completenessRate ?? 1)
        }
      : null,
    scannedOrders: normalizeNonNegativeNumber(record.scannedOrders),
    recoverableOrders: normalizeNonNegativeNumber(record.recoverableOrders),
    recoveredOrders: normalizeNonNegativeNumber(record.recoveredOrders),
    unrecoverableOrders: normalizeNonNegativeNumber(record.unrecoverableOrders),
    failedOrders: normalizeNonNegativeNumber(record.failedOrders),
    shopifyWritebackCompleted: normalizeNonNegativeNumber(record.shopifyWritebackCompleted),
    shopifyWritebackSkipped: normalizeNonNegativeNumber(record.shopifyWritebackSkipped),
    shopifyWritebackFailed: normalizeNonNegativeNumber(record.shopifyWritebackFailed),
    failures: Array.isArray(record.failures) ? (record.failures as OrderAttributionBackfillProgress['failures']) : defaults.failures,
    preview: Array.isArray(record.preview) ? (record.preview as OrderAttributionBackfillProgress['preview']) : defaults.preview,
    cursor: {
      lastOrderOccurredAt: typeof cursor.lastOrderOccurredAt === 'string' ? cursor.lastOrderOccurredAt : null,
      lastOrderRowId: typeof cursor.lastOrderRowId === 'string' ? cursor.lastOrderRowId : null,
      completed: cursor.completed === true,
      batchesProcessed: normalizeNonNegativeNumber(cursor.batchesProcessed)
    }
  };
}
