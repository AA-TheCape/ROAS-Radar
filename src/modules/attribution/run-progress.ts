export type AttributionRunCursor = {
  offset: number;
  completed: boolean;
  batchesProcessed: number;
};

export type AttributionRunProgress = {
  processedOrders: number;
  succeededOrders: number;
  failedOrders: number;
  retryOrderIds: string[];
  lastProcessedOrderId: string | null;
  cursor: AttributionRunCursor;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeNonNegativeInteger(value: unknown): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return 0;
  }

  return Math.trunc(numeric);
}

export function buildEmptyAttributionRunProgress(): AttributionRunProgress {
  return {
    processedOrders: 0,
    succeededOrders: 0,
    failedOrders: 0,
    retryOrderIds: [],
    lastProcessedOrderId: null,
    cursor: {
      offset: 0,
      completed: false,
      batchesProcessed: 0
    }
  };
}

export function parseAttributionRunProgress(value: unknown): AttributionRunProgress {
  const defaults = buildEmptyAttributionRunProgress();
  const record = isRecord(value) ? value : {};
  const cursor = isRecord(record.cursor) ? record.cursor : {};

  return {
    processedOrders: normalizeNonNegativeInteger(record.processedOrders),
    succeededOrders: normalizeNonNegativeInteger(record.succeededOrders),
    failedOrders: normalizeNonNegativeInteger(record.failedOrders),
    retryOrderIds: Array.isArray(record.retryOrderIds)
      ? record.retryOrderIds.filter((entry): entry is string => typeof entry === 'string' && entry.trim().length > 0)
      : defaults.retryOrderIds,
    lastProcessedOrderId:
      typeof record.lastProcessedOrderId === 'string' && record.lastProcessedOrderId.trim()
        ? record.lastProcessedOrderId.trim()
        : null,
    cursor: {
      offset: normalizeNonNegativeInteger(cursor.offset),
      completed: cursor.completed === true,
      batchesProcessed: normalizeNonNegativeInteger(cursor.batchesProcessed)
    }
  };
}
