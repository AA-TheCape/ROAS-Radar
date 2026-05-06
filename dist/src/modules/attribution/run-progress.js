function isRecord(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function normalizeNonNegativeInteger(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0) {
        return 0;
    }
    return Math.trunc(numeric);
}
export function buildEmptyAttributionRunProgress() {
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
export function parseAttributionRunProgress(value) {
    const defaults = buildEmptyAttributionRunProgress();
    const record = isRecord(value) ? value : {};
    const cursor = isRecord(record.cursor) ? record.cursor : {};
    return {
        processedOrders: normalizeNonNegativeInteger(record.processedOrders),
        succeededOrders: normalizeNonNegativeInteger(record.succeededOrders),
        failedOrders: normalizeNonNegativeInteger(record.failedOrders),
        retryOrderIds: Array.isArray(record.retryOrderIds)
            ? record.retryOrderIds.filter((entry) => typeof entry === 'string' && entry.trim().length > 0)
            : defaults.retryOrderIds,
        lastProcessedOrderId: typeof record.lastProcessedOrderId === 'string' && record.lastProcessedOrderId.trim()
            ? record.lastProcessedOrderId.trim()
            : null,
        cursor: {
            offset: normalizeNonNegativeInteger(cursor.offset),
            completed: cursor.completed === true,
            batchesProcessed: normalizeNonNegativeInteger(cursor.batchesProcessed)
        }
    };
}
