function isRecord(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function normalizeNonNegativeNumber(value) {
    const normalized = Number(value ?? 0);
    return Number.isFinite(normalized) && normalized >= 0 ? normalized : 0;
}
function normalizeTierCounts(value) {
    const record = isRecord(value) ? value : {};
    return {
        deterministic_first_party: normalizeNonNegativeNumber(record.deterministic_first_party),
        deterministic_shopify_hint: normalizeNonNegativeNumber(record.deterministic_shopify_hint),
        platform_reported_meta: normalizeNonNegativeNumber(record.platform_reported_meta),
        ga4_fallback: normalizeNonNegativeNumber(record.ga4_fallback),
        unattributed: normalizeNonNegativeNumber(record.unattributed)
    };
}
export function buildEmptyOrderAttributionBackfillProgress() {
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
export function parseOrderAttributionBackfillProgress(value) {
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
                completenessRate: Number(beforeMetrics.completenessRate ?? 1),
                tierCounts: normalizeTierCounts(beforeMetrics.tierCounts)
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
        failures: Array.isArray(record.failures) ? record.failures : defaults.failures,
        preview: Array.isArray(record.preview) ? record.preview : defaults.preview,
        cursor: {
            lastOrderOccurredAt: typeof cursor.lastOrderOccurredAt === 'string' ? cursor.lastOrderOccurredAt : null,
            lastOrderRowId: typeof cursor.lastOrderRowId === 'string' ? cursor.lastOrderRowId : null,
            completed: cursor.completed === true,
            batchesProcessed: normalizeNonNegativeNumber(cursor.batchesProcessed)
        }
    };
}
