"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getIdentityGraphBackfillRun = getIdentityGraphBackfillRun;
exports.backfillHistoricalIdentityGraph = backfillHistoricalIdentityGraph;
const node_crypto_1 = require("node:crypto");
const pool_js_1 = require("../../db/pool.js");
const index_js_1 = require("../../observability/index.js");
const index_js_2 = require("./index.js");
const IDENTITY_BACKFILL_SOURCES = [
    'tracking_sessions',
    'tracking_events',
    'shopify_customers',
    'shopify_orders'
];
const DEFAULT_IDENTITY_GRAPH_BACKFILL_BATCH_SIZE = 250;
function buildEmptyOutcomeCount() {
    return {
        linked: 0,
        skipped: 0,
        conflict: 0,
        deduplicated: 0
    };
}
function buildEmptyMetrics() {
    return {
        expectedCounts: {
            tracking_sessions: 0,
            tracking_events: 0,
            shopify_customers: 0,
            shopify_orders: 0
        },
        processedCounts: {
            tracking_sessions: 0,
            tracking_events: 0,
            shopify_customers: 0,
            shopify_orders: 0
        },
        outcomeCounts: {
            tracking_sessions: buildEmptyOutcomeCount(),
            tracking_events: buildEmptyOutcomeCount(),
            shopify_customers: buildEmptyOutcomeCount(),
            shopify_orders: buildEmptyOutcomeCount()
        },
        ingestionMetrics: {
            processedNodes: 0,
            attachedNodes: 0,
            rehomedNodes: 0,
            quarantinedNodes: 0
        }
    };
}
function buildEmptyCheckpoints() {
    return {
        tracking_sessions: {
            lastTimestamp: null,
            lastCursor: null,
            completed: false
        },
        tracking_events: {
            lastTimestamp: null,
            lastCursor: null,
            completed: false
        },
        shopify_customers: {
            lastTimestamp: null,
            lastCursor: null,
            completed: false
        },
        shopify_orders: {
            lastTimestamp: null,
            lastCursor: null,
            completed: false
        }
    };
}
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
function normalizeDateInput(value, fieldName) {
    if (value == null) {
        return null;
    }
    const normalized = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(normalized.getTime())) {
        throw new Error(`Invalid ${fieldName} value`);
    }
    return normalized.toISOString();
}
function normalizeSourceList(sources) {
    if (!sources || sources.length === 0) {
        return [...IDENTITY_BACKFILL_SOURCES];
    }
    const requestedSources = [...new Set(sources)];
    for (const source of requestedSources) {
        if (!IDENTITY_BACKFILL_SOURCES.includes(source)) {
            throw new Error(`Unsupported identity graph backfill source: ${source}`);
        }
    }
    return IDENTITY_BACKFILL_SOURCES.filter((source) => requestedSources.includes(source));
}
function normalizeBatchSize(batchSize) {
    const normalized = Math.floor(batchSize ?? DEFAULT_IDENTITY_GRAPH_BACKFILL_BATCH_SIZE);
    if (!Number.isFinite(normalized) || normalized <= 0) {
        throw new Error('Identity graph backfill batch size must be a positive integer');
    }
    return normalized;
}
function normalizeBackfillScope(options) {
    const startAt = normalizeDateInput(options.startAt, 'startAt');
    const endAt = normalizeDateInput(options.endAt, 'endAt');
    if (startAt && endAt && startAt > endAt) {
        throw new Error('Identity graph backfill startAt must be before or equal to endAt');
    }
    return {
        startAt,
        endAt,
        batchSize: normalizeBatchSize(options.batchSize),
        sources: normalizeSourceList(options.sources)
    };
}
function isRecord(value) {
    return typeof value === 'object' && value !== null;
}
function parseOutcomeCount(value) {
    const record = isRecord(value) ? value : {};
    return {
        linked: Number(record.linked ?? 0),
        skipped: Number(record.skipped ?? 0),
        conflict: Number(record.conflict ?? 0),
        deduplicated: Number(record.deduplicated ?? 0)
    };
}
function parseCheckpoints(value) {
    const defaults = buildEmptyCheckpoints();
    const record = isRecord(value) ? value : {};
    for (const source of IDENTITY_BACKFILL_SOURCES) {
        const sourceValue = isRecord(record[source]) ? record[source] : {};
        defaults[source] = {
            lastTimestamp: typeof sourceValue.lastTimestamp === 'string' ? sourceValue.lastTimestamp : null,
            lastCursor: typeof sourceValue.lastCursor === 'string' ? sourceValue.lastCursor : null,
            completed: sourceValue.completed === true
        };
    }
    return defaults;
}
function parseMetrics(value) {
    const defaults = buildEmptyMetrics();
    const record = isRecord(value) ? value : {};
    const expectedCounts = isRecord(record.expectedCounts) ? record.expectedCounts : {};
    const processedCounts = isRecord(record.processedCounts) ? record.processedCounts : {};
    const outcomeCounts = isRecord(record.outcomeCounts) ? record.outcomeCounts : {};
    const ingestionMetrics = isRecord(record.ingestionMetrics) ? record.ingestionMetrics : {};
    for (const source of IDENTITY_BACKFILL_SOURCES) {
        defaults.expectedCounts[source] = Number(expectedCounts[source] ?? 0);
        defaults.processedCounts[source] = Number(processedCounts[source] ?? 0);
        defaults.outcomeCounts[source] = parseOutcomeCount(outcomeCounts[source]);
    }
    defaults.ingestionMetrics = {
        processedNodes: Number(ingestionMetrics.processedNodes ?? 0),
        attachedNodes: Number(ingestionMetrics.attachedNodes ?? 0),
        rehomedNodes: Number(ingestionMetrics.rehomedNodes ?? 0),
        quarantinedNodes: Number(ingestionMetrics.quarantinedNodes ?? 0)
    };
    return defaults;
}
function buildReconciliation(metrics) {
    const sources = {};
    let matches = true;
    for (const source of IDENTITY_BACKFILL_SOURCES) {
        const expected = metrics.expectedCounts[source];
        const processed = metrics.processedCounts[source];
        const remaining = Math.max(0, expected - processed);
        const sourceMatches = expected === processed;
        matches &&= sourceMatches;
        sources[source] = {
            expected,
            processed,
            remaining,
            matches: sourceMatches
        };
    }
    return {
        matches,
        sources
    };
}
function buildReportFromState(input) {
    return {
        runId: input.runId,
        status: input.status,
        requestedBy: input.requestedBy,
        workerId: input.workerId,
        scope: input.scope,
        checkpoints: input.checkpoints,
        metrics: input.metrics,
        reconciliation: buildReconciliation(input.metrics),
        startedAt: input.startedAt,
        completedAt: input.completedAt
    };
}
function mapRunRow(row) {
    const optionsRecord = isRecord(row.options) ? row.options : {};
    const scope = normalizeBackfillScope({
        requestedBy: row.requested_by,
        workerId: row.worker_id ?? 'identity-graph-backfill',
        startAt: typeof optionsRecord.startAt === 'string' ? optionsRecord.startAt : null,
        endAt: typeof optionsRecord.endAt === 'string' ? optionsRecord.endAt : null,
        batchSize: Number(optionsRecord.batchSize ?? DEFAULT_IDENTITY_GRAPH_BACKFILL_BATCH_SIZE),
        sources: Array.isArray(optionsRecord.sources)
            ? optionsRecord.sources.filter((value) => typeof value === 'string')
            : undefined
    });
    const checkpoints = parseCheckpoints(row.checkpoints);
    const metrics = parseMetrics(row.metrics);
    return {
        runId: row.id,
        status: row.status,
        requestedBy: row.requested_by,
        workerId: row.worker_id ?? 'identity-graph-backfill',
        scope,
        checkpoints,
        metrics,
        report: isRecord(row.report) ? row.report : null,
        startedAt: row.started_at.toISOString(),
        completedAt: row.completed_at?.toISOString() ?? null
    };
}
async function createIdentityGraphBackfillRun(options) {
    const runId = (0, node_crypto_1.randomUUID)();
    await (0, pool_js_1.query)(`
      INSERT INTO identity_graph_backfill_runs (
        id,
        status,
        requested_by,
        worker_id,
        options,
        checkpoints,
        metrics,
        started_at,
        last_heartbeat_at,
        created_at,
        updated_at
      )
      VALUES (
        $1::uuid,
        'processing',
        $2,
        $3,
        $4::jsonb,
        $5::jsonb,
        $6::jsonb,
        now(),
        now(),
        now(),
        now()
      )
    `, [
        runId,
        options.requestedBy,
        options.workerId,
        JSON.stringify(options.scope),
        JSON.stringify(buildEmptyCheckpoints()),
        JSON.stringify(buildEmptyMetrics())
    ]);
    return runId;
}
async function fetchIdentityGraphBackfillRunRow(runId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        id::text AS id,
        status,
        requested_by,
        worker_id,
        options,
        checkpoints,
        metrics,
        reconciliation,
        report,
        error_code,
        error_message,
        started_at,
        completed_at
      FROM identity_graph_backfill_runs
      WHERE id = $1::uuid
      LIMIT 1
    `, [runId]);
    return result.rows[0] ?? null;
}
async function getIdentityGraphBackfillRun(runId) {
    const row = await fetchIdentityGraphBackfillRunRow(runId);
    if (!row) {
        return null;
    }
    const mapped = mapRunRow(row);
    return (mapped.report ??
        buildReportFromState({
            runId: mapped.runId,
            status: mapped.status,
            requestedBy: mapped.requestedBy,
            workerId: mapped.workerId,
            scope: mapped.scope,
            checkpoints: mapped.checkpoints,
            metrics: mapped.metrics,
            startedAt: mapped.startedAt,
            completedAt: mapped.completedAt
        }));
}
async function updateIdentityGraphBackfillRunProgress(input) {
    await (0, pool_js_1.query)(`
      UPDATE identity_graph_backfill_runs
      SET
        status = 'processing',
        worker_id = $2,
        checkpoints = $3::jsonb,
        metrics = $4::jsonb,
        reconciliation = $5::jsonb,
        last_heartbeat_at = now(),
        updated_at = now()
      WHERE id = $1::uuid
    `, [
        input.runId,
        input.workerId,
        JSON.stringify(input.checkpoints),
        JSON.stringify(input.metrics),
        JSON.stringify(buildReconciliation(input.metrics))
    ]);
}
async function completeIdentityGraphBackfillRun(input) {
    await (0, pool_js_1.query)(`
      UPDATE identity_graph_backfill_runs
      SET
        status = 'completed',
        worker_id = $2,
        checkpoints = $3::jsonb,
        metrics = $4::jsonb,
        reconciliation = $5::jsonb,
        report = $6::jsonb,
        completed_at = now(),
        last_heartbeat_at = now(),
        updated_at = now(),
        error_code = NULL,
        error_message = NULL
      WHERE id = $1::uuid
    `, [
        input.runId,
        input.workerId,
        JSON.stringify(input.report.checkpoints),
        JSON.stringify(input.report.metrics),
        JSON.stringify(input.report.reconciliation),
        JSON.stringify(input.report)
    ]);
}
function normalizeErrorCode(error) {
    if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
        return error.code.trim();
    }
    if (error instanceof Error && error.name.trim()) {
        return error.name.trim();
    }
    return 'identity_graph_backfill_failed';
}
function normalizeErrorMessage(error) {
    if (error instanceof Error && error.message.trim()) {
        return error.message.trim();
    }
    if (typeof error === 'string' && error.trim()) {
        return error.trim();
    }
    return 'Identity graph backfill failed';
}
async function failIdentityGraphBackfillRun(input) {
    await (0, pool_js_1.query)(`
      UPDATE identity_graph_backfill_runs
      SET
        status = 'failed',
        worker_id = $2,
        checkpoints = $3::jsonb,
        metrics = $4::jsonb,
        reconciliation = $5::jsonb,
        completed_at = now(),
        last_heartbeat_at = now(),
        updated_at = now(),
        error_code = $6,
        error_message = $7
      WHERE id = $1::uuid
    `, [
        input.runId,
        input.workerId,
        JSON.stringify(input.checkpoints),
        JSON.stringify(input.metrics),
        JSON.stringify(buildReconciliation(input.metrics)),
        normalizeErrorCode(input.error),
        normalizeErrorMessage(input.error)
    ]);
}
async function hydrateExpectedCounts(scope, metrics) {
    for (const source of scope.sources) {
        metrics.expectedCounts[source] = await countSourceRows(source, scope);
    }
}
async function countSourceRows(source, scope) {
    switch (source) {
        case 'tracking_sessions': {
            const result = await (0, pool_js_1.query)(`
          SELECT COUNT(*)::text AS row_count
          FROM tracking_sessions
          WHERE ($1::timestamptz IS NULL OR first_seen_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR first_seen_at <= $2::timestamptz)
        `, [scope.startAt, scope.endAt]);
            return Number(result.rows[0]?.row_count ?? '0');
        }
        case 'tracking_events': {
            const result = await (0, pool_js_1.query)(`
          SELECT COUNT(*)::text AS row_count
          FROM tracking_events
          WHERE ($1::timestamptz IS NULL OR occurred_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR occurred_at <= $2::timestamptz)
        `, [scope.startAt, scope.endAt]);
            return Number(result.rows[0]?.row_count ?? '0');
        }
        case 'shopify_customers': {
            const result = await (0, pool_js_1.query)(`
          SELECT COUNT(*)::text AS row_count
          FROM shopify_customers
          WHERE ($1::timestamptz IS NULL OR created_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR created_at <= $2::timestamptz)
        `, [scope.startAt, scope.endAt]);
            return Number(result.rows[0]?.row_count ?? '0');
        }
        case 'shopify_orders': {
            const result = await (0, pool_js_1.query)(`
          SELECT COUNT(*)::text AS row_count
          FROM shopify_orders
          WHERE ($1::timestamptz IS NULL OR COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR COALESCE(processed_at, created_at_shopify, ingested_at) <= $2::timestamptz)
        `, [scope.startAt, scope.endAt]);
            return Number(result.rows[0]?.row_count ?? '0');
        }
    }
}
async function fetchSourceBatch(source, scope, checkpoint) {
    switch (source) {
        case 'tracking_sessions': {
            const result = await (0, pool_js_1.query)(`
          SELECT
            id::text AS session_id,
            first_seen_at AS source_timestamp
          FROM tracking_sessions
          WHERE ($1::timestamptz IS NULL OR first_seen_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR first_seen_at <= $2::timestamptz)
            AND (
              $3::timestamptz IS NULL
              OR first_seen_at > $3::timestamptz
              OR (first_seen_at = $3::timestamptz AND id::text > $4)
            )
          ORDER BY first_seen_at ASC, id ASC
          LIMIT $5
        `, [scope.startAt, scope.endAt, checkpoint.lastTimestamp, checkpoint.lastCursor, scope.batchSize]);
            return result.rows.map((row) => ({
                source,
                cursorKey: row.session_id,
                ...row
            }));
        }
        case 'tracking_events': {
            const result = await (0, pool_js_1.query)(`
          SELECT
            id::text AS event_id,
            session_id::text AS session_id,
            shopify_checkout_token AS checkout_token,
            shopify_cart_token AS cart_token,
            occurred_at AS source_timestamp
          FROM tracking_events
          WHERE ($1::timestamptz IS NULL OR occurred_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR occurred_at <= $2::timestamptz)
            AND (
              $3::timestamptz IS NULL
              OR occurred_at > $3::timestamptz
              OR (occurred_at = $3::timestamptz AND id::text > $4)
            )
          ORDER BY occurred_at ASC, id ASC
          LIMIT $5
        `, [scope.startAt, scope.endAt, checkpoint.lastTimestamp, checkpoint.lastCursor, scope.batchSize]);
            return result.rows.map((row) => ({
                source,
                cursorKey: row.event_id,
                ...row
            }));
        }
        case 'shopify_customers': {
            const result = await (0, pool_js_1.query)(`
          SELECT
            id::text AS row_id,
            shopify_customer_id,
            email_hash,
            phone_hash,
            created_at AS source_timestamp
          FROM shopify_customers
          WHERE ($1::timestamptz IS NULL OR created_at >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR created_at <= $2::timestamptz)
            AND (
              $3::timestamptz IS NULL
              OR created_at > $3::timestamptz
              OR (created_at = $3::timestamptz AND id > $4::bigint)
            )
          ORDER BY created_at ASC, id ASC
          LIMIT $5
        `, [scope.startAt, scope.endAt, checkpoint.lastTimestamp, checkpoint.lastCursor ?? '0', scope.batchSize]);
            return result.rows.map((row) => ({
                source,
                cursorKey: row.row_id,
                ...row
            }));
        }
        case 'shopify_orders': {
            const result = await (0, pool_js_1.query)(`
          SELECT
            id::text AS row_id,
            shopify_order_id,
          landing_session_id::text AS landing_session_id,
          checkout_token,
          cart_token,
          shopify_customer_id,
          email_hash,
          phone_hash,
          COALESCE(processed_at, created_at_shopify, ingested_at) AS source_timestamp
          FROM shopify_orders
          WHERE ($1::timestamptz IS NULL OR COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz)
            AND ($2::timestamptz IS NULL OR COALESCE(processed_at, created_at_shopify, ingested_at) <= $2::timestamptz)
            AND (
              $3::timestamptz IS NULL
              OR COALESCE(processed_at, created_at_shopify, ingested_at) > $3::timestamptz
              OR (COALESCE(processed_at, created_at_shopify, ingested_at) = $3::timestamptz AND id > $4::bigint)
            )
          ORDER BY COALESCE(processed_at, created_at_shopify, ingested_at) ASC, id ASC
          LIMIT $5
        `, [scope.startAt, scope.endAt, checkpoint.lastTimestamp, checkpoint.lastCursor ?? '0', scope.batchSize]);
            return result.rows.map((row) => ({
                source,
                cursorKey: row.row_id,
                ...row
            }));
        }
    }
}
async function processSourceRow(row) {
    return (0, pool_js_1.withTransaction)(async (client) => {
        switch (row.source) {
            case 'tracking_sessions': {
                const result = await (0, index_js_2.ingestIdentityEdges)(client, {
                    sourceTimestamp: row.source_timestamp,
                    evidenceSource: 'backfill',
                    sourceTable: 'tracking_sessions',
                    sourceRecordId: row.session_id,
                    idempotencyKey: `identity_graph_backfill:tracking_sessions:${row.session_id}`,
                    sessionId: row.session_id
                });
                return {
                    outcome: result.outcome,
                    deduplicated: result.deduplicated,
                    metrics: result.metrics
                };
            }
            case 'tracking_events': {
                const result = await (0, index_js_2.ingestIdentityEdges)(client, {
                    sourceTimestamp: row.source_timestamp,
                    evidenceSource: 'backfill',
                    sourceTable: 'tracking_events',
                    sourceRecordId: row.event_id,
                    idempotencyKey: `identity_graph_backfill:tracking_events:${row.event_id}`,
                    sessionId: row.session_id,
                    checkoutToken: row.checkout_token,
                    cartToken: row.cart_token
                });
                return {
                    outcome: result.outcome,
                    deduplicated: result.deduplicated,
                    metrics: result.metrics
                };
            }
            case 'shopify_customers': {
                const result = await (0, index_js_2.ingestIdentityEdges)(client, {
                    sourceTimestamp: row.source_timestamp,
                    evidenceSource: 'backfill',
                    sourceTable: 'shopify_customers',
                    sourceRecordId: row.shopify_customer_id ?? row.row_id,
                    idempotencyKey: `identity_graph_backfill:shopify_customers:${row.row_id}`,
                    shopifyCustomerId: row.shopify_customer_id,
                    hashedEmail: normalizeNullableString(row.email_hash),
                    phoneHash: normalizeNullableString(row.phone_hash)
                });
                return {
                    outcome: result.outcome,
                    deduplicated: result.deduplicated,
                    metrics: result.metrics
                };
            }
            case 'shopify_orders': {
                const result = await (0, index_js_2.ingestIdentityEdges)(client, {
                    sourceTimestamp: row.source_timestamp,
                    evidenceSource: 'backfill',
                    sourceTable: 'shopify_orders',
                    sourceRecordId: row.shopify_order_id,
                    idempotencyKey: `identity_graph_backfill:shopify_orders:${row.row_id}`,
                    sessionId: row.landing_session_id,
                    checkoutToken: row.checkout_token,
                    cartToken: row.cart_token,
                    shopifyCustomerId: row.shopify_customer_id,
                    hashedEmail: normalizeNullableString(row.email_hash),
                    phoneHash: normalizeNullableString(row.phone_hash)
                });
                return {
                    outcome: result.outcome,
                    deduplicated: result.deduplicated,
                    metrics: result.metrics
                };
            }
        }
    });
}
function accumulateRowMetrics(metrics, source, rowResult) {
    metrics.processedCounts[source] += 1;
    metrics.outcomeCounts[source][rowResult.outcome] += 1;
    if (rowResult.deduplicated) {
        metrics.outcomeCounts[source].deduplicated += 1;
    }
    metrics.ingestionMetrics.processedNodes += rowResult.metrics.processedNodes;
    metrics.ingestionMetrics.attachedNodes += rowResult.metrics.attachedNodes;
    metrics.ingestionMetrics.rehomedNodes += rowResult.metrics.rehomedNodes;
    metrics.ingestionMetrics.quarantinedNodes += rowResult.metrics.quarantinedNodes;
}
async function backfillHistoricalIdentityGraph(options) {
    const requestedBy = normalizeNullableString(options.requestedBy);
    const workerId = normalizeNullableString(options.workerId) ?? 'identity-graph-backfill';
    if (!requestedBy) {
        throw new Error('Identity graph backfill requestedBy is required');
    }
    const scope = normalizeBackfillScope(options);
    const runId = normalizeNullableString(options.runId) ?? (await createIdentityGraphBackfillRun({
        requestedBy,
        workerId,
        scope
    }));
    const maxBatches = options.maxBatches == null ? null : Math.max(1, Math.floor(options.maxBatches));
    const runRow = await fetchIdentityGraphBackfillRunRow(runId);
    if (!runRow) {
        throw new Error(`Identity graph backfill run ${runId} was not found`);
    }
    const runState = mapRunRow(runRow);
    if (runState.status === 'completed' && runState.report) {
        return runState.report;
    }
    const checkpoints = runState.checkpoints;
    const metrics = runState.metrics;
    let processedBatches = 0;
    if (Object.values(metrics.expectedCounts).every((value) => value === 0)) {
        await hydrateExpectedCounts(runState.scope, metrics);
        await updateIdentityGraphBackfillRunProgress({
            runId,
            workerId,
            checkpoints,
            metrics
        });
    }
    (0, index_js_1.logInfo)('identity_graph_backfill_started', {
        runId,
        requestedBy,
        workerId,
        scope: runState.scope
    });
    try {
        for (const source of runState.scope.sources) {
            const checkpoint = checkpoints[source];
            while (!checkpoint.completed) {
                const rows = await fetchSourceBatch(source, runState.scope, checkpoint);
                if (rows.length === 0) {
                    checkpoint.completed = true;
                    await updateIdentityGraphBackfillRunProgress({
                        runId,
                        workerId,
                        checkpoints,
                        metrics
                    });
                    break;
                }
                for (const row of rows) {
                    const rowResult = await processSourceRow(row);
                    accumulateRowMetrics(metrics, source, rowResult);
                }
                const lastRow = rows[rows.length - 1];
                checkpoint.lastTimestamp = lastRow.source_timestamp.toISOString();
                checkpoint.lastCursor = lastRow.cursorKey;
                processedBatches += 1;
                await updateIdentityGraphBackfillRunProgress({
                    runId,
                    workerId,
                    checkpoints,
                    metrics
                });
                if (maxBatches !== null && processedBatches >= maxBatches) {
                    return buildReportFromState({
                        runId,
                        status: 'processing',
                        requestedBy,
                        workerId,
                        scope: runState.scope,
                        checkpoints,
                        metrics,
                        startedAt: runState.startedAt,
                        completedAt: null
                    });
                }
            }
        }
        const report = buildReportFromState({
            runId,
            status: 'completed',
            requestedBy,
            workerId,
            scope: runState.scope,
            checkpoints,
            metrics,
            startedAt: runState.startedAt,
            completedAt: new Date().toISOString()
        });
        await completeIdentityGraphBackfillRun({
            runId,
            workerId,
            report
        });
        (0, index_js_1.logInfo)('identity_graph_backfill_completed', {
            runId,
            workerId,
            reconciliationMatches: report.reconciliation.matches,
            metrics: report.metrics
        });
        return report;
    }
    catch (error) {
        await failIdentityGraphBackfillRun({
            runId,
            workerId,
            checkpoints,
            metrics,
            error
        });
        (0, index_js_1.logError)('identity_graph_backfill_failed', error, {
            runId,
            workerId,
            checkpoints,
            metrics
        });
        throw error;
    }
}
