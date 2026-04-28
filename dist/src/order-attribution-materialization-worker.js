"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveOrderAttributionMaterializationExecution = resolveOrderAttributionMaterializationExecution;
const node_crypto_1 = require("node:crypto");
const node_url_1 = require("node:url");
const pool_js_1 = require("./db/pool.js");
const backfill_js_1 = require("./modules/attribution/backfill.js");
const ga4_bigquery_config_js_1 = require("./modules/attribution/ga4-bigquery-config.js");
const index_js_1 = require("./observability/index.js");
function parseOptionalInteger(name) {
    const value = process.env[name]?.trim();
    if (!value) {
        return undefined;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return parsed;
}
function parseBoolean(name, defaultValue) {
    const value = process.env[name]?.trim().toLowerCase();
    if (!value) {
        return defaultValue;
    }
    return ['1', 'true', 'yes', 'on'].includes(value);
}
function startOfUtcDay(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
}
function endOfUtcDay(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999));
}
function resolveOrderAttributionMaterializationExecution(now) {
    const requestedBy = process.env.ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY?.trim() ||
        'cloud-run-scheduler';
    const workerId = process.env.ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID?.trim() ||
        process.env.K_JOB_EXECUTION?.trim() ||
        `order-attribution-materialization-${(0, node_crypto_1.randomUUID)()}`;
    const lookbackDays = parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LOOKBACK_DAYS') ?? 2;
    const lagDays = parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LAG_DAYS') ?? 1;
    const anchorDate = new Date(now.getTime() - lagDays * 24 * 60 * 60 * 1000);
    const windowEnd = endOfUtcDay(anchorDate);
    const windowStart = startOfUtcDay(new Date(windowEnd.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000));
    const dryRun = parseBoolean('ORDER_ATTRIBUTION_MATERIALIZATION_DRY_RUN', false);
    const onlyWebOrders = parseBoolean('ORDER_ATTRIBUTION_MATERIALIZATION_ONLY_WEB_ORDERS', true);
    const writeToShopifyWhenAvailable = !parseBoolean('ORDER_ATTRIBUTION_MATERIALIZATION_SKIP_SHOPIFY_WRITEBACK', false);
    return {
        requestedBy,
        workerId,
        windowStart,
        windowEnd,
        limit: parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LIMIT'),
        dryRun,
        onlyWebOrders,
        writeToShopifyWhenAvailable
    };
}
async function run() {
    (0, ga4_bigquery_config_js_1.assertGa4BigQueryIngestionConfig)();
    const execution = resolveOrderAttributionMaterializationExecution(new Date());
    (0, index_js_1.logInfo)('order_attribution_materialization_worker_started', {
        workerId: execution.workerId,
        requestedBy: execution.requestedBy,
        windowStart: execution.windowStart.toISOString(),
        windowEnd: execution.windowEnd.toISOString(),
        dryRun: execution.dryRun,
        onlyWebOrders: execution.onlyWebOrders,
        writeToShopifyWhenAvailable: execution.writeToShopifyWhenAvailable,
        service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-order-attribution-materialization'
    });
    const report = await (0, backfill_js_1.backfillRecentOrdersWithRecoveredAttribution)(execution);
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    await pool_js_1.pool.end();
}
if (process.argv[1] && import.meta.url === (0, node_url_1.pathToFileURL)(process.argv[1]).href) {
    run().catch(async (error) => {
        (0, index_js_1.logError)('order_attribution_materialization_worker_failed', error, {
            service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-order-attribution-materialization'
        });
        await pool_js_1.pool.end().catch(() => undefined);
        process.exit(1);
    });
}
