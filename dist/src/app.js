"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createApp = createApp;
const express_1 = __importDefault(require("express"));
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/auth/index.js");
const admin_js_1 = require("./modules/attribution/admin.js");
const ga4_bigquery_config_js_1 = require("./modules/attribution/ga4-bigquery-config.js");
const index_js_2 = require("./modules/google-ads/index.js");
const index_js_3 = require("./modules/meta-ads/index.js");
const index_js_4 = require("./modules/reporting/index.js");
const index_js_5 = require("./modules/settings/index.js");
const index_js_6 = require("./modules/shopify/index.js");
const index_js_7 = require("./modules/tracking/index.js");
const admin_js_2 = require("./modules/identity/admin.js");
const read_api_js_1 = require("./modules/identity/read-api.js");
const index_js_8 = require("./observability/index.js");
function createApp() {
    (0, ga4_bigquery_config_js_1.assertGa4BigQueryIngestionConfig)();
    const app = (0, express_1.default)();
    const serviceName = process.env.K_SERVICE ?? 'roas-radar-api';
    app.disable('x-powered-by');
    app.use((0, index_js_8.createRequestLoggingMiddleware)(serviceName));
    app.use((req, res, next) => {
        const origin = req.header('origin');
        const allowedOrigins = (0, env_js_1.getApiAllowedOrigins)();
        const isAllowedOrigin = origin ? allowedOrigins.includes(origin) : false;
        if (origin) {
            res.append('Vary', 'Origin');
        }
        if (isAllowedOrigin && origin) {
            res.setHeader('Access-Control-Allow-Origin', origin);
            res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
            res.setHeader('Access-Control-Allow-Headers', 'authorization,content-type,x-roas-radar-tenant-id');
        }
        if (origin && !isAllowedOrigin) {
            if (req.method === 'OPTIONS') {
                res.status(403).json({
                    error: 'origin_not_allowed',
                    message: 'Request origin is not allowed'
                });
                return;
            }
            next();
            return;
        }
        if (req.method === 'OPTIONS') {
            res.status(204).end();
            return;
        }
        next();
    });
    app.get('/healthz', (_req, res) => {
        res.status(200).json({ ok: true });
    });
    app.get('/readyz', async (_req, res) => {
        try {
            const status = await (0, pool_js_1.checkDatabaseHealth)();
            res.status(200).json(status);
        }
        catch (error) {
            res.status(503).json({
                ok: false,
                error: error instanceof Error ? error.message : String(error)
            });
        }
    });
    app.use('/webhooks/shopify', express_1.default.raw({ type: '*/*', limit: env_js_1.env.SHOPIFY_WEBHOOK_BODY_LIMIT }), (0, index_js_6.createShopifyWebhookRouter)());
    app.use('/track', express_1.default.text({ type: 'text/plain', limit: env_js_1.env.TRACKING_BODY_LIMIT }), express_1.default.json({ type: 'application/json', limit: env_js_1.env.TRACKING_BODY_LIMIT }), (0, index_js_7.createTrackingRouter)());
    app.use(express_1.default.json({ limit: env_js_1.env.API_JSON_BODY_LIMIT }));
    app.use('/api/auth', (0, index_js_1.createAuthRouter)());
    app.use('/api/settings', (0, index_js_5.createSettingsRouter)());
    app.use('/api/reporting', (0, index_js_4.createReportingRouter)());
    app.use('/api/internal/identity', (0, read_api_js_1.createInternalIdentityRouter)());
    app.use('/api/admin/identity', (0, admin_js_2.createIdentityAdminRouter)());
    app.use('/api/admin/users', (0, index_js_1.createUserAdminRouter)());
    app.use('/api/admin/attribution', (0, admin_js_1.createAttributionAdminRouter)());
    app.use('/shopify', (0, index_js_6.createShopifyPublicRouter)());
    app.use('/api/admin/shopify', (0, index_js_6.createShopifyAdminRouter)());
    app.use('/meta-ads', (0, index_js_3.createMetaAdsPublicRouter)());
    app.use('/api/admin/meta-ads', (0, index_js_3.createMetaAdsAdminRouter)());
    app.use('/google-ads', (0, index_js_2.createGoogleAdsPublicRouter)());
    app.use('/api/admin/google-ads', (0, index_js_2.createGoogleAdsAdminRouter)());
    app.use((error, _req, res, _next) => {
        const statusCode = typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number'
            ? error.statusCode
            : typeof error === 'object' &&
                error !== null &&
                'type' in error &&
                error.type === 'entity.too.large'
                ? 413
                : 500;
        const code = typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string'
            ? error.code
            : typeof error === 'object' &&
                error !== null &&
                'type' in error &&
                error.type === 'entity.too.large'
                ? 'payload_too_large'
                : 'internal_server_error';
        const message = error instanceof Error ? error.message : 'Unexpected error';
        const details = typeof error === 'object' && error !== null && 'details' in error ? error.details : undefined;
        if (statusCode >= 500) {
            (0, index_js_8.logHttpError)('http_request_failed', error, _req, {
                responseStatusCode: statusCode
            });
        }
        res.status(statusCode).json({
            error: code,
            message,
            ...(details === undefined ? {} : { details })
        });
    });
    return app;
}
