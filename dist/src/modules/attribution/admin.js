"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createAttributionAdminRouter = createAttributionAdminRouter;
const express_1 = require("express");
const zod_1 = require("zod");
const index_js_1 = require("../../../packages/attribution-schema/index.js");
const pool_js_1 = require("../../db/pool.js");
const index_js_2 = require("../../observability/index.js");
const index_js_3 = require("../auth/index.js");
const ga4_rollout_js_1 = require("./ga4-rollout.js");
const backfill_run_store_js_1 = require("./backfill-run-store.js");
class AttributionAdminHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'AttributionAdminHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
function parseBackfillRequest(input) {
    try {
        return (0, index_js_1.normalizeOrderAttributionBackfillRequest)(input);
    }
    catch (error) {
        if (error instanceof zod_1.z.ZodError) {
            throw new AttributionAdminHttpError(400, 'invalid_request', 'Invalid order attribution backfill request', error.flatten());
        }
        throw error;
    }
}
const dateStringSchema = zod_1.z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const ga4ShadowReportQuerySchema = zod_1.z
    .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema
})
    .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
        ctx.addIssue({
            code: zod_1.z.ZodIssueCode.custom,
            message: 'startDate must be on or before endDate',
            path: ['startDate']
        });
    }
});
function parseShadowReportQuery(input) {
    try {
        return ga4ShadowReportQuerySchema.parse(input);
    }
    catch (error) {
        if (error instanceof zod_1.z.ZodError) {
            throw new AttributionAdminHttpError(400, 'invalid_request', 'Invalid GA4 fallback shadow report request', error.flatten());
        }
        throw error;
    }
}
function getSubmittedBy(auth) {
    if (!auth) {
        throw new AttributionAdminHttpError(401, 'unauthorized', 'Authentication required');
    }
    if (auth.kind === 'internal') {
        return 'internal';
    }
    return auth.user.email;
}
async function loadOrderAttributionBackfillRun(jobId) {
    const row = await (0, backfill_run_store_js_1.getOrderAttributionBackfillRun)(jobId);
    if (!row) {
        throw new AttributionAdminHttpError(404, 'backfill_job_not_found', 'Order attribution backfill job was not found');
    }
    return row;
}
function createAttributionAdminRouter() {
    const router = (0, express_1.Router)();
    const queryExecutor = { query: pool_js_1.query };
    router.use(index_js_3.attachAuthContext);
    router.use(index_js_3.requireAdmin);
    router.post('/orders/backfill', async (req, res, next) => {
        try {
            const auth = res.locals.auth;
            const options = parseBackfillRequest(req.body ?? {});
            const response = await (0, backfill_run_store_js_1.enqueueOrderAttributionBackfillRun)(options, getSubmittedBy(auth));
            (0, index_js_2.emitOrderAttributionBackfillJobLifecycleLog)({
                stage: 'enqueued',
                jobId: response.jobId,
                submittedAt: response.submittedAt,
                options: response.options
            });
            res.status(202).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/orders/backfill/:jobId', async (req, res, next) => {
        try {
            const response = await loadOrderAttributionBackfillRun(req.params.jobId);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/ga4-fallback/shadow-report', async (req, res, next) => {
        try {
            const input = parseShadowReportQuery(req.query);
            const report = await (0, ga4_rollout_js_1.fetchGa4FallbackShadowReport)(queryExecutor, input);
            res.status(200).json({
                ...report,
                rolloutMode: (0, ga4_rollout_js_1.getGa4FallbackRolloutMode)()
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
