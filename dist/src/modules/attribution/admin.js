import { Router as createRouter } from "express";
import { z } from "zod";
import { normalizeOrderAttributionBackfillRequest, } from "../../../packages/attribution-schema/index.js";
import { query } from "../../db/pool.js";
import { emitOrderAttributionBackfillJobLifecycleLog } from "../../observability/index.js";
import { attachAuthContext, requireAdmin, } from "../auth/index.js";
import { enqueueOrderAttributionBackfillRun, getOrderAttributionBackfillRun, } from "./backfill-run-store.js";
import { fetchGa4FallbackShadowReport, getGa4FallbackRolloutMode, } from "./ga4-rollout.js";
class AttributionAdminHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = "AttributionAdminHttpError";
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
function parseBackfillRequest(input) {
    try {
        return normalizeOrderAttributionBackfillRequest(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new AttributionAdminHttpError(400, "invalid_request", "Invalid order attribution backfill request", error.flatten());
        }
        throw error;
    }
}
const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const ga4ShadowReportQuerySchema = z
    .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
})
    .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: "startDate must be on or before endDate",
            path: ["startDate"],
        });
    }
});
function parseShadowReportQuery(input) {
    try {
        return ga4ShadowReportQuerySchema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new AttributionAdminHttpError(400, "invalid_request", "Invalid GA4 fallback shadow report request", error.flatten());
        }
        throw error;
    }
}
function getSubmittedBy(auth) {
    if (!auth) {
        throw new AttributionAdminHttpError(401, "unauthorized", "Authentication required");
    }
    if (auth.kind === "internal") {
        return "internal";
    }
    return auth.user.email;
}
async function loadOrderAttributionBackfillRun(jobId) {
    const row = await getOrderAttributionBackfillRun(jobId);
    if (!row) {
        throw new AttributionAdminHttpError(404, "backfill_job_not_found", "Order attribution backfill job was not found");
    }
    return row;
}
export function createAttributionAdminRouter() {
    const router = createRouter();
    const queryExecutor = { query };
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.post("/orders/backfill", async (req, res, next) => {
        try {
            const auth = res.locals.auth;
            const options = parseBackfillRequest(req.body ?? {});
            const response = await enqueueOrderAttributionBackfillRun(options, getSubmittedBy(auth));
            emitOrderAttributionBackfillJobLifecycleLog({
                stage: "enqueued",
                jobId: response.jobId,
                submittedAt: response.submittedAt,
                options: response.options,
            });
            res.status(202).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get("/orders/backfill/:jobId", async (req, res, next) => {
        try {
            const response = await loadOrderAttributionBackfillRun(req.params.jobId);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get("/ga4-fallback/shadow-report", async (req, res, next) => {
        try {
            const input = parseShadowReportQuery(req.query);
            const report = await fetchGa4FallbackShadowReport(queryExecutor, input);
            res.status(200).json({
                ...report,
                rolloutMode: getGa4FallbackRolloutMode(),
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
