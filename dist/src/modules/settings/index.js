"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_REPORTING_TIMEZONE = void 0;
exports.getReportingTimezone = getReportingTimezone;
exports.formatDateInTimezone = formatDateInTimezone;
exports.createSettingsRouter = createSettingsRouter;
const express_1 = require("express");
const zod_1 = require("zod");
const pool_js_1 = require("../../db/pool.js");
const index_js_1 = require("../auth/index.js");
const aggregates_js_1 = require("../reporting/aggregates.js");
const REPORTING_TIMEZONE_ALIASES = {
    PST: 'America/Los_Angeles',
    PDT: 'America/Los_Angeles',
    PT: 'America/Los_Angeles'
};
exports.DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';
class SettingsHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'SettingsHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
function normalizeReportingTimezone(input) {
    const trimmed = input.trim();
    const aliased = REPORTING_TIMEZONE_ALIASES[trimmed.toUpperCase()] ?? trimmed;
    try {
        return new Intl.DateTimeFormat('en-US', { timeZone: aliased }).resolvedOptions().timeZone;
    }
    catch {
        throw new SettingsHttpError(400, 'invalid_timezone', 'Timezone must be a valid IANA timezone or supported alias');
    }
}
const updateSettingsSchema = zod_1.z.object({
    reportingTimezone: zod_1.z.string().min(1).transform(normalizeReportingTimezone)
});
function executeQuery(executor, sql, params) {
    if (typeof executor === 'function') {
        return executor(sql, params);
    }
    return executor.query(sql, params);
}
async function ensureAppSettingsRow(executor) {
    await executeQuery(executor, `
      INSERT INTO app_settings (singleton, reporting_timezone)
      VALUES (true, $1)
      ON CONFLICT (singleton) DO NOTHING
    `, [exports.DEFAULT_REPORTING_TIMEZONE]);
}
async function fetchAppSettingsRow(executor) {
    await ensureAppSettingsRow(executor);
    const result = await executeQuery(executor, `
      SELECT reporting_timezone, updated_at
      FROM app_settings
      WHERE singleton = true
      LIMIT 1
    `);
    if (!result.rowCount) {
        return {
            reporting_timezone: exports.DEFAULT_REPORTING_TIMEZONE,
            updated_at: new Date()
        };
    }
    return result.rows[0];
}
async function getReportingTimezone(executor = pool_js_1.query) {
    const row = await fetchAppSettingsRow(executor);
    return normalizeReportingTimezone(row.reporting_timezone || exports.DEFAULT_REPORTING_TIMEZONE);
}
function formatDateInTimezone(date, reportingTimezone) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: reportingTimezone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    });
    const parts = formatter.formatToParts(date);
    const year = parts.find((part) => part.type === 'year')?.value;
    const month = parts.find((part) => part.type === 'month')?.value;
    const day = parts.find((part) => part.type === 'day')?.value;
    if (!year || !month || !day) {
        throw new Error('Failed to format date for reporting timezone');
    }
    return `${year}-${month}-${day}`;
}
function serializeSettings(row) {
    return {
        reportingTimezone: normalizeReportingTimezone(row.reporting_timezone || exports.DEFAULT_REPORTING_TIMEZONE),
        updatedAt: row.updated_at.toISOString()
    };
}
function createSettingsRouter() {
    const router = (0, express_1.Router)();
    router.use(index_js_1.attachAuthContext);
    router.use(index_js_1.requireAuthenticated);
    router.get('/', async (_req, res, next) => {
        try {
            const settings = await fetchAppSettingsRow(pool_js_1.query);
            res.json(serializeSettings(settings));
        }
        catch (error) {
            next(error);
        }
    });
    router.put('/', index_js_1.requireAdmin, async (req, res, next) => {
        try {
            const input = updateSettingsSchema.parse(req.body ?? {});
            const settings = await (0, pool_js_1.withTransaction)(async (client) => {
                await ensureAppSettingsRow(client);
                const result = await client.query(`
            UPDATE app_settings
            SET
              reporting_timezone = $1,
              updated_at = now()
            WHERE singleton = true
            RETURNING reporting_timezone, updated_at
          `, [input.reportingTimezone]);
                await (0, aggregates_js_1.refreshAllDailyReportingMetrics)(client);
                return result.rows[0];
            });
            res.json({
                ok: true,
                settings: serializeSettings(settings)
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
