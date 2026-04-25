"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runSessionAttributionRetention = runSessionAttributionRetention;
exports.runSessionAttributionRetentionJob = runSessionAttributionRetentionJob;
const env_js_1 = require("../../config/env.js");
const pool_js_1 = require("../../db/pool.js");
function normalizePositiveInteger(value, fallback) {
    if (!Number.isFinite(value)) {
        return fallback;
    }
    return Math.max(Math.trunc(value ?? fallback), 1);
}
function resolveCutoffAt(asOf) {
    const referenceTime = asOf ? new Date(asOf) : new Date();
    referenceTime.setUTCDate(referenceTime.getUTCDate() - env_js_1.env.SESSION_ATTRIBUTION_RETENTION_DAYS);
    return referenceTime;
}
async function execute(client, callback) {
    if (client) {
        return callback(client);
    }
    return (0, pool_js_1.withTransaction)(callback);
}
async function countProtectedRows(client, cutoffAt) {
    const result = await client.query(`...`, [cutoffAt]);
    return result.rows[0] ?? { protected_sessions: '0', protected_touch_events: '0' };
}
async function deleteExpiredTouchEvents(client, cutoffAt, batchSize) {
    const result = await client.query(`...DELETE expired unlinked touch events in batches...`, [cutoffAt, batchSize]);
    return result.rowCount ?? 0;
}
async function deleteExpiredSessions(client, cutoffAt, batchSize) {
    const result = await client.query(`...DELETE expired unlinked session identities in batches...`, [cutoffAt, batchSize]);
    return result.rowCount ?? 0;
}
async function runSessionAttributionRetention(options = {}) {
    // computes cutoff, loops in batches, logs each batch, returns totals
}
async function runSessionAttributionRetentionJob(options = {}) {
    // wraps the job with structured error logging
}
