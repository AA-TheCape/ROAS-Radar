import { env } from '../../config/env.js';
import { withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';
const DEFAULT_RETENTION_BATCH_SIZE = 100;
const DEFAULT_RETENTION_MAX_BATCHES = 50;
function normalizePositiveInteger(value, fallback) {
    if (!Number.isFinite(value)) {
        return fallback;
    }
    return Math.max(Math.trunc(value ?? fallback), 1);
}
function resolveCutoffAt(asOf) {
    const referenceTime = asOf ? new Date(asOf) : new Date();
    referenceTime.setUTCDate(referenceTime.getUTCDate() - env.SESSION_ATTRIBUTION_RETENTION_DAYS);
    return referenceTime;
}
async function execute(client, callback) {
    if (client) {
        return callback(client);
    }
    return withTransaction(callback);
}
async function countProtectedRows(client, cutoffAt) {
    const result = await client.query(`
      SELECT
        (
          SELECT COUNT(*)::text
          FROM session_attribution_identities identities
          WHERE identities.retained_until < $1::timestamptz
            AND EXISTS (
              SELECT 1
              FROM order_attribution_links links
              WHERE links.roas_radar_session_id = identities.roas_radar_session_id
            )
        ) AS protected_sessions,
        (
          SELECT COUNT(*)::text
          FROM session_attribution_touch_events touch_events
          WHERE touch_events.retained_until < $1::timestamptz
            AND EXISTS (
              SELECT 1
              FROM order_attribution_links links
              WHERE links.roas_radar_session_id = touch_events.roas_radar_session_id
            )
        ) AS protected_touch_events
    `, [cutoffAt]);
    return result.rows[0] ?? { protected_sessions: '0', protected_touch_events: '0' };
}
async function deleteExpiredTouchEvents(client, cutoffAt, batchSize) {
    const result = await client.query(`
      WITH expired_touch_events AS (
        SELECT touch_events.id
        FROM session_attribution_touch_events touch_events
        WHERE touch_events.retained_until < $1::timestamptz
          AND NOT EXISTS (
            SELECT 1
            FROM order_attribution_links links
            WHERE links.roas_radar_session_id = touch_events.roas_radar_session_id
          )
        ORDER BY touch_events.retained_until ASC, touch_events.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM session_attribution_touch_events touch_events
      USING expired_touch_events
      WHERE touch_events.id = expired_touch_events.id
    `, [cutoffAt, batchSize]);
    return result.rowCount ?? 0;
}
async function deleteExpiredSessions(client, cutoffAt, batchSize) {
    const result = await client.query(`
      WITH expired_sessions AS (
        SELECT identities.roas_radar_session_id
        FROM session_attribution_identities identities
        WHERE identities.retained_until < $1::timestamptz
          AND NOT EXISTS (
            SELECT 1
            FROM order_attribution_links links
            WHERE links.roas_radar_session_id = identities.roas_radar_session_id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM session_attribution_touch_events touch_events
            WHERE touch_events.roas_radar_session_id = identities.roas_radar_session_id
          )
        ORDER BY identities.retained_until ASC, identities.roas_radar_session_id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM session_attribution_identities identities
      USING expired_sessions
      WHERE identities.roas_radar_session_id = expired_sessions.roas_radar_session_id
    `, [cutoffAt, batchSize]);
    return result.rowCount ?? 0;
}
export async function runSessionAttributionRetention(options = {}) {
    const batchSize = normalizePositiveInteger(options.batchSize, DEFAULT_RETENTION_BATCH_SIZE);
    const maxBatches = normalizePositiveInteger(options.maxBatches, DEFAULT_RETENTION_MAX_BATCHES);
    const cutoffAt = resolveCutoffAt(options.asOf);
    const emitLogs = options.emitLogs ?? true;
    return execute(options.client, async (client) => {
        const protectedCounts = await countProtectedRows(client, cutoffAt);
        let batchesRun = 0;
        let deletedTouchEvents = 0;
        let deletedSessions = 0;
        for (let batchNumber = 1; batchNumber <= maxBatches; batchNumber += 1) {
            const deletedTouchEventsInBatch = await deleteExpiredTouchEvents(client, cutoffAt, batchSize);
            const deletedSessionsInBatch = await deleteExpiredSessions(client, cutoffAt, batchSize);
            if (deletedTouchEventsInBatch === 0 && deletedSessionsInBatch === 0) {
                break;
            }
            batchesRun += 1;
            deletedTouchEvents += deletedTouchEventsInBatch;
            deletedSessions += deletedSessionsInBatch;
            if (emitLogs) {
                logInfo('session_attribution_retention_batch_completed', {
                    batchNumber,
                    cutoffAt: cutoffAt.toISOString(),
                    batchSize,
                    deletedTouchEventsInBatch,
                    deletedSessionsInBatch
                });
            }
        }
        const result = {
            cutoffAt: cutoffAt.toISOString(),
            batchSize,
            maxBatches,
            batchesRun,
            deletedTouchEvents,
            deletedSessions,
            protectedSessionsSkipped: Number(protectedCounts.protected_sessions ?? '0'),
            protectedTouchEventsSkipped: Number(protectedCounts.protected_touch_events ?? '0')
        };
        if (emitLogs) {
            logInfo('session_attribution_retention_completed', result);
        }
        return result;
    });
}
export async function runSessionAttributionRetentionJob(options = {}) {
    try {
        return await runSessionAttributionRetention(options);
    }
    catch (error) {
        logError('session_attribution_retention_failed', error, {
            batchSize: options.batchSize ?? DEFAULT_RETENTION_BATCH_SIZE,
            maxBatches: options.maxBatches ?? DEFAULT_RETENTION_MAX_BATCHES,
            hasCustomAsOf: Boolean(options.asOf)
        });
        throw error;
    }
}
