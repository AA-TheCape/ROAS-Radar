import { query, withTransaction } from "../../db/pool.js";
import { logError, logInfo } from "../../observability/index.js";
import { recordDeadLetter } from "../dead-letters/index.js";
import { GA4_SESSION_ATTRIBUTION_PIPELINE, getGa4SessionAttributionWatermark, ingestGa4SessionAttributionHours, planGa4SessionAttributionHourlyWindows, } from "./ga4-session-attribution.js";
const DEFAULT_BATCH_SIZE = 24;
const DEFAULT_MAX_RETRIES = 5;
const DEFAULT_INITIAL_BACKOFF_SECONDS = 30;
const DEFAULT_MAX_BACKOFF_SECONDS = 1_800;
const DEFAULT_STALE_LOCK_MINUTES = 30;
function toHourStart(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours()));
}
function addHours(date, hours) {
    return new Date(date.getTime() + hours * 60 * 60 * 1000);
}
function normalizeHourStart(value, fieldName = "hourStart") {
    const normalized = typeof value === "string" ? value.trim() : "";
    if (!normalized) {
        throw new Error(`${fieldName} is required`);
    }
    const parsed = new Date(normalized);
    if (Number.isNaN(parsed.getTime())) {
        throw new Error(`Invalid ${fieldName}: ${String(value)}`);
    }
    return toHourStart(parsed).toISOString();
}
function normalizePositiveInteger(value, fallback) {
    if (!Number.isFinite(value)) {
        return fallback;
    }
    const normalized = Math.trunc(value ?? fallback);
    return normalized > 0 ? normalized : fallback;
}
function computeBackoffSeconds(attempts, initialBackoffSeconds, maxBackoffSeconds) {
    const normalizedAttempts = Math.max(1, Math.trunc(attempts));
    return Math.min(initialBackoffSeconds * 2 ** (normalizedAttempts - 1), maxBackoffSeconds);
}
export function listHourlyRange(startHour, endHour) {
    const normalizedStart = normalizeHourStart(startHour, "startHour");
    const normalizedEnd = normalizeHourStart(endHour, "endHour");
    if (normalizedStart > normalizedEnd) {
        throw new Error(`startHour must be less than or equal to endHour: ${normalizedStart} > ${normalizedEnd}`);
    }
    const hours = [];
    for (let cursor = new Date(normalizedStart); cursor.getTime() <= new Date(normalizedEnd).getTime(); cursor = addHours(cursor, 1)) {
        hours.push(cursor.toISOString());
    }
    return hours;
}
async function upsertHourlyJob(client, input) {
    const reviveDeadLettered = Boolean(input.reviveDeadLettered);
    await client.query(`
      INSERT INTO ga4_bigquery_hourly_jobs (
        pipeline_name,
        hour_start,
        status,
        attempts,
        available_at,
        requested_by,
        dead_lettered_at,
        updated_at
      )
      VALUES ($1, $2::timestamptz, 'pending', 0, now(), $3, NULL, now())
      ON CONFLICT (pipeline_name, hour_start)
      DO UPDATE SET
        requested_by = COALESCE(EXCLUDED.requested_by, ga4_bigquery_hourly_jobs.requested_by),
        status = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'processing' THEN ga4_bigquery_hourly_jobs.status
          WHEN ga4_bigquery_hourly_jobs.status = 'dead_lettered' AND NOT $4::boolean THEN ga4_bigquery_hourly_jobs.status
          ELSE 'pending'
        END,
        available_at = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'processing' THEN ga4_bigquery_hourly_jobs.available_at
          WHEN ga4_bigquery_hourly_jobs.status = 'dead_lettered' AND NOT $4::boolean THEN ga4_bigquery_hourly_jobs.available_at
          ELSE now()
        END,
        locked_at = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'processing' THEN ga4_bigquery_hourly_jobs.locked_at
          ELSE NULL
        END,
        locked_by = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'processing' THEN ga4_bigquery_hourly_jobs.locked_by
          ELSE NULL
        END,
        last_error = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'dead_lettered' AND NOT $4::boolean THEN ga4_bigquery_hourly_jobs.last_error
          ELSE NULL
        END,
        dead_lettered_at = CASE
          WHEN ga4_bigquery_hourly_jobs.status = 'dead_lettered' AND NOT $4::boolean THEN ga4_bigquery_hourly_jobs.dead_lettered_at
          ELSE NULL
        END,
        updated_at = now()
    `, [
        input.pipelineName,
        input.hourStart,
        input.requestedBy ?? null,
        reviveDeadLettered,
    ]);
}
export async function enqueueHours(input) {
    const hourStarts = Array.from(new Set((input.hourStarts ?? []).map((hour) => normalizeHourStart(hour)))).sort();
    if (hourStarts.length === 0) {
        return { hourStarts: [], enqueuedCount: 0 };
    }
    await withTransaction(async (client) => {
        for (const hourStart of hourStarts) {
            await upsertHourlyJob(client, {
                pipelineName: input.pipelineName ?? GA4_SESSION_ATTRIBUTION_PIPELINE,
                hourStart,
                requestedBy: input.requestedBy ?? null,
                reviveDeadLettered: input.reviveDeadLettered ?? false,
            });
        }
    });
    return {
        hourStarts,
        enqueuedCount: hourStarts.length,
    };
}
async function requeueStaleLocks(client, pipelineName, staleLockMinutes) {
    const result = await client.query(`
      WITH stale_jobs AS (
        SELECT pipeline_name, hour_start
        FROM ga4_bigquery_hourly_jobs
        WHERE pipeline_name = $1
          AND status = 'processing'
          AND locked_at < now() - ($2::int * interval '1 minute')
        ORDER BY locked_at ASC, hour_start ASC
        FOR UPDATE SKIP LOCKED
      )
      UPDATE ga4_bigquery_hourly_jobs AS jobs
      SET
        status = 'retry',
        locked_at = NULL,
        locked_by = NULL,
        available_at = now(),
        last_error = COALESCE(jobs.last_error, 'ga4_hour_requeued_after_stale_lock'),
        updated_at = now()
      FROM stale_jobs
      WHERE jobs.pipeline_name = stale_jobs.pipeline_name
        AND jobs.hour_start = stale_jobs.hour_start
      RETURNING jobs.hour_start
    `, [pipelineName, staleLockMinutes]);
    return result.rowCount ?? result.rows.length;
}
export async function claimHourlyJobs(input) {
    return withTransaction(async (client) => {
        await requeueStaleLocks(client, input.pipelineName ?? GA4_SESSION_ATTRIBUTION_PIPELINE, normalizePositiveInteger(input.staleLockMinutes, DEFAULT_STALE_LOCK_MINUTES));
        const explicitHours = Array.isArray(input.explicitHourStarts) &&
            input.explicitHourStarts.length > 0
            ? Array.from(new Set(input.explicitHourStarts.map((hour) => normalizeHourStart(hour)))).sort()
            : null;
        const result = await client.query(`
        WITH candidate_jobs AS (
          SELECT pipeline_name, hour_start
          FROM ga4_bigquery_hourly_jobs
          WHERE pipeline_name = $1
            AND status IN ('pending', 'retry')
            AND available_at <= now()
            AND (
              $3::timestamptz[] IS NULL
              OR hour_start = ANY($3::timestamptz[])
            )
          ORDER BY hour_start ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE ga4_bigquery_hourly_jobs AS jobs
        SET
          status = 'processing',
          attempts = jobs.attempts + 1,
          locked_at = now(),
          locked_by = $4,
          last_run_started_at = now(),
          updated_at = now()
        FROM candidate_jobs
        WHERE jobs.pipeline_name = candidate_jobs.pipeline_name
          AND jobs.hour_start = candidate_jobs.hour_start
        RETURNING
          jobs.pipeline_name,
          jobs.hour_start,
          jobs.status,
          jobs.attempts,
          jobs.requested_by,
          jobs.available_at,
          jobs.locked_at,
          jobs.locked_by
      `, [
            input.pipelineName ?? GA4_SESSION_ATTRIBUTION_PIPELINE,
            Math.max(1, Math.trunc(input.limit ?? DEFAULT_BATCH_SIZE)),
            explicitHours,
            input.workerId,
        ]);
        return result.rows.map((row) => ({
            pipelineName: row.pipeline_name,
            hourStart: row.hour_start.toISOString(),
            attempts: row.attempts,
            requestedBy: row.requested_by,
        }));
    });
}
async function markHourlyJobCompleted(client, job, workerId) {
    await client.query(`
      UPDATE ga4_bigquery_hourly_jobs
      SET
        status = 'completed',
        locked_at = NULL,
        locked_by = NULL,
        last_run_completed_at = now(),
        last_error = NULL,
        updated_at = now()
      WHERE pipeline_name = $1
        AND hour_start = $2::timestamptz
        AND locked_by = $3
    `, [job.pipelineName, job.hourStart, workerId]);
}
async function markHourlyJobForRetry(client, input) {
    await client.query(`
      UPDATE ga4_bigquery_hourly_jobs
      SET
        status = 'retry',
        available_at = now() + ($4::int * interval '1 second'),
        locked_at = NULL,
        locked_by = NULL,
        last_run_completed_at = now(),
        last_error = $5,
        updated_at = now()
      WHERE pipeline_name = $1
        AND hour_start = $2::timestamptz
        AND locked_by = $3
    `, [
        input.job.pipelineName,
        input.job.hourStart,
        input.workerId,
        input.backoffSeconds,
        input.errorMessage,
    ]);
}
async function markHourlyJobDeadLettered(client, input) {
    const sourceRecordId = input.job.hourStart;
    const sourceQueueKey = input.job.pipelineName;
    await recordDeadLetter(client, {
        eventType: "ga4_session_attribution_hour_failed",
        sourceTable: "ga4_bigquery_hourly_jobs",
        sourceRecordId,
        sourceQueueKey,
        payload: {
            pipelineName: input.job.pipelineName,
            hourStart: input.job.hourStart,
            workerId: input.workerId,
            attempts: input.job.attempts,
        },
        error: input.error,
    });
    await client.query(`
      UPDATE ga4_bigquery_hourly_jobs
      SET
        status = 'dead_lettered',
        dead_lettered_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_run_completed_at = now(),
        last_error = $4,
        updated_at = now()
      WHERE pipeline_name = $1
        AND hour_start = $2::timestamptz
        AND locked_by = $3
    `, [
        input.job.pipelineName,
        input.job.hourStart,
        input.workerId,
        input.errorMessage,
    ]);
}
async function seedHoursForProcessing(input) {
    if (input.explicitHourStarts && input.explicitHourStarts.length > 0) {
        return enqueueHours({
            pipelineName: input.pipelineName,
            requestedBy: input.requestedBy,
            hourStarts: input.explicitHourStarts,
            reviveDeadLettered: true,
        });
    }
    const watermarkHour = await getGa4SessionAttributionWatermark({ query });
    const windows = planGa4SessionAttributionHourlyWindows({
        now: input.now ?? new Date(),
        watermarkHour: watermarkHour ? new Date(watermarkHour) : null,
        config: input.config,
    });
    return enqueueHours({
        pipelineName: input.pipelineName,
        requestedBy: input.requestedBy,
        hourStarts: windows.map((window) => window.hourStart),
        reviveDeadLettered: false,
    });
}
export async function processGa4SessionAttributionHourlyJobs(input) {
    const pipelineName = input.pipelineName ?? GA4_SESSION_ATTRIBUTION_PIPELINE;
    const batchSize = normalizePositiveInteger(input.batchSize, DEFAULT_BATCH_SIZE);
    const maxRetries = normalizePositiveInteger(input.maxRetries, DEFAULT_MAX_RETRIES);
    const initialBackoffSeconds = normalizePositiveInteger(input.initialBackoffSeconds, DEFAULT_INITIAL_BACKOFF_SECONDS);
    const maxBackoffSeconds = normalizePositiveInteger(input.maxBackoffSeconds, DEFAULT_MAX_BACKOFF_SECONDS);
    const staleLockMinutes = normalizePositiveInteger(input.staleLockMinutes, DEFAULT_STALE_LOCK_MINUTES);
    const explicitHourStarts = input.explicitHourStarts?.length
        ? Array.from(new Set(input.explicitHourStarts.map((hour) => normalizeHourStart(hour)))).sort()
        : null;
    const seeded = await seedHoursForProcessing({
        pipelineName,
        config: input.config,
        requestedBy: input.requestedBy,
        explicitHourStarts,
        now: input.now ?? new Date(),
    });
    const claimedJobs = await claimHourlyJobs({
        pipelineName,
        workerId: input.workerId,
        limit: batchSize,
        staleLockMinutes,
        explicitHourStarts,
    });
    let succeededJobs = 0;
    let retriedJobs = 0;
    let deadLetteredJobs = 0;
    for (const job of claimedJobs) {
        try {
            await ingestGa4SessionAttributionHours({
                config: input.config,
                executor: input.executor,
                now: input.now ?? new Date(),
                hourStarts: [job.hourStart],
            });
            await withTransaction(async (client) => {
                await markHourlyJobCompleted(client, job, input.workerId);
            });
            succeededJobs += 1;
        }
        catch (error) {
            const errorMessage = error instanceof Error
                ? error.message.slice(0, 1000)
                : String(error).slice(0, 1000);
            const shouldDeadLetter = job.attempts >= maxRetries;
            logError("ga4_session_attribution_hour_failed", error, {
                pipelineName,
                workerId: input.workerId,
                hourStart: job.hourStart,
                attempts: job.attempts,
                shouldDeadLetter,
            });
            await withTransaction(async (client) => {
                if (shouldDeadLetter) {
                    await markHourlyJobDeadLettered(client, {
                        job,
                        workerId: input.workerId,
                        error,
                        errorMessage,
                    });
                }
                else {
                    await markHourlyJobForRetry(client, {
                        job,
                        workerId: input.workerId,
                        backoffSeconds: computeBackoffSeconds(job.attempts, initialBackoffSeconds, maxBackoffSeconds),
                        errorMessage,
                    });
                }
            });
            if (shouldDeadLetter) {
                deadLetteredJobs += 1;
            }
            else {
                retriedJobs += 1;
            }
        }
    }
    const result = {
        pipelineName,
        requestedBy: input.requestedBy,
        workerId: input.workerId,
        seededHours: seeded.hourStarts,
        seededHourCount: seeded.enqueuedCount,
        claimedHourCount: claimedJobs.length,
        claimedHours: claimedJobs.map((job) => job.hourStart),
        succeededJobs,
        retriedJobs,
        deadLetteredJobs,
    };
    logInfo("ga4_session_attribution_hourly_jobs_completed", result);
    return result;
}
