import { withTransaction } from '../../db/pool.js';
import { logError } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { getReportingTimezone, formatDateInTimezone } from '../settings/index.js';
import { ATTRIBUTION_MODELS, computeAttributionOutputs } from './engine.js';
const ATTRIBUTION_MODEL_VERSION = 1;
const ATTRIBUTION_WINDOW_DAYS = 7;
const JOB_STALE_AFTER_MINUTES = 15;
const MAX_RETRY_DELAY_SECONDS = 1_800;
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
export function buildQueueKey(shopifyOrderId) {
    return `order:${shopifyOrderId}`;
}
export function computeRetryDelaySeconds(attempts) {
    const normalizedAttempts = Number.isFinite(attempts) ? Math.max(Math.trunc(attempts), 1) : 1;
    return Math.min(30 * 2 ** (normalizedAttempts - 1), MAX_RETRY_DELAY_SECONDS);
}
export function buildProcessingMetricsLog(result) {
    return JSON.stringify({
        severity: 'INFO',
        event: 'attribution_queue_run',
        message: 'attribution_queue_run',
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
        ...result
    });
}
function buildQueueOutcomeLog(workerId, outcome, value) {
    return JSON.stringify({
        severity: 'INFO',
        event: 'attribution_queue_outcome',
        message: 'attribution_queue_outcome',
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
        workerId,
        outcome,
        value
    });
}
async function execute(client, callback) {
    if (client) {
        return callback(client);
    }
    return withTransaction(callback);
}
export async function enqueueAttributionForOrder(shopifyOrderId, requestedReason, client) {
    await execute(client, async (db) => {
        await db.query(`
        INSERT INTO attribution_jobs (
          queue_key,
          job_type,
          shopify_order_id,
          requested_reason,
          requested_model_version,
          status,
          attempts,
          available_at,
          updated_at
        )
        VALUES ($1, 'order', $2, $3, $4, 'pending', 0, now(), now())
        ON CONFLICT (queue_key)
        DO UPDATE SET
          requested_reason = EXCLUDED.requested_reason,
          requested_model_version = EXCLUDED.requested_model_version,
          status = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.status
            ELSE 'pending'
          END,
          available_at = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.available_at
            ELSE now()
          END,
          completed_at = NULL,
          last_error = NULL,
          updated_at = now()
      `, [buildQueueKey(shopifyOrderId), shopifyOrderId, requestedReason, ATTRIBUTION_MODEL_VERSION]);
    });
}
export async function enqueueAttributionForTrackingTouchpoint(client, input) {
    const result = await client.query(`
      SELECT DISTINCT o.shopify_order_id
      FROM shopify_orders o
      WHERE o.landing_session_id = $1::uuid
         OR ($2::text IS NOT NULL AND o.checkout_token = $2)
         OR ($3::text IS NOT NULL AND o.cart_token = $3)
         OR EXISTS (
           SELECT 1
           FROM tracking_sessions s
           WHERE s.id = $1::uuid
             AND s.customer_identity_id IS NOT NULL
             AND s.customer_identity_id = o.customer_identity_id
         )
    `, [input.sessionId, input.shopifyCheckoutToken, input.shopifyCartToken]);
    for (const row of result.rows) {
        await enqueueAttributionForOrder(row.shopify_order_id, 'tracking_touchpoint_updated', client);
    }
    return result.rowCount ?? result.rows.length;
}
async function requeueStaleJobs(client, staleScanLimit) {
    if (staleScanLimit <= 0) {
        return 0;
    }
    const result = await client.query(`
      WITH stale_jobs AS (
        SELECT id
        FROM attribution_jobs
        WHERE status = 'processing'
          AND locked_at < now() - ($1::int * interval '1 minute')
        ORDER BY locked_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE attribution_jobs j
      SET
        status = 'retry',
        locked_at = NULL,
        locked_by = NULL,
        available_at = now(),
        last_error = COALESCE(j.last_error, 'job_requeued_after_stale_lock'),
        updated_at = now()
      FROM stale_jobs
      WHERE j.id = stale_jobs.id
      RETURNING j.id
    `, [JOB_STALE_AFTER_MINUTES, staleScanLimit]);
    return result.rowCount ?? result.rows.length;
}
async function claimJobs(client, workerId, limit) {
    const result = await client.query(`
      WITH candidate_jobs AS (
        SELECT id
        FROM attribution_jobs
        WHERE status IN ('pending', 'retry')
          AND available_at <= now()
        ORDER BY available_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE attribution_jobs j
      SET
        status = 'processing',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1,
        updated_at = now()
      FROM candidate_jobs
      WHERE j.id = candidate_jobs.id
      RETURNING j.id, j.shopify_order_id, j.attempts
    `, [workerId, Math.max(limit, 0)]);
    return result.rows;
}
async function fetchOrder(client, shopifyOrderId) {
    const result = await client.query(`
      SELECT
        shopify_order_id,
        total_price::text,
        processed_at,
        created_at_shopify,
        ingested_at,
        landing_session_id::text,
        checkout_token,
        cart_token,
        email_hash,
        customer_identity_id::text
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `, [shopifyOrderId]);
    return result.rows[0] ?? null;
}
function buildSessionTouchpoint(row, attributionReason, isForced) {
    const isDirect = !row.source && !row.medium && !row.campaign && !row.click_id_value;
    return {
        sessionId: row.session_id,
        occurredAt: row.occurred_at,
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        clickIdType: row.click_id_type,
        clickIdValue: row.click_id_value,
        attributionReason,
        isDirect,
        isForced
    };
}
async function fetchExactSession(client, sessionId, reason) {
    const result = await client.query(`
      SELECT
        s.id::text AS session_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(s.initial_gclid, s.initial_fbclid, s.initial_ttclid, s.initial_msclkid) AS click_id_value
      FROM tracking_sessions s
      WHERE s.id = $1::uuid
      LIMIT 1
    `, [sessionId]);
    const row = result.rows[0];
    if (!row) {
        return null;
    }
    return {
        touchpoints: [buildSessionTouchpoint(row, reason, true)],
        confidenceScore: 1
    };
}
async function fetchJourneyByIdentity(client, order, orderOccurredAt) {
    if (!order.customer_identity_id && !order.email_hash) {
        return null;
    }
    const result = await client.query(`
      SELECT
        s.id::text AS session_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(s.initial_gclid, s.initial_fbclid, s.initial_ttclid, s.initial_msclkid) AS click_id_value
      FROM tracking_sessions s
      WHERE (
        ($1::uuid IS NOT NULL AND s.customer_identity_id = $1::uuid)
        OR EXISTS (
          SELECT 1
          FROM shopify_orders o
          WHERE o.shopify_order_id = $2
            AND o.customer_identity_id IS NOT NULL
            AND o.customer_identity_id = s.customer_identity_id
        )
      )
        AND s.first_seen_at <= $3
        AND s.first_seen_at >= $3 - ($4::int * interval '1 day')
      ORDER BY s.first_seen_at ASC, s.id ASC
    `, [order.customer_identity_id, order.shopify_order_id, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]);
    if (result.rows.length === 0) {
        return null;
    }
    return {
        touchpoints: result.rows.map((row) => buildSessionTouchpoint(row, 'matched_by_customer_identity', false)),
        confidenceScore: 0.6
    };
}
async function resolveAttributionJourney(client, order) {
    const orderOccurredAt = order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
    if (order.landing_session_id) {
        const journey = await fetchExactSession(client, order.landing_session_id, 'matched_by_landing_session');
        if (journey) {
            return journey;
        }
    }
    if (order.checkout_token) {
        const checkoutMatch = await client.query(`
        SELECT
          e.session_id::text AS session_id,
          e.occurred_at,
          COALESCE(e.utm_source, s.initial_utm_source) AS source,
          COALESCE(e.utm_medium, s.initial_utm_medium) AS medium,
          COALESCE(e.utm_campaign, s.initial_utm_campaign) AS campaign,
          COALESCE(e.utm_content, s.initial_utm_content) AS content,
          COALESCE(e.utm_term, s.initial_utm_term) AS term,
          CASE
            WHEN e.gclid IS NOT NULL THEN 'gclid'
            WHEN e.fbclid IS NOT NULL THEN 'fbclid'
            WHEN e.ttclid IS NOT NULL THEN 'ttclid'
            WHEN e.msclkid IS NOT NULL THEN 'msclkid'
            WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
            WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
            WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
            WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
            ELSE NULL
          END AS click_id_type,
          COALESCE(
            e.gclid,
            e.fbclid,
            e.ttclid,
            e.msclkid,
            s.initial_gclid,
            s.initial_fbclid,
            s.initial_ttclid,
            s.initial_msclkid
          ) AS click_id_value
        FROM tracking_events e
        INNER JOIN tracking_sessions s
          ON s.id = e.session_id
        WHERE e.shopify_checkout_token = $1
          AND e.occurred_at <= $2
          AND e.occurred_at >= $2 - ($3::int * interval '1 day')
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `, [order.checkout_token, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]);
        if (checkoutMatch.rows[0]) {
            return {
                touchpoints: [buildSessionTouchpoint(checkoutMatch.rows[0], 'matched_by_checkout_token', true)],
                confidenceScore: 1
            };
        }
    }
    if (order.cart_token) {
        const cartMatch = await client.query(`
        SELECT
          e.session_id::text AS session_id,
          e.occurred_at,
          COALESCE(e.utm_source, s.initial_utm_source) AS source,
          COALESCE(e.utm_medium, s.initial_utm_medium) AS medium,
          COALESCE(e.utm_campaign, s.initial_utm_campaign) AS campaign,
          COALESCE(e.utm_content, s.initial_utm_content) AS content,
          COALESCE(e.utm_term, s.initial_utm_term) AS term,
          CASE
            WHEN e.gclid IS NOT NULL THEN 'gclid'
            WHEN e.fbclid IS NOT NULL THEN 'fbclid'
            WHEN e.ttclid IS NOT NULL THEN 'ttclid'
            WHEN e.msclkid IS NOT NULL THEN 'msclkid'
            WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
            WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
            WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
            WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
            ELSE NULL
          END AS click_id_type,
          COALESCE(
            e.gclid,
            e.fbclid,
            e.ttclid,
            e.msclkid,
            s.initial_gclid,
            s.initial_fbclid,
            s.initial_ttclid,
            s.initial_msclkid
          ) AS click_id_value
        FROM tracking_events e
        INNER JOIN tracking_sessions s
          ON s.id = e.session_id
        WHERE e.shopify_cart_token = $1
          AND e.occurred_at <= $2
          AND e.occurred_at >= $2 - ($3::int * interval '1 day')
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `, [order.cart_token, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]);
        if (cartMatch.rows[0]) {
            return {
                touchpoints: [buildSessionTouchpoint(cartMatch.rows[0], 'matched_by_cart_token', true)],
                confidenceScore: 0.9
            };
        }
    }
    const identityJourney = await fetchJourneyByIdentity(client, order, orderOccurredAt);
    if (identityJourney) {
        return identityJourney;
    }
    return {
        touchpoints: [],
        confidenceScore: 0
    };
}
function selectPrimaryCredit(credits) {
    return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}
async function persistAttribution(client, order, journey) {
    const orderOccurredAt = order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
    const outputs = computeAttributionOutputs(journey.touchpoints, {
        orderOccurredAt,
        orderRevenue: order.total_price
    });
    const primaryCredit = selectPrimaryCredit(outputs.last_touch);
    await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [order.shopify_order_id]);
    for (const model of ATTRIBUTION_MODELS) {
        const modelCredits = outputs[model];
        for (const credit of modelCredits) {
            await client.query(`
          INSERT INTO attribution_order_credits (
            shopify_order_id,
            attribution_model,
            touchpoint_position,
            session_id,
            touchpoint_occurred_at,
            attributed_source,
            attributed_medium,
            attributed_campaign,
            attributed_content,
            attributed_term,
            attributed_click_id_type,
            attributed_click_id_value,
            credit_weight,
            revenue_credit,
            is_primary,
            attribution_reason,
            model_version
          )
          VALUES (
            $1,
            $2,
            $3,
            $4::uuid,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17
          )
        `, [
                order.shopify_order_id,
                credit.attributionModel,
                credit.touchpointPosition,
                credit.sessionId,
                credit.touchpointOccurredAt,
                normalizeNullableString(credit.source),
                normalizeNullableString(credit.medium),
                normalizeNullableString(credit.campaign),
                normalizeNullableString(credit.content),
                normalizeNullableString(credit.term),
                normalizeNullableString(credit.clickIdType),
                normalizeNullableString(credit.clickIdValue),
                credit.creditWeight,
                credit.revenueCredit,
                credit.isPrimary,
                credit.attributionReason,
                ATTRIBUTION_MODEL_VERSION
            ]);
        }
    }
    await client.query(`
      INSERT INTO attribution_results (
        shopify_order_id,
        session_id,
        attribution_model,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version,
        model_version
      )
      VALUES (
        $1,
        $2::uuid,
        'last_touch',
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        now(),
        1,
        $12
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        session_id = EXCLUDED.session_id,
        attribution_model = EXCLUDED.attribution_model,
        attributed_source = EXCLUDED.attributed_source,
        attributed_medium = EXCLUDED.attributed_medium,
        attributed_campaign = EXCLUDED.attributed_campaign,
        attributed_content = EXCLUDED.attributed_content,
        attributed_term = EXCLUDED.attributed_term,
        attributed_click_id_type = EXCLUDED.attributed_click_id_type,
        attributed_click_id_value = EXCLUDED.attributed_click_id_value,
        confidence_score = EXCLUDED.confidence_score,
        attribution_reason = EXCLUDED.attribution_reason,
        attributed_at = now(),
        model_version = EXCLUDED.model_version
    `, [
        order.shopify_order_id,
        primaryCredit.sessionId,
        normalizeNullableString(primaryCredit.source),
        normalizeNullableString(primaryCredit.medium),
        normalizeNullableString(primaryCredit.campaign),
        normalizeNullableString(primaryCredit.content),
        normalizeNullableString(primaryCredit.term),
        normalizeNullableString(primaryCredit.clickIdType),
        normalizeNullableString(primaryCredit.clickIdValue),
        journey.confidenceScore,
        primaryCredit.attributionReason,
        ATTRIBUTION_MODEL_VERSION
    ]);
}
async function processClaimedJob(client, job, workerId) {
    const order = await fetchOrder(client, job.shopify_order_id);
    if (!order) {
        await client.query(`
        UPDATE attribution_jobs
        SET
          status = 'completed',
          completed_at = now(),
          locked_at = NULL,
          locked_by = NULL,
          last_error = 'order_not_found',
          updated_at = now()
        WHERE id = $1
      `, [job.id]);
        process.stdout.write(`${JSON.stringify({
            severity: 'WARNING',
            event: 'attribution_job_skipped',
            message: 'attribution_job_skipped',
            timestamp: new Date().toISOString(),
            workerId,
            shopifyOrderId: job.shopify_order_id,
            reason: 'order_not_found'
        })}\n`);
        return;
    }
    const journey = await resolveAttributionJourney(client, order);
    await persistAttribution(client, order, journey);
    const metricDate = formatDateInTimezone(order.processed_at ?? order.created_at_shopify ?? order.ingested_at, await getReportingTimezone(client));
    await refreshDailyReportingMetrics(client, [metricDate]);
    await client.query(`
      UPDATE attribution_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `, [job.id, workerId]);
    process.stdout.write(`${JSON.stringify({
        severity: 'INFO',
        event: 'attribution_job_processed',
        message: 'attribution_job_processed',
        timestamp: new Date().toISOString(),
        workerId,
        shopifyOrderId: job.shopify_order_id,
        confidenceScore: journey.confidenceScore,
        touchpointCount: journey.touchpoints.length,
        attributionReason: primaryCreditReason(journey)
    })}\n`);
}
function primaryCreditReason(journey) {
    const lastTouchpoint = journey.touchpoints[journey.touchpoints.length - 1];
    return lastTouchpoint?.attributionReason ?? 'unattributed';
}
async function markJobForRetry(client, job, workerId, error) {
    await client.query(`
      UPDATE attribution_jobs
      SET
        status = 'retry',
        available_at = now() + ($3::int * interval '1 second'),
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `, [
        job.id,
        workerId,
        computeRetryDelaySeconds(job.attempts),
        error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000)
    ]);
}
export async function processAttributionQueue(options) {
    const startedAt = Date.now();
    const result = await withTransaction(async (client) => {
        const staleJobsEnqueued = await requeueStaleJobs(client, options.staleScanLimit ?? 0);
        const claimedJobs = await claimJobs(client, options.workerId, options.limit);
        return {
            staleJobsEnqueued,
            claimedJobs
        };
    });
    let succeededJobs = 0;
    let failedJobs = 0;
    for (const job of result.claimedJobs) {
        try {
            await withTransaction(async (client) => {
                await processClaimedJob(client, job, options.workerId);
            });
            succeededJobs += 1;
        }
        catch (error) {
            failedJobs += 1;
            logError('attribution_job_failed', error, {
                workerId: options.workerId,
                shopifyOrderId: job.shopify_order_id,
                attempts: job.attempts
            });
            await withTransaction(async (client) => {
                await markJobForRetry(client, job, options.workerId, error);
            });
        }
    }
    const summary = {
        workerId: options.workerId,
        modelVersion: ATTRIBUTION_MODEL_VERSION,
        staleJobsEnqueued: result.staleJobsEnqueued,
        claimedJobs: result.claimedJobs.length,
        succeededJobs,
        failedJobs,
        durationMs: Date.now() - startedAt
    };
    if (options.emitMetrics) {
        process.stdout.write(`${buildProcessingMetricsLog(summary)}\n`);
        process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'claimed', summary.claimedJobs)}\n`);
        process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'succeeded', summary.succeededJobs)}\n`);
        process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'failed', summary.failedJobs)}\n`);
    }
    return summary;
}
export const __attributionTestUtils = {
    buildQueueKey,
    computeRetryDelaySeconds,
    buildProcessingMetricsLog
};
