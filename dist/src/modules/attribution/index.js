import { withTransaction } from '../../db/pool.js';
import { logError } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { getReportingTimezone, formatDateInTimezone } from '../settings/index.js';
import { ATTRIBUTION_MODELS, computeAttributionOutputs, computeSingleWinnerCredits } from './engine.js';
import { confidenceLabelForScore, confidenceScoreForWinner, dedupeDeterministicCandidates, isDirectTouchpoint, selectGa4FallbackWinner, selectLastNonDirectWinner } from './resolver.js';
import { lookupGa4FallbackCandidates } from './ga4-fallback-candidates.js';
import { extractShopifyHintAttribution } from './shopify-hints.js';
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
    `, [input.sessionId, input.shopifyCheckoutToken ?? null, input.shopifyCartToken ?? null]);
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
        source_name,
        raw_payload,
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
function resolveOrderOccurredAt(order) {
    return order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
}
function buildResolvedTouchpointFromValues(input) {
    const confidenceScore = confidenceScoreForWinner({
        matchSource: input.matchSource,
        clickIdValue: input.clickIdValue
    });
    return {
        sessionId: input.sessionId,
        sourceTouchEventId: input.sourceTouchEventId,
        occurredAt: input.occurredAt,
        source: input.source,
        medium: input.medium,
        campaign: input.campaign,
        content: input.content,
        term: input.term,
        clickIdType: input.clickIdType,
        clickIdValue: input.clickIdValue,
        attributionReason: input.attributionReason,
        matchSource: input.matchSource,
        ingestionSource: input.ingestionSource,
        confidenceLabel: confidenceLabelForScore(confidenceScore),
        ga4ClientId: input.ga4ClientId ?? null,
        ga4SessionId: input.ga4SessionId ?? null,
        isDirect: isDirectTouchpoint({
            source: input.source,
            medium: input.medium,
            campaign: input.campaign,
            content: input.content,
            term: input.term,
            clickIdValue: input.clickIdValue
        }),
        isForced: input.isForced
    };
}
function buildResolvedTouchpoint(row, ingestionSource, attributionReason, isForced) {
    return buildResolvedTouchpointFromValues({
        sessionId: row.session_id,
        sourceTouchEventId: row.source_touch_event_id,
        occurredAt: row.occurred_at,
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        clickIdType: row.click_id_type,
        clickIdValue: row.click_id_value,
        attributionReason,
        matchSource: ingestionSource,
        ingestionSource,
        isForced
    });
}
function serializeResolvedTouchpoint(touchpoint) {
    return {
        sessionId: touchpoint.sessionId,
        sourceTouchEventId: touchpoint.sourceTouchEventId,
        occurredAt: touchpoint.occurredAt.toISOString(),
        source: touchpoint.source,
        medium: touchpoint.medium,
        campaign: touchpoint.campaign,
        content: touchpoint.content,
        term: touchpoint.term,
        clickIdType: touchpoint.clickIdType,
        clickIdValue: touchpoint.clickIdValue,
        attributionReason: touchpoint.attributionReason,
        matchSource: touchpoint.matchSource,
        confidenceLabel: touchpoint.confidenceLabel,
        ingestionSource: touchpoint.ingestionSource,
        ga4ClientId: touchpoint.ga4ClientId,
        ga4SessionId: touchpoint.ga4SessionId,
        isDirect: touchpoint.isDirect
    };
}
async function fetchLandingSessionCandidate(client, sessionId) {
    const result = await client.query(`
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) AS first_event ON true
      WHERE s.id = $1::uuid
      LIMIT 1
    `, [sessionId]);
    const row = result.rows[0];
    return row ? buildResolvedTouchpoint(row, 'landing_session_id', 'matched_by_landing_session', true) : null;
}
async function fetchLatestTokenCandidate(client, tokenColumn, token, orderOccurredAt, ingestionSource, attributionReason) {
    const result = await client.query(`
      SELECT
        e.session_id::text AS session_id,
        e.id::text AS source_touch_event_id,
        e.occurred_at,
        COALESCE(e.utm_source, s.initial_utm_source) AS source,
        COALESCE(e.utm_medium, s.initial_utm_medium) AS medium,
        COALESCE(e.utm_campaign, s.initial_utm_campaign) AS campaign,
        COALESCE(e.utm_content, s.initial_utm_content) AS content,
        COALESCE(e.utm_term, s.initial_utm_term) AS term,
        CASE
          WHEN e.gclid IS NOT NULL THEN 'gclid'
          WHEN e.gbraid IS NOT NULL THEN 'gbraid'
          WHEN e.wbraid IS NOT NULL THEN 'wbraid'
          WHEN e.fbclid IS NOT NULL THEN 'fbclid'
          WHEN e.ttclid IS NOT NULL THEN 'ttclid'
          WHEN e.msclkid IS NOT NULL THEN 'msclkid'
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          e.gclid,
          e.gbraid,
          e.wbraid,
          e.fbclid,
          e.ttclid,
          e.msclkid,
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_events e
      INNER JOIN tracking_sessions s
        ON s.id = e.session_id
      WHERE ${tokenColumn} = $1
        AND e.occurred_at <= $2
        AND e.occurred_at >= $2 - ($3::int * interval '1 day')
      ORDER BY e.occurred_at DESC, e.id DESC
      LIMIT 1
    `, [token, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]);
    const row = result.rows[0];
    return row ? buildResolvedTouchpoint(row, ingestionSource, attributionReason, true) : null;
}
async function fetchIdentityCandidates(client, order, orderOccurredAt) {
    if (!order.customer_identity_id && !order.email_hash) {
        return [];
    }
    const result = await client.query(`
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) AS first_event ON true
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
    return result.rows.map((row) => buildResolvedTouchpoint(row, 'customer_identity', 'matched_by_customer_identity', false));
}
async function collectDeterministicCandidates(client, order) {
    const orderOccurredAt = resolveOrderOccurredAt(order);
    const candidates = [];
    if (order.landing_session_id) {
        const landingCandidate = await fetchLandingSessionCandidate(client, order.landing_session_id);
        if (landingCandidate) {
            candidates.push(landingCandidate);
        }
    }
    if (order.checkout_token) {
        const checkoutCandidate = await fetchLatestTokenCandidate(client, 'shopify_checkout_token', order.checkout_token, orderOccurredAt, 'checkout_token', 'matched_by_checkout_token');
        if (checkoutCandidate) {
            candidates.push(checkoutCandidate);
        }
    }
    if (order.cart_token) {
        const cartCandidate = await fetchLatestTokenCandidate(client, 'shopify_cart_token', order.cart_token, orderOccurredAt, 'cart_token', 'matched_by_cart_token');
        if (cartCandidate) {
            candidates.push(cartCandidate);
        }
    }
    candidates.push(...(await fetchIdentityCandidates(client, order, orderOccurredAt)));
    return candidates;
}
function resolveShopifyHintFallback(order) {
    if (normalizeNullableString(order.source_name) !== 'web') {
        return null;
    }
    const hint = extractShopifyHintAttribution(order.raw_payload);
    if (!hint) {
        return null;
    }
    return buildResolvedTouchpointFromValues({
        sessionId: null,
        sourceTouchEventId: null,
        occurredAt: resolveOrderOccurredAt(order),
        source: normalizeNullableString(hint.source),
        medium: normalizeNullableString(hint.medium),
        campaign: normalizeNullableString(hint.campaign),
        content: normalizeNullableString(hint.content),
        term: normalizeNullableString(hint.term),
        clickIdType: normalizeNullableString(hint.clickIdType),
        clickIdValue: normalizeNullableString(hint.clickIdValue),
        attributionReason: 'shopify_hint_derived',
        matchSource: 'shopify_hint_fallback',
        ingestionSource: null,
        isForced: true
    });
}
async function resolveGa4Fallback(client, order, orderOccurredAt) {
    if (normalizeNullableString(order.source_name) !== 'web') {
        return null;
    }
    const winner = selectGa4FallbackWinner(await lookupGa4FallbackCandidates({
        orderOccurredAt: orderOccurredAt.toISOString(),
        customerIdentityId: order.customer_identity_id,
        emailHash: order.email_hash,
        transactionId: order.shopify_order_id
    }, client), orderOccurredAt);
    if (!winner) {
        return null;
    }
    return buildResolvedTouchpointFromValues({
        sessionId: null,
        sourceTouchEventId: null,
        occurredAt: new Date(winner.occurredAt),
        source: normalizeNullableString(winner.source),
        medium: normalizeNullableString(winner.medium),
        campaign: normalizeNullableString(winner.campaign),
        content: normalizeNullableString(winner.content),
        term: normalizeNullableString(winner.term),
        clickIdType: normalizeNullableString(winner.clickIdType),
        clickIdValue: normalizeNullableString(winner.clickIdValue),
        attributionReason: 'ga4_fallback_derived',
        matchSource: 'ga4_fallback',
        ingestionSource: null,
        isForced: true,
        ga4ClientId: normalizeNullableString(winner.ga4ClientId),
        ga4SessionId: normalizeNullableString(winner.ga4SessionId)
    });
}
async function resolveAttributionJourney(client, order) {
    const orderOccurredAt = resolveOrderOccurredAt(order);
    const deterministicCandidates = await collectDeterministicCandidates(client, order);
    const touchpoints = dedupeDeterministicCandidates(deterministicCandidates);
    const winner = selectLastNonDirectWinner(touchpoints) ??
        resolveShopifyHintFallback(order) ??
        (await resolveGa4Fallback(client, order, orderOccurredAt));
    const confidenceScore = confidenceScoreForWinner(winner);
    return {
        touchpoints: winner && touchpoints.length === 0 ? [winner] : touchpoints,
        winner,
        confidenceScore,
        confidenceLabel: confidenceLabelForScore(confidenceScore)
    };
}
function selectPrimaryCredit(credits) {
    return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}
function touchpointForCredit(journey, credit) {
    return (journey.touchpoints[credit.touchpointPosition] ?? {
        matchSource: 'unattributed',
        confidenceLabel: 'none',
        ga4ClientId: null,
        ga4SessionId: null
    });
}
async function shouldPersistSyntheticAttribution(client, shopifyOrderId, matchSource, confidenceScore) {
    const result = await client.query(`
      SELECT match_source, confidence_score::text
      FROM attribution_results
      WHERE shopify_order_id = $1
      LIMIT 1
    `, [shopifyOrderId]);
    const existing = result.rows[0];
    if (!existing) {
        return true;
    }
    if (existing.match_source === 'landing_session_id' ||
        existing.match_source === 'checkout_token' ||
        existing.match_source === 'cart_token' ||
        existing.match_source === 'customer_identity') {
        return false;
    }
    if (existing.match_source === 'shopify_hint_fallback' && matchSource === 'ga4_fallback') {
        return false;
    }
    return Number.parseFloat(existing.confidence_score) <= confidenceScore;
}
async function persistAttribution(client, order, journey, options = {}) {
    const orderOccurredAt = resolveOrderOccurredAt(order);
    const outputs = computeAttributionOutputs(journey.touchpoints, {
        orderOccurredAt,
        orderRevenue: order.total_price
    });
    if (journey.winner) {
        const winnerIndex = journey.touchpoints.findIndex((touchpoint) => touchpoint.sessionId === journey.winner?.sessionId);
        if (winnerIndex >= 0) {
            outputs.last_touch = computeSingleWinnerCredits('last_touch', journey.touchpoints, winnerIndex, order.total_price);
        }
    }
    const primaryCredit = selectPrimaryCredit(outputs.last_touch);
    if (!primaryCredit) {
        throw new Error(`Failed to compute attribution credits for Shopify order ${order.shopify_order_id}`);
    }
    if (options.preventDowngrade &&
        !(await shouldPersistSyntheticAttribution(client, order.shopify_order_id, journey.winner?.matchSource ?? 'unattributed', journey.confidenceScore))) {
        return;
    }
    await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [order.shopify_order_id]);
    for (const model of ATTRIBUTION_MODELS) {
        const modelCredits = outputs[model];
        for (const credit of modelCredits) {
            const touchpoint = touchpointForCredit(journey, credit);
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
            match_source,
            confidence_label,
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
            $17,
            $18,
            $19
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
                touchpoint.matchSource,
                touchpoint.confidenceLabel,
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
        match_source,
        confidence_score,
        confidence_label,
        attribution_reason,
        ga4_client_id,
        ga4_session_id,
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
        $12,
        $13,
        $14,
        $15,
        now(),
        1,
        $16
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
        match_source = EXCLUDED.match_source,
        confidence_score = EXCLUDED.confidence_score,
        confidence_label = EXCLUDED.confidence_label,
        attribution_reason = EXCLUDED.attribution_reason,
        ga4_client_id = EXCLUDED.ga4_client_id,
        ga4_session_id = EXCLUDED.ga4_session_id,
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
        journey.winner?.matchSource ?? 'unattributed',
        journey.confidenceScore,
        journey.confidenceLabel,
        primaryCredit.attributionReason,
        journey.winner?.ga4ClientId ?? null,
        journey.winner?.ga4SessionId ?? null,
        ATTRIBUTION_MODEL_VERSION
    ]);
    await client.query(`
      UPDATE shopify_orders
      SET
        attribution_snapshot = $2::jsonb,
        attribution_snapshot_updated_at = now()
      WHERE shopify_order_id = $1
    `, [
        order.shopify_order_id,
        JSON.stringify({
            confidenceScore: journey.confidenceScore,
            confidenceLabel: journey.confidenceLabel,
            winner: journey.winner ? serializeResolvedTouchpoint(journey.winner) : null,
            timeline: journey.touchpoints.map(serializeResolvedTouchpoint)
        })
    ]);
}
function primaryCreditReason(journey) {
    return journey.winner?.attributionReason ?? 'unattributed';
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
    const metricDate = formatDateInTimezone(resolveOrderOccurredAt(order), await getReportingTimezone(client));
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
export async function applySyntheticAttributionForOrder(shopifyOrderId, input, client) {
    await execute(client, async (db) => {
        const order = await fetchOrder(db, shopifyOrderId);
        if (!order) {
            throw new Error(`Shopify order ${shopifyOrderId} not found`);
        }
        const orderOccurredAt = resolveOrderOccurredAt(order);
        const confidenceScore = input.confidenceScore ?? (input.matchSource === 'shopify_hint_fallback' ? 0.4 : 0.25);
        const touchpoint = buildResolvedTouchpointFromValues({
            sessionId: null,
            sourceTouchEventId: null,
            occurredAt: input.occurredAt ?? orderOccurredAt,
            source: normalizeNullableString(input.source),
            medium: normalizeNullableString(input.medium),
            campaign: normalizeNullableString(input.campaign),
            content: normalizeNullableString(input.content),
            term: normalizeNullableString(input.term),
            clickIdType: normalizeNullableString(input.clickIdType),
            clickIdValue: normalizeNullableString(input.clickIdValue),
            attributionReason: input.attributionReason,
            matchSource: input.matchSource,
            ingestionSource: null,
            isForced: true,
            ga4ClientId: normalizeNullableString(input.ga4ClientId),
            ga4SessionId: normalizeNullableString(input.ga4SessionId)
        });
        await persistAttribution(db, order, {
            touchpoints: [touchpoint],
            winner: touchpoint,
            confidenceScore,
            confidenceLabel: confidenceLabelForScore(confidenceScore)
        }, {
            preventDowngrade: true
        });
        const metricDate = formatDateInTimezone(orderOccurredAt, await getReportingTimezone(db));
        await refreshDailyReportingMetrics(db, [metricDate]);
    });
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
    buildProcessingMetricsLog,
    dedupeDeterministicCandidates,
    selectLastNonDirectWinner,
    confidenceScoreForWinner,
    confidenceLabelForScore,
    selectGa4FallbackWinner
};
