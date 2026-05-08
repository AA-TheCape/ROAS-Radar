import { ATTRIBUTION_SCHEMA_VERSION, normalizeAttributionHintInputV1, normalizeAttributionOrderInputV1, normalizeAttributionString, normalizeAttributionTouchpointInputV1 } from '../../../packages/attribution-schema/index.js';
import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
import { extractShopifyHintAttribution } from '../shopify/attribution-hints.js';
const CLICK_LOOKBACK_WINDOW_MS = 28 * 24 * 60 * 60 * 1000;
const VIEW_LOOKBACK_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const EVIDENCE_SOURCE_PRECEDENCE = {
    landing_session_id: 0,
    checkout_token: 1,
    cart_token: 2,
    customer_identity: 3,
    shopify_marketing_hint: 4,
    ga4_fallback: 5
};
const CLICK_EVENT_HINTS = new Set([
    'ad_click',
    'click',
    'checkout_started',
    'landing',
    'landing_page',
    'page_view',
    'session_start'
]);
const VIEW_EVENT_HINTS = new Set(['ad_view', 'impression', 'product_view', 'view', 'video_view']);
function normalizeNullableString(value) {
    return normalizeAttributionString(value == null ? null : String(value));
}
function normalizeIsoTimestamp(value) {
    if (!value) {
        return null;
    }
    if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? null : value.toISOString();
    }
    const normalized = value.trim();
    if (!normalized || !/(Z|[+-]\d{2}:\d{2})$/i.test(normalized)) {
        return null;
    }
    const parsed = new Date(normalized);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}
function normalizeDecimalString(value) {
    if (typeof value === 'number') {
        return Number.isFinite(value) && value >= 0 ? value.toFixed(2) : null;
    }
    const normalized = normalizeNullableString(value);
    if (!normalized || !/^\d+(?:\.\d+)?$/.test(normalized)) {
        return null;
    }
    return Number.parseFloat(normalized).toFixed(2);
}
function resolveOrderTimestampSource(order) {
    const candidates = [
        { source: 'processed_at', value: order.processedAt },
        { source: 'created_at_shopify', value: order.createdAtShopify },
        { source: 'ingested_at', value: order.ingestedAt }
    ];
    for (const candidate of candidates) {
        const normalized = normalizeIsoTimestamp(candidate.value);
        if (normalized) {
            return {
                occurredAtUtc: normalized,
                source: candidate.source
            };
        }
    }
    return {
        occurredAtUtc: null,
        source: null
    };
}
function normalizeIngestionSource(value) {
    const normalized = normalizeNullableString(value)?.toLowerCase();
    if (normalized === 'server' || normalized === 'request_query' || normalized === 'shopify_marketing_hint') {
        return normalized;
    }
    return 'browser';
}
function countMetadataFields(touchpoint) {
    return [
        touchpoint.source,
        touchpoint.medium,
        touchpoint.campaign,
        touchpoint.content,
        touchpoint.term,
        touchpoint.click_id_type,
        touchpoint.click_id_value,
        touchpoint.identity_journey_id,
        touchpoint.attribution_reason
    ].filter(Boolean).length;
}
function classifyEngagementType(input) {
    if (input.clickIdValue) {
        return 'click';
    }
    const explicit = normalizeNullableString(input.engagementType)?.toLowerCase();
    if (explicit === 'click' || explicit === 'view') {
        return explicit;
    }
    const rawEventType = normalizeNullableString(input.eventType)?.toLowerCase();
    if (rawEventType && CLICK_EVENT_HINTS.has(rawEventType)) {
        return 'click';
    }
    if (rawEventType && VIEW_EVENT_HINTS.has(rawEventType)) {
        return 'view';
    }
    if (input.rawPayload && typeof input.rawPayload === 'object') {
        const rawRecord = input.rawPayload;
        const payloadHint = normalizeNullableString(typeof rawRecord.engagement_type === 'string'
            ? rawRecord.engagement_type
            : typeof rawRecord.touch_type === 'string'
                ? rawRecord.touch_type
                : typeof rawRecord.event_type === 'string'
                    ? rawRecord.event_type
                    : null)?.toLowerCase();
        if (payloadHint === 'click' || payloadHint === 'view') {
            return payloadHint;
        }
        if (payloadHint && CLICK_EVENT_HINTS.has(payloadHint)) {
            return 'click';
        }
        if (payloadHint && VIEW_EVENT_HINTS.has(payloadHint)) {
            return 'view';
        }
    }
    if (input.sourceKind === 'shopify_hint' &&
        ((input.source && input.medium) || (input.source && input.campaign))) {
        return 'click';
    }
    return 'unknown';
}
function isDirectTouchpoint(input) {
    return !input.source && !input.medium && !input.campaign && !input.content && !input.term && !input.clickIdValue;
}
function determineEligibility(orderOccurredAtUtc, touchpointOccurredAtUtc, engagementType) {
    if (engagementType === 'unknown') {
        return {
            isEligible: false,
            ineligibilityReason: 'unknown_engagement_type'
        };
    }
    const deltaMs = new Date(orderOccurredAtUtc).getTime() - new Date(touchpointOccurredAtUtc).getTime();
    if (deltaMs < 0) {
        return {
            isEligible: false,
            ineligibilityReason: 'future_touchpoint'
        };
    }
    if (engagementType === 'click' && deltaMs > CLICK_LOOKBACK_WINDOW_MS) {
        return {
            isEligible: false,
            ineligibilityReason: 'outside_click_lookback_window'
        };
    }
    if (engagementType === 'view' && deltaMs > VIEW_LOOKBACK_WINDOW_MS) {
        return {
            isEligible: false,
            ineligibilityReason: 'outside_view_lookback_window'
        };
    }
    return {
        isEligible: true,
        ineligibilityReason: null
    };
}
function logFailure(failures, options, failure) {
    failures.push(failure);
    options?.logger?.(failure);
}
function compareTouchpointWinnerPriority(left, right) {
    if (left.touchpoint.touchpoint_occurred_at_utc !== right.touchpoint.touchpoint_occurred_at_utc) {
        return right.touchpoint.touchpoint_occurred_at_utc.localeCompare(left.touchpoint.touchpoint_occurred_at_utc);
    }
    if (left.evidenceRank !== right.evidenceRank) {
        return left.evidenceRank - right.evidenceRank;
    }
    if (left.metadataCompleteness !== right.metadataCompleteness) {
        return right.metadataCompleteness - left.metadataCompleteness;
    }
    if (left.clickIdPresent !== right.clickIdPresent) {
        return Number(right.clickIdPresent) - Number(left.clickIdPresent);
    }
    return left.touchpoint.touchpoint_id.localeCompare(right.touchpoint.touchpoint_id);
}
function compareCanonicalTouchpointOrder(left, right) {
    if (left.touchpoint_occurred_at_utc !== right.touchpoint_occurred_at_utc) {
        return left.touchpoint_occurred_at_utc.localeCompare(right.touchpoint_occurred_at_utc);
    }
    const leftEvidence = EVIDENCE_SOURCE_PRECEDENCE[left.evidence_source];
    const rightEvidence = EVIDENCE_SOURCE_PRECEDENCE[right.evidence_source];
    if (leftEvidence !== rightEvidence) {
        return leftEvidence - rightEvidence;
    }
    if (left.engagement_type !== right.engagement_type) {
        if (left.engagement_type === 'click') {
            return -1;
        }
        if (right.engagement_type === 'click') {
            return 1;
        }
    }
    if (Boolean(left.click_id_value) !== Boolean(right.click_id_value)) {
        return Number(Boolean(right.click_id_value)) - Number(Boolean(left.click_id_value));
    }
    return left.touchpoint_id.localeCompare(right.touchpoint_id);
}
function buildHintInput(payload) {
    const hint = extractShopifyHintAttribution(payload);
    if (!hint) {
        return null;
    }
    const confidenceLabel = hint.clickIdValue ? 'medium' : 'low';
    const hintType = Array.isArray(payload.note_attributes) && payload.note_attributes.length > 0
        ? 'note_attributes'
        : Array.isArray(payload.attributes) && payload.attributes.length > 0
            ? 'attributes_array'
            : 'landing_site';
    const rawHintKeys = [
        ...(payload.note_attributes ?? []).map((attribute) => normalizeNullableString(attribute.name)).filter(Boolean),
        ...(payload.attributes ?? []).map((attribute) => normalizeNullableString(attribute.name)).filter(Boolean)
    ];
    return normalizeAttributionHintInputV1({
        hint_source_system: 'shopify_order',
        hint_type: hintType,
        source: hint.source,
        medium: hint.medium,
        campaign: hint.campaign,
        content: hint.content,
        term: hint.term,
        click_id_type: hint.clickIdType,
        click_id_value: hint.clickIdValue,
        hint_confidence_score: hint.confidenceScore.toFixed(2),
        hint_confidence_label: confidenceLabel,
        raw_hint_keys: Array.from(new Set(rawHintKeys)).sort()
    });
}
function resolveMatchedSessionContexts(order, sessionIdentityById, eventsBySessionId, journeyBySessionId) {
    const sessionIds = new Set();
    const normalizedCheckoutToken = normalizeNullableString(order.checkoutToken);
    const normalizedCartToken = normalizeNullableString(order.cartToken);
    const normalizedLandingSessionId = normalizeNullableString(order.landingSessionId);
    const normalizedCustomerId = normalizeNullableString(order.shopifyCustomerId);
    const normalizedEmailHash = normalizeNullableString(order.emailHash)?.toLowerCase() ?? null;
    const normalizedIdentityJourneyId = normalizeNullableString(order.identityJourneyId);
    if (normalizedLandingSessionId) {
        sessionIds.add(normalizedLandingSessionId);
    }
    for (const [sessionId, events] of eventsBySessionId.entries()) {
        const tokenMatched = events.some((event) => {
            const eventCheckoutToken = normalizeNullableString(event.shopifyCheckoutToken);
            const eventCartToken = normalizeNullableString(event.shopifyCartToken);
            return ((normalizedCheckoutToken && normalizedCheckoutToken === eventCheckoutToken) ||
                (normalizedCartToken && normalizedCartToken === eventCartToken));
        });
        if (tokenMatched) {
            sessionIds.add(sessionId);
        }
    }
    for (const [sessionId, identity] of sessionIdentityById.entries()) {
        const journey = journeyBySessionId.get(sessionId);
        const customerIdentityId = normalizeNullableString(identity.customerIdentityId);
        const sessionEmailHash = normalizeNullableString(journey?.primaryEmailHash ?? identity.emailHash)?.toLowerCase() ?? null;
        const identityJourneyId = normalizeNullableString(journey?.identityJourneyId ?? identity.identityJourneyId);
        if ((normalizedCustomerId && normalizedCustomerId === customerIdentityId) ||
            (normalizedEmailHash && normalizedEmailHash === sessionEmailHash) ||
            (normalizedIdentityJourneyId && normalizedIdentityJourneyId === identityJourneyId)) {
            sessionIds.add(sessionId);
        }
    }
    return Array.from(sessionIds)
        .sort()
        .map((sessionId) => {
        const identity = sessionIdentityById.get(sessionId) ?? null;
        const journey = journeyBySessionId.get(sessionId);
        return {
            sessionId,
            identity,
            identityJourneyId: normalizeNullableString(journey?.identityJourneyId ?? identity?.identityJourneyId),
            emailHash: normalizeNullableString(journey?.primaryEmailHash ?? identity?.emailHash)?.toLowerCase() ?? null,
            customerIdentityId: normalizeNullableString(journey?.authoritativeShopifyCustomerId ?? identity?.customerIdentityId)
        };
    });
}
function determineEvidenceSource(order, sessionContext, event) {
    const landingSessionId = normalizeNullableString(order.landingSessionId);
    if (landingSessionId && sessionContext.sessionId === landingSessionId) {
        return {
            evidenceSource: 'landing_session_id',
            attributionReason: 'matched_by_landing_session_id'
        };
    }
    const checkoutToken = normalizeNullableString(order.checkoutToken);
    const cartToken = normalizeNullableString(order.cartToken);
    if (event) {
        if (checkoutToken && checkoutToken === normalizeNullableString(event.shopifyCheckoutToken)) {
            return {
                evidenceSource: 'checkout_token',
                attributionReason: 'matched_by_checkout_token'
            };
        }
        if (cartToken && cartToken === normalizeNullableString(event.shopifyCartToken)) {
            return {
                evidenceSource: 'cart_token',
                attributionReason: 'matched_by_cart_token'
            };
        }
    }
    const shopifyCustomerId = normalizeNullableString(order.shopifyCustomerId);
    const emailHash = normalizeNullableString(order.emailHash)?.toLowerCase() ?? null;
    const identityJourneyId = normalizeNullableString(order.identityJourneyId);
    if ((shopifyCustomerId && shopifyCustomerId === sessionContext.customerIdentityId) ||
        (emailHash && emailHash === sessionContext.emailHash) ||
        (identityJourneyId && identityJourneyId === sessionContext.identityJourneyId)) {
        return {
            evidenceSource: 'customer_identity',
            attributionReason: identityJourneyId ? 'matched_by_identity_journey' : 'matched_by_customer_identity'
        };
    }
    return null;
}
function buildFirstTouchCandidate(order, orderOccurredAtUtc, sessionContext) {
    const identity = sessionContext.identity;
    if (!identity) {
        return null;
    }
    const touchpointOccurredAtUtc = normalizeIsoTimestamp(identity.firstCapturedAt);
    const touchpointCapturedAtUtc = normalizeIsoTimestamp(identity.firstCapturedAt ?? identity.lastCapturedAt);
    if (!touchpointOccurredAtUtc || !touchpointCapturedAtUtc) {
        return null;
    }
    const canonicalDimensions = buildCanonicalTouchpointDimensions({
        source: identity.initialUtmSource,
        medium: identity.initialUtmMedium,
        campaign: identity.initialUtmCampaign,
        content: identity.initialUtmContent,
        term: identity.initialUtmTerm,
        gclid: identity.initialGclid,
        gbraid: identity.initialGbraid,
        wbraid: identity.initialWbraid,
        fbclid: identity.initialFbclid,
        ttclid: identity.initialTtclid,
        msclkid: identity.initialMsclkid
    });
    const evidence = determineEvidenceSource(order, sessionContext, null);
    if (!evidence) {
        return null;
    }
    const engagementType = classifyEngagementType({
        eventType: 'session_start',
        clickIdValue: canonicalDimensions.clickIdValue,
        sourceKind: 'session_first_touch',
        source: canonicalDimensions.source,
        medium: canonicalDimensions.medium,
        campaign: canonicalDimensions.campaign
    });
    const eligibility = determineEligibility(orderOccurredAtUtc, touchpointOccurredAtUtc, engagementType);
    const touchpoint = normalizeAttributionTouchpointInputV1({
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        touchpoint_id: `session:${sessionContext.sessionId}:first_touch`,
        session_id: sessionContext.sessionId,
        identity_journey_id: sessionContext.identityJourneyId,
        touchpoint_occurred_at_utc: touchpointOccurredAtUtc,
        touchpoint_captured_at_utc: touchpointCapturedAtUtc,
        touchpoint_source_kind: 'session_first_touch',
        ingestion_source: 'browser',
        source: canonicalDimensions.source,
        medium: canonicalDimensions.medium,
        campaign: canonicalDimensions.campaign,
        content: canonicalDimensions.content,
        term: canonicalDimensions.term,
        click_id_type: canonicalDimensions.clickIdType,
        click_id_value: canonicalDimensions.clickIdValue,
        evidence_source: evidence.evidenceSource,
        is_direct: isDirectTouchpoint({
            source: canonicalDimensions.source,
            medium: canonicalDimensions.medium,
            campaign: canonicalDimensions.campaign,
            content: canonicalDimensions.content,
            term: canonicalDimensions.term,
            clickIdValue: canonicalDimensions.clickIdValue
        }),
        engagement_type: engagementType,
        is_synthetic: false,
        is_eligible: eligibility.isEligible,
        ineligibility_reason: eligibility.ineligibilityReason,
        attribution_reason: evidence.attributionReason,
        attribution_hint: null
    });
    return {
        orderId: order.shopifyOrderId,
        touchpoint,
        evidenceRank: EVIDENCE_SOURCE_PRECEDENCE[evidence.evidenceSource],
        clickIdPresent: Boolean(touchpoint.click_id_value),
        metadataCompleteness: countMetadataFields(touchpoint)
    };
}
function buildEventCandidate(order, orderOccurredAtUtc, sessionContext, event) {
    const evidence = determineEvidenceSource(order, sessionContext, event);
    if (!evidence) {
        return null;
    }
    const touchpointOccurredAtUtc = normalizeIsoTimestamp(event.occurredAt);
    const touchpointCapturedAtUtc = normalizeIsoTimestamp(event.capturedAt ?? event.occurredAt);
    if (!touchpointOccurredAtUtc || !touchpointCapturedAtUtc) {
        return null;
    }
    const canonicalDimensions = buildCanonicalTouchpointDimensions({
        source: event.utmSource,
        medium: event.utmMedium,
        campaign: event.utmCampaign,
        content: event.utmContent,
        term: event.utmTerm,
        gclid: event.gclid,
        gbraid: event.gbraid,
        wbraid: event.wbraid,
        fbclid: event.fbclid,
        ttclid: event.ttclid,
        msclkid: event.msclkid
    });
    const engagementType = classifyEngagementType({
        eventType: event.eventType,
        engagementType: event.engagementType,
        rawPayload: event.rawPayload,
        clickIdValue: canonicalDimensions.clickIdValue,
        sourceKind: 'session_event',
        source: canonicalDimensions.source,
        medium: canonicalDimensions.medium,
        campaign: canonicalDimensions.campaign
    });
    const eligibility = determineEligibility(orderOccurredAtUtc, touchpointOccurredAtUtc, engagementType);
    const touchpoint = normalizeAttributionTouchpointInputV1({
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        touchpoint_id: `event:${event.touchEventId}`,
        session_id: sessionContext.sessionId,
        identity_journey_id: sessionContext.identityJourneyId,
        touchpoint_occurred_at_utc: touchpointOccurredAtUtc,
        touchpoint_captured_at_utc: touchpointCapturedAtUtc,
        touchpoint_source_kind: 'session_event',
        ingestion_source: normalizeIngestionSource(event.ingestionSource),
        source: canonicalDimensions.source,
        medium: canonicalDimensions.medium,
        campaign: canonicalDimensions.campaign,
        content: canonicalDimensions.content,
        term: canonicalDimensions.term,
        click_id_type: canonicalDimensions.clickIdType,
        click_id_value: canonicalDimensions.clickIdValue,
        evidence_source: evidence.evidenceSource,
        is_direct: isDirectTouchpoint({
            source: canonicalDimensions.source,
            medium: canonicalDimensions.medium,
            campaign: canonicalDimensions.campaign,
            content: canonicalDimensions.content,
            term: canonicalDimensions.term,
            clickIdValue: canonicalDimensions.clickIdValue
        }),
        engagement_type: engagementType,
        is_synthetic: false,
        is_eligible: eligibility.isEligible,
        ineligibility_reason: eligibility.ineligibilityReason,
        attribution_reason: evidence.attributionReason,
        attribution_hint: null
    });
    return {
        orderId: order.shopifyOrderId,
        touchpoint,
        evidenceRank: EVIDENCE_SOURCE_PRECEDENCE[evidence.evidenceSource],
        clickIdPresent: Boolean(touchpoint.click_id_value),
        metadataCompleteness: countMetadataFields(touchpoint)
    };
}
function buildHintCandidate(order, orderOccurredAtUtc, hint) {
    const engagementType = classifyEngagementType({
        sourceKind: 'shopify_hint',
        clickIdValue: hint.click_id_value,
        source: hint.source,
        medium: hint.medium,
        campaign: hint.campaign
    });
    const eligibility = determineEligibility(orderOccurredAtUtc, orderOccurredAtUtc, engagementType);
    const touchpoint = normalizeAttributionTouchpointInputV1({
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        touchpoint_id: `shopify_hint:${order.shopifyOrderId}`,
        session_id: null,
        identity_journey_id: normalizeNullableString(order.identityJourneyId),
        touchpoint_occurred_at_utc: orderOccurredAtUtc,
        touchpoint_captured_at_utc: orderOccurredAtUtc,
        touchpoint_source_kind: 'shopify_hint',
        ingestion_source: 'shopify_marketing_hint',
        source: hint.source,
        medium: hint.medium,
        campaign: hint.campaign,
        content: hint.content,
        term: hint.term,
        click_id_type: hint.click_id_type,
        click_id_value: hint.click_id_value,
        evidence_source: 'shopify_marketing_hint',
        is_direct: isDirectTouchpoint({
            source: hint.source,
            medium: hint.medium,
            campaign: hint.campaign,
            content: hint.content,
            term: hint.term,
            clickIdValue: hint.click_id_value
        }),
        engagement_type: engagementType,
        is_synthetic: true,
        is_eligible: eligibility.isEligible,
        ineligibility_reason: eligibility.ineligibilityReason,
        attribution_reason: 'shopify_hint_derived',
        attribution_hint: hint
    });
    return {
        orderId: order.shopifyOrderId,
        touchpoint,
        evidenceRank: EVIDENCE_SOURCE_PRECEDENCE.shopify_marketing_hint,
        clickIdPresent: Boolean(touchpoint.click_id_value),
        metadataCompleteness: countMetadataFields(touchpoint)
    };
}
function dedupeCandidateTouchpoints(candidates, failures, options) {
    const byTouchpointId = new Map();
    for (const candidate of candidates) {
        const existing = byTouchpointId.get(candidate.touchpoint.touchpoint_id);
        if (!existing) {
            byTouchpointId.set(candidate.touchpoint.touchpoint_id, candidate);
            continue;
        }
        const keepCandidate = compareTouchpointWinnerPriority(candidate, existing) < 0;
        const discarded = keepCandidate ? existing : candidate;
        const kept = keepCandidate ? candidate : existing;
        byTouchpointId.set(candidate.touchpoint.touchpoint_id, kept);
        logFailure(failures, options, {
            scope: 'touchpoint',
            orderId: discarded.orderId,
            touchpointId: discarded.touchpoint.touchpoint_id,
            sessionId: discarded.touchpoint.session_id,
            reasonCode: 'duplicate_touchpoint_dropped',
            details: {
                keptTouchpointId: kept.touchpoint.touchpoint_id,
                evidenceSource: discarded.touchpoint.evidence_source
            }
        });
    }
    return Array.from(byTouchpointId.values())
        .map((candidate) => candidate.touchpoint)
        .sort(compareCanonicalTouchpointOrder);
}
export function preprocessAttributionSnapshot(snapshot, options) {
    const failures = [];
    const orders = [];
    const touchpointCandidates = [];
    const sessionIdentityById = new Map();
    for (const identity of snapshot.sessionIdentities) {
        sessionIdentityById.set(identity.sessionId, identity);
    }
    const eventsBySessionId = new Map();
    for (const event of snapshot.touchEvents) {
        const sessionEvents = eventsBySessionId.get(event.sessionId) ?? [];
        sessionEvents.push(event);
        eventsBySessionId.set(event.sessionId, sessionEvents);
    }
    const journeyBySessionId = new Map();
    for (const journey of snapshot.journeySessions ?? []) {
        journeyBySessionId.set(journey.sessionId, journey);
    }
    for (const order of snapshot.orders.slice().sort((left, right) => left.shopifyOrderId.localeCompare(right.shopifyOrderId))) {
        const { occurredAtUtc, source } = resolveOrderTimestampSource(order);
        if (!occurredAtUtc || !source) {
            logFailure(failures, options, {
                scope: 'order',
                orderId: order.shopifyOrderId,
                touchpointId: null,
                sessionId: normalizeNullableString(order.landingSessionId),
                reasonCode: 'missing_order_timestamp',
                details: {}
            });
            continue;
        }
        const subtotalAmount = normalizeDecimalString(order.subtotalAmount);
        const totalAmount = normalizeDecimalString(order.totalAmount);
        const currencyCode = normalizeNullableString(order.currencyCode)?.toUpperCase() ?? null;
        if (!subtotalAmount || !totalAmount || !currencyCode) {
            logFailure(failures, options, {
                scope: 'order',
                orderId: order.shopifyOrderId,
                touchpointId: null,
                sessionId: normalizeNullableString(order.landingSessionId),
                reasonCode: 'invalid_order_monetary_fields',
                details: {
                    currencyCode,
                    subtotalAmount,
                    totalAmount
                }
            });
            continue;
        }
        const normalizedOrder = normalizeAttributionOrderInputV1({
            schema_version: ATTRIBUTION_SCHEMA_VERSION,
            order_id: order.shopifyOrderId,
            order_platform: 'shopify',
            order_occurred_at_utc: occurredAtUtc,
            order_timestamp_source: source,
            currency_code: currencyCode,
            subtotal_amount: subtotalAmount,
            total_amount: totalAmount,
            landing_session_id: normalizeNullableString(order.landingSessionId),
            checkout_token: normalizeNullableString(order.checkoutToken),
            cart_token: normalizeNullableString(order.cartToken),
            shopify_customer_id: normalizeNullableString(order.shopifyCustomerId),
            email_hash: normalizeNullableString(order.emailHash)?.toLowerCase() ?? null,
            source_name: normalizeNullableString(order.sourceName),
            identity_journey_id: normalizeNullableString(order.identityJourneyId),
            raw_order_ref: order.rawPayload && typeof order.rawPayload === 'object'
                ? {
                    source: 'shopify_orders.raw_payload'
                }
                : null
        });
        orders.push(normalizedOrder);
        const matchedSessions = resolveMatchedSessionContexts(order, sessionIdentityById, eventsBySessionId, journeyBySessionId);
        if (normalizeNullableString(order.landingSessionId) && !sessionIdentityById.has(String(order.landingSessionId))) {
            logFailure(failures, options, {
                scope: 'touchpoint',
                orderId: order.shopifyOrderId,
                touchpointId: null,
                sessionId: normalizeNullableString(order.landingSessionId),
                reasonCode: 'missing_session_identity',
                details: {
                    matchSource: 'landing_session_id'
                }
            });
        }
        for (const sessionContext of matchedSessions) {
            const firstTouchCandidate = buildFirstTouchCandidate(order, occurredAtUtc, sessionContext);
            if (firstTouchCandidate) {
                touchpointCandidates.push(firstTouchCandidate);
            }
            else if (!sessionContext.identity) {
                logFailure(failures, options, {
                    scope: 'touchpoint',
                    orderId: order.shopifyOrderId,
                    touchpointId: null,
                    sessionId: sessionContext.sessionId,
                    reasonCode: 'missing_session_identity',
                    details: {
                        matchSource: 'event_only_session'
                    }
                });
            }
            for (const event of (eventsBySessionId.get(sessionContext.sessionId) ?? []).slice().sort((left, right) => left.touchEventId.localeCompare(right.touchEventId))) {
                const eventCandidate = buildEventCandidate(order, occurredAtUtc, sessionContext, event);
                if (!eventCandidate) {
                    continue;
                }
                touchpointCandidates.push(eventCandidate);
            }
        }
        if (order.rawPayload && typeof order.rawPayload === 'object') {
            const hint = buildHintInput(order.rawPayload);
            if (hint) {
                touchpointCandidates.push(buildHintCandidate(order, occurredAtUtc, hint));
            }
        }
        else if (order.rawPayload != null) {
            logFailure(failures, options, {
                scope: 'hint',
                orderId: order.shopifyOrderId,
                touchpointId: null,
                sessionId: null,
                reasonCode: 'invalid_shopify_payload_shape',
                details: {}
            });
        }
    }
    const touchpoints = dedupeCandidateTouchpoints(touchpointCandidates, failures, options);
    return {
        orders,
        touchpoints,
        failures
    };
}
export async function loadAttributionPreprocessingSnapshot(client, orderIds) {
    const normalizedOrderIds = Array.from(new Set(orderIds.map((orderId) => normalizeNullableString(orderId)).filter(Boolean))).sort();
    if (normalizedOrderIds.length === 0) {
        return {
            orders: [],
            sessionIdentities: [],
            touchEvents: [],
            journeySessions: []
        };
    }
    const ordersResult = await client.query(`
      SELECT
        shopify_order_id,
        processed_at,
        created_at_shopify,
        ingested_at,
        currency AS currency_code,
        subtotal_price::text AS subtotal_amount,
        total_price::text AS total_amount,
        landing_session_id::text,
        checkout_token,
        cart_token,
        customer_identity_id::text AS shopify_customer_id,
        email_hash,
        source_name,
        identity_journey_id::text,
        raw_payload
      FROM shopify_orders
      WHERE shopify_order_id = ANY($1::text[])
      ORDER BY shopify_order_id ASC
    `, [normalizedOrderIds]);
    const orders = ordersResult.rows.map((row) => ({
        shopifyOrderId: row.shopify_order_id,
        processedAt: row.processed_at,
        createdAtShopify: row.created_at_shopify,
        ingestedAt: row.ingested_at,
        currencyCode: row.currency_code,
        subtotalAmount: row.subtotal_amount,
        totalAmount: row.total_amount,
        landingSessionId: row.landing_session_id,
        checkoutToken: row.checkout_token,
        cartToken: row.cart_token,
        shopifyCustomerId: row.shopify_customer_id,
        emailHash: row.email_hash,
        sourceName: row.source_name,
        identityJourneyId: row.identity_journey_id,
        rawPayload: row.raw_payload
    }));
    const landingSessionIds = Array.from(new Set(orders.map((order) => normalizeNullableString(order.landingSessionId)).filter(Boolean)));
    const checkoutTokens = Array.from(new Set(orders.map((order) => normalizeNullableString(order.checkoutToken)).filter(Boolean)));
    const cartTokens = Array.from(new Set(orders.map((order) => normalizeNullableString(order.cartToken)).filter(Boolean)));
    const shopifyCustomerIds = Array.from(new Set(orders.map((order) => normalizeNullableString(order.shopifyCustomerId)).filter(Boolean)));
    const emailHashes = Array.from(new Set(orders.map((order) => normalizeNullableString(order.emailHash)?.toLowerCase() ?? null).filter(Boolean)));
    const identityJourneyIds = Array.from(new Set(orders.map((order) => normalizeNullableString(order.identityJourneyId)).filter(Boolean)));
    const journeyRows = await client.query(`
      SELECT
        session_id::text AS "sessionId",
        identity_journey_id::text AS "identityJourneyId",
        authoritative_shopify_customer_id AS "authoritativeShopifyCustomerId",
        primary_email_hash AS "primaryEmailHash"
      FROM customer_journey
      WHERE (
        cardinality($1::uuid[]) > 0 AND identity_journey_id = ANY($1::uuid[])
      ) OR (
        cardinality($2::text[]) > 0 AND authoritative_shopify_customer_id = ANY($2::text[])
      ) OR (
        cardinality($3::text[]) > 0 AND primary_email_hash = ANY($3::text[])
      )
    `, [identityJourneyIds, shopifyCustomerIds, emailHashes]);
    const journeySessionIds = journeyRows.rows.map((row) => row.sessionId);
    const allSessionIds = Array.from(new Set([...landingSessionIds, ...journeySessionIds])).sort();
    const touchEventRows = await client.query(`
      SELECT
        id::text AS touch_event_id,
        roas_radar_session_id::text AS session_id,
        occurred_at,
        captured_at,
        event_type,
        ingestion_source,
        page_url,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        fbclid,
        ttclid,
        msclkid,
        shopify_checkout_token,
        shopify_cart_token,
        raw_payload
      FROM session_attribution_touch_events
      WHERE (
        cardinality($1::uuid[]) > 0 AND roas_radar_session_id = ANY($1::uuid[])
      ) OR (
        cardinality($2::text[]) > 0 AND shopify_checkout_token = ANY($2::text[])
      ) OR (
        cardinality($3::text[]) > 0 AND shopify_cart_token = ANY($3::text[])
      )
      ORDER BY occurred_at ASC, id ASC
    `, [allSessionIds, checkoutTokens, cartTokens]);
    for (const row of touchEventRows.rows) {
        allSessionIds.push(row.session_id);
    }
    const sessionIdentityRows = await client.query(`
      SELECT
        i.roas_radar_session_id::text AS session_id,
        i.customer_identity_id::text,
        j.identity_journey_id::text,
        j.primary_email_hash AS email_hash,
        i.first_captured_at,
        i.last_captured_at,
        i.landing_url,
        i.referrer_url,
        i.initial_utm_source,
        i.initial_utm_medium,
        i.initial_utm_campaign,
        i.initial_utm_content,
        i.initial_utm_term,
        i.initial_gclid,
        i.initial_gbraid,
        i.initial_wbraid,
        i.initial_fbclid,
        i.initial_ttclid,
        i.initial_msclkid
      FROM session_attribution_identities i
      LEFT JOIN customer_journey j
        ON j.session_id = i.roas_radar_session_id
      WHERE (
        cardinality($1::uuid[]) > 0 AND i.roas_radar_session_id = ANY($1::uuid[])
      ) OR (
        cardinality($2::text[]) > 0 AND i.customer_identity_id::text = ANY($2::text[])
      )
      ORDER BY i.roas_radar_session_id ASC
    `, [Array.from(new Set(allSessionIds)).sort(), shopifyCustomerIds]);
    return {
        orders,
        sessionIdentities: sessionIdentityRows.rows.map((row) => ({
            sessionId: row.session_id,
            customerIdentityId: row.customer_identity_id,
            identityJourneyId: row.identity_journey_id,
            emailHash: row.email_hash,
            firstCapturedAt: row.first_captured_at,
            lastCapturedAt: row.last_captured_at,
            landingUrl: row.landing_url,
            referrerUrl: row.referrer_url,
            initialUtmSource: row.initial_utm_source,
            initialUtmMedium: row.initial_utm_medium,
            initialUtmCampaign: row.initial_utm_campaign,
            initialUtmContent: row.initial_utm_content,
            initialUtmTerm: row.initial_utm_term,
            initialGclid: row.initial_gclid,
            initialGbraid: row.initial_gbraid,
            initialWbraid: row.initial_wbraid,
            initialFbclid: row.initial_fbclid,
            initialTtclid: row.initial_ttclid,
            initialMsclkid: row.initial_msclkid
        })),
        touchEvents: touchEventRows.rows.map((row) => ({
            touchEventId: row.touch_event_id,
            sessionId: row.session_id,
            occurredAt: row.occurred_at,
            capturedAt: row.captured_at,
            eventType: row.event_type,
            ingestionSource: row.ingestion_source,
            pageUrl: row.page_url,
            referrerUrl: row.referrer_url,
            utmSource: row.utm_source,
            utmMedium: row.utm_medium,
            utmCampaign: row.utm_campaign,
            utmContent: row.utm_content,
            utmTerm: row.utm_term,
            gclid: row.gclid,
            gbraid: row.gbraid,
            wbraid: row.wbraid,
            fbclid: row.fbclid,
            ttclid: row.ttclid,
            msclkid: row.msclkid,
            shopifyCheckoutToken: row.shopify_checkout_token,
            shopifyCartToken: row.shopify_cart_token,
            rawPayload: row.raw_payload
        })),
        journeySessions: journeyRows.rows
    };
}
export async function preprocessAttributionOrders(client, orderIds, options) {
    const snapshot = await loadAttributionPreprocessingSnapshot(client, orderIds);
    return preprocessAttributionSnapshot(snapshot, options);
}
