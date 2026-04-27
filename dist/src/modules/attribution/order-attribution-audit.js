export const ORDER_ATTRIBUTION_TIERS = [
    'deterministic_first_party',
    'deterministic_shopify_hint',
    'ga4_fallback',
    'unattributed'
];
function mapDeterministicSource(source) {
    switch (source) {
        case 'landing_session_id':
            return 'landing_session_id';
        case 'checkout_token':
            return 'checkout_token';
        case 'cart_token':
            return 'cart_token';
        case 'customer_identity':
            return 'stitched_identity_journey';
    }
}
function mapAttributionSource(source) {
    switch (source) {
        case 'shopify_marketing_hint':
            return 'shopify_marketing_hint';
        case 'ga4_fallback':
            return 'ga4_fallback';
        default:
            return mapDeterministicSource(source);
    }
}
export function buildOrderAttributionAuditRecord(winner, matchedAt) {
    if (!winner || winner.attributionReason === 'unattributed') {
        return {
            tier: 'unattributed',
            source: 'unattributed',
            matchedAt,
            reason: 'unattributed'
        };
    }
    if (winner.ingestionSource === 'shopify_marketing_hint' || winner.attributionReason === 'shopify_hint_derived') {
        return {
            tier: 'deterministic_shopify_hint',
            source: 'shopify_marketing_hint',
            matchedAt,
            reason: winner.attributionReason
        };
    }
    if (winner.ingestionSource === 'ga4_fallback') {
        return {
            tier: 'ga4_fallback',
            source: 'ga4_fallback',
            matchedAt,
            reason: winner.attributionReason
        };
    }
    if (!winner.ingestionSource) {
        throw new Error('Deterministic attribution winner is missing an ingestion source');
    }
    return {
        tier: 'deterministic_first_party',
        source: mapAttributionSource(winner.ingestionSource),
        matchedAt,
        reason: winner.attributionReason
    };
}
