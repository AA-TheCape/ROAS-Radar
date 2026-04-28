import { normalizeAttributionString, normalizeAttributionUrl } from '../../../packages/attribution-schema/index.js';
import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
function normalizeNullableString(value) {
    return normalizeAttributionString(value);
}
function stringifyAttributeValue(value) {
    if (typeof value !== 'string') {
        return null;
    }
    const normalized = value.trim();
    return normalized ? normalized : null;
}
function normalizeAttributeArray(input) {
    if (!Array.isArray(input)) {
        return [];
    }
    return input.filter((entry) => typeof entry === 'object' && entry !== null);
}
function getAttributeValue(attributes, key) {
    const normalizedKey = key.trim().toLowerCase();
    for (const attribute of attributes) {
        const rawKey = stringifyAttributeValue(attribute.name ?? attribute.key);
        if (!rawKey || rawKey.trim().toLowerCase() !== normalizedKey) {
            continue;
        }
        return stringifyAttributeValue(attribute.value);
    }
    return null;
}
function getAttributeValueFromKeys(attributes, keys) {
    for (const key of keys) {
        const value = getAttributeValue(attributes, key);
        if (value) {
            return value;
        }
    }
    return null;
}
function hasAttributionDimensions(value) {
    return Boolean(value.source ||
        value.medium ||
        value.campaign ||
        value.content ||
        value.term ||
        value.clickIdType ||
        value.clickIdValue);
}
export function extractShopifyHintAttribution(payload) {
    if (!payload || typeof payload !== 'object') {
        return null;
    }
    const record = payload;
    const noteAttributes = normalizeAttributeArray(record.note_attributes);
    const legacyAttributes = normalizeAttributeArray(record.attributes);
    const rawDimensions = {
        source: getAttributeValueFromKeys(noteAttributes, ['utm_source', 'roas_radar_utm_source']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_source', 'roas_radar_utm_source']),
        medium: getAttributeValueFromKeys(noteAttributes, ['utm_medium', 'roas_radar_utm_medium']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_medium', 'roas_radar_utm_medium']),
        campaign: getAttributeValueFromKeys(noteAttributes, ['utm_campaign', 'roas_radar_utm_campaign']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_campaign', 'roas_radar_utm_campaign']),
        content: getAttributeValueFromKeys(noteAttributes, ['utm_content', 'roas_radar_utm_content']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_content', 'roas_radar_utm_content']),
        term: getAttributeValueFromKeys(noteAttributes, ['utm_term', 'roas_radar_utm_term']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_term', 'roas_radar_utm_term']),
        gclid: getAttributeValueFromKeys(noteAttributes, ['gclid', 'roas_radar_gclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['gclid', 'roas_radar_gclid']),
        gbraid: getAttributeValueFromKeys(noteAttributes, ['gbraid', 'roas_radar_gbraid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['gbraid', 'roas_radar_gbraid']),
        wbraid: getAttributeValueFromKeys(noteAttributes, ['wbraid', 'roas_radar_wbraid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['wbraid', 'roas_radar_wbraid']),
        fbclid: getAttributeValueFromKeys(noteAttributes, ['fbclid', 'roas_radar_fbclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['fbclid', 'roas_radar_fbclid']),
        ttclid: getAttributeValueFromKeys(noteAttributes, ['ttclid', 'roas_radar_ttclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['ttclid', 'roas_radar_ttclid']),
        msclkid: getAttributeValueFromKeys(noteAttributes, ['msclkid', 'roas_radar_msclkid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['msclkid', 'roas_radar_msclkid'])
    };
    const hintCandidates = [
        typeof record.landing_site === 'string' ? record.landing_site : null,
        getAttributeValueFromKeys(noteAttributes, ['landing_url', 'page_url', 'roas_radar_landing_path', 'landing_site']),
        getAttributeValueFromKeys(legacyAttributes, ['landing_url', 'page_url', 'roas_radar_landing_path', 'landing_site'])
    ].filter((value) => Boolean(value));
    for (const candidate of hintCandidates) {
        try {
            const url = new URL(normalizeAttributionUrl(candidate, 'https://shopify-hint.local') ?? candidate);
            rawDimensions.source ??= normalizeNullableString(url.searchParams.get('utm_source'));
            rawDimensions.medium ??= normalizeNullableString(url.searchParams.get('utm_medium'));
            rawDimensions.campaign ??= normalizeNullableString(url.searchParams.get('utm_campaign'));
            rawDimensions.content ??= normalizeNullableString(url.searchParams.get('utm_content'));
            rawDimensions.term ??= normalizeNullableString(url.searchParams.get('utm_term'));
            rawDimensions.gclid ??= normalizeNullableString(url.searchParams.get('gclid'));
            rawDimensions.gbraid ??= normalizeNullableString(url.searchParams.get('gbraid'));
            rawDimensions.wbraid ??= normalizeNullableString(url.searchParams.get('wbraid'));
            rawDimensions.fbclid ??= normalizeNullableString(url.searchParams.get('fbclid'));
            rawDimensions.ttclid ??= normalizeNullableString(url.searchParams.get('ttclid'));
            rawDimensions.msclkid ??= normalizeNullableString(url.searchParams.get('msclkid'));
        }
        catch { }
    }
    const canonicalDimensions = buildCanonicalTouchpointDimensions({
        source: rawDimensions.source,
        medium: rawDimensions.medium,
        campaign: rawDimensions.campaign,
        content: rawDimensions.content,
        term: rawDimensions.term,
        gclid: rawDimensions.gclid,
        gbraid: rawDimensions.gbraid,
        wbraid: rawDimensions.wbraid,
        fbclid: rawDimensions.fbclid,
        ttclid: rawDimensions.ttclid,
        msclkid: rawDimensions.msclkid
    });
    if (!hasAttributionDimensions(canonicalDimensions)) {
        return null;
    }
    return {
        ...canonicalDimensions,
        confidenceScore: canonicalDimensions.clickIdValue ? 0.55 : 0.4
    };
}
