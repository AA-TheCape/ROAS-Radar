export const CLICK_LOOKBACK_WINDOW_DAYS = 28;
export const VIEW_LOOKBACK_WINDOW_DAYS = 7;
export const CLICK_LOOKBACK_WINDOW_MS = CLICK_LOOKBACK_WINDOW_DAYS * 24 * 60 * 60 * 1000;
export const VIEW_LOOKBACK_WINDOW_MS = VIEW_LOOKBACK_WINDOW_DAYS * 24 * 60 * 60 * 1000;
export function hasClickId(clickIdValue) {
    return Boolean(clickIdValue);
}
export function qualifiesSyntheticHintSignal(input) {
    if (input.clickIdType && input.clickIdValue) {
        return true;
    }
    if (input.source && input.medium) {
        return true;
    }
    if (input.source && input.campaign) {
        return true;
    }
    return false;
}
export function isDirectTouchpoint(input) {
    return !input.source && !input.medium && !input.campaign && !input.content && !input.term && !input.clickIdValue;
}
export function inferEngagementType(input) {
    if (hasClickId(input.clickIdValue)) {
        return 'click';
    }
    if (input.engagementType === 'click' || input.engagementType === 'view' || input.engagementType === 'unknown') {
        return input.engagementType;
    }
    if (input.defaultEngagementType === 'click' ||
        input.defaultEngagementType === 'view' ||
        input.defaultEngagementType === 'unknown') {
        return input.defaultEngagementType;
    }
    return 'unknown';
}
export function isWithinLookbackWindow(orderOccurredAt, touchpointOccurredAt, engagementType) {
    const deltaMs = orderOccurredAt.getTime() - touchpointOccurredAt.getTime();
    if (deltaMs < 0) {
        return false;
    }
    if (engagementType === 'click') {
        return deltaMs <= CLICK_LOOKBACK_WINDOW_MS;
    }
    if (engagementType === 'view') {
        return deltaMs <= VIEW_LOOKBACK_WINDOW_MS;
    }
    return false;
}
