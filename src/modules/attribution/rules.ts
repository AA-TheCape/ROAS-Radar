export type AttributionEngagementType = 'click' | 'view' | 'unknown';

export const DEFAULT_CLICK_LOOKBACK_WINDOW_DAYS = 30;
export const DEFAULT_VIEW_LOOKBACK_WINDOW_DAYS = 7;
export const CLICK_LOOKBACK_WINDOW_DAYS = DEFAULT_CLICK_LOOKBACK_WINDOW_DAYS;
export const VIEW_LOOKBACK_WINDOW_DAYS = DEFAULT_VIEW_LOOKBACK_WINDOW_DAYS;

export type AttributionLookbackWindows = {
  clickWindowDays: number;
  viewWindowDays: number;
};

export const DEFAULT_ATTRIBUTION_LOOKBACK_WINDOWS: AttributionLookbackWindows = {
  clickWindowDays: DEFAULT_CLICK_LOOKBACK_WINDOW_DAYS,
  viewWindowDays: DEFAULT_VIEW_LOOKBACK_WINDOW_DAYS
};

export function normalizeAttributionLookbackWindows(
  windows: Partial<AttributionLookbackWindows> | undefined
): AttributionLookbackWindows {
  const clickWindowDays = windows?.clickWindowDays ?? DEFAULT_CLICK_LOOKBACK_WINDOW_DAYS;
  const viewWindowDays = windows?.viewWindowDays ?? DEFAULT_VIEW_LOOKBACK_WINDOW_DAYS;

  if (!Number.isFinite(clickWindowDays) || clickWindowDays < 0) {
    throw new Error(`clickWindowDays must be a finite non-negative number, received ${String(clickWindowDays)}`);
  }

  if (!Number.isFinite(viewWindowDays) || viewWindowDays < 0) {
    throw new Error(`viewWindowDays must be a finite non-negative number, received ${String(viewWindowDays)}`);
  }

  return {
    clickWindowDays,
    viewWindowDays
  };
}

export function lookbackWindowMs(days: number): number {
  return days * 24 * 60 * 60 * 1000;
}

export const CLICK_LOOKBACK_WINDOW_MS = lookbackWindowMs(CLICK_LOOKBACK_WINDOW_DAYS);
export const VIEW_LOOKBACK_WINDOW_MS = lookbackWindowMs(VIEW_LOOKBACK_WINDOW_DAYS);

export function hasClickId(clickIdValue: string | null | undefined): boolean {
  return Boolean(clickIdValue);
}

export function qualifiesSyntheticHintSignal(input: {
  source: string | null;
  medium: string | null;
  campaign: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
}): boolean {
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

export function isDirectTouchpoint(input: {
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdValue: string | null;
}): boolean {
  return !input.source && !input.medium && !input.campaign && !input.content && !input.term && !input.clickIdValue;
}

export function inferEngagementType(input: {
  engagementType?: AttributionEngagementType | null;
  clickIdValue?: string | null;
  defaultEngagementType?: AttributionEngagementType | null;
}): AttributionEngagementType {
  if (hasClickId(input.clickIdValue)) {
    return 'click';
  }

  if (input.engagementType === 'click' || input.engagementType === 'view' || input.engagementType === 'unknown') {
    return input.engagementType;
  }

  if (
    input.defaultEngagementType === 'click' ||
    input.defaultEngagementType === 'view' ||
    input.defaultEngagementType === 'unknown'
  ) {
    return input.defaultEngagementType;
  }

  return 'unknown';
}

export function isWithinLookbackWindow(
  orderOccurredAt: Date,
  touchpointOccurredAt: Date,
  engagementType: AttributionEngagementType,
  windows?: Partial<AttributionLookbackWindows>
): boolean {
  const normalizedWindows = normalizeAttributionLookbackWindows(windows);
  const deltaMs = orderOccurredAt.getTime() - touchpointOccurredAt.getTime();
  if (deltaMs < 0) {
    return false;
  }

  if (engagementType === 'click') {
    return deltaMs <= lookbackWindowMs(normalizedWindows.clickWindowDays);
  }

  if (engagementType === 'view') {
    return deltaMs <= lookbackWindowMs(normalizedWindows.viewWindowDays);
  }

  return false;
}
