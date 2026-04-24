import { useEffect } from 'react';
import {
  isAttributionSessionId,
  normalizeAttributionUrl,
  type AttributionConsentState
} from '../../../packages/attribution-schema/index.js';

type RuntimeConfig = {
  apiBaseUrl?: string;
  reportingToken?: string;
  reportingTenantId?: string;
};

type TrackerConfig = {
  retryBaseDelayMs?: number;
  maxRetryDelayMs?: number;
  queueStorageKey?: string;
  consentState?: AttributionConsentState;
};

type SessionInfo = {
  sessionId: string;
  createdAt: string | null;
};

type TrackingEventPayload = {
  eventType: 'page_view';
  occurredAt: string;
  sessionId: string;
  pageUrl: string;
  referrerUrl: string | null;
  shopifyCartToken: null;
  shopifyCheckoutToken: null;
  clientEventId: string;
  consentState: AttributionConsentState;
  context: {
    userAgent: string | null;
    screen: string | null;
    language: string | null;
  };
};

type PendingEvent = {
  payload: TrackingEventPayload;
  attempt: number;
  nextAttemptAt: number;
};

declare global {
  interface Window {
    __ROAS_RADAR_RUNTIME_CONFIG__?: RuntimeConfig;
    __ROAS_RADAR_ATTRIBUTION_CONFIG__?: TrackerConfig;
    __ROAS_RADAR_ATTRIBUTION_TRACKER_STARTED__?: boolean;
  }
}

const viteEnv = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env ?? {};
const SESSION_ID_STORAGE_KEY = 'roas_radar_session_id';
const SESSION_CREATED_AT_STORAGE_KEY = 'roas_radar_session_created_at';
const LANDING_URL_STORAGE_KEY = 'roas_radar_landing_url';
const DEFAULT_QUEUE_STORAGE_KEY = 'roas_radar_pending_track_events';
const DEFAULT_RETRY_BASE_DELAY_MS = 1_000;
const DEFAULT_MAX_RETRY_DELAY_MS = 30_000;
const TRANSIENT_STATUS_CODES = new Set([408, 425, 429, 500, 502, 503, 504]);

let sessionPromise: Promise<SessionInfo> | null = null;
let flushPromise: Promise<void> | null = null;
let retryTimer: number | null = null;
let lastTrackedPageUrl: string | null = null;
let lastObservedPageUrl: string | null = null;
let historyTrackingInstalled = false;
let originalPushState: History['pushState'] | null = null;
let originalReplaceState: History['replaceState'] | null = null;

function readTrackerConfig(): Required<TrackerConfig> {
  const windowConfig = window.__ROAS_RADAR_ATTRIBUTION_CONFIG__ ?? {};

  return {
    retryBaseDelayMs: windowConfig.retryBaseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS,
    maxRetryDelayMs: windowConfig.maxRetryDelayMs ?? DEFAULT_MAX_RETRY_DELAY_MS,
    queueStorageKey: windowConfig.queueStorageKey ?? DEFAULT_QUEUE_STORAGE_KEY,
    consentState: windowConfig.consentState ?? 'unknown'
  };
}

function buildApiUrl(path: string): string {
  const runtimeConfig = window.__ROAS_RADAR_RUNTIME_CONFIG__;
  const baseUrl = (runtimeConfig?.apiBaseUrl ?? viteEnv.VITE_API_BASE_URL ?? '').replace(/\/$/, '');
  return `${baseUrl}${path}`;
}

function safeGetStorage(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function safeSetStorage(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Ignore storage failures and rely on the in-memory path for this page.
  }
}

function normalizeUrl(rawValue: string | null | undefined): string | null {
  try {
    return normalizeAttributionUrl(rawValue, window.location.origin);
  } catch {
    return null;
  }
}

function generateUuid(): string {
  if (window.crypto && typeof window.crypto.randomUUID === 'function') {
    return window.crypto.randomUUID();
  }

  const bytes = new Uint8Array(16);

  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Math.floor(Math.random() * 256);
  }

  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0'));

  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex.slice(8, 10).join('')}-${hex
    .slice(10)
    .join('')}`;
}

function readSessionInfo(): SessionInfo | null {
  const sessionId = safeGetStorage(SESSION_ID_STORAGE_KEY);

  if (!isAttributionSessionId(sessionId)) {
    return null;
  }

  const createdAt = safeGetStorage(SESSION_CREATED_AT_STORAGE_KEY);

  return {
    sessionId,
    createdAt: createdAt?.trim() ? createdAt : null
  };
}

function persistSessionInfo(session: SessionInfo): SessionInfo {
  safeSetStorage(SESSION_ID_STORAGE_KEY, session.sessionId);

  if (session.createdAt) {
    safeSetStorage(SESSION_CREATED_AT_STORAGE_KEY, session.createdAt);
  }

  return session;
}

function resolveLandingUrl(pageUrl: string): { landingUrl: string; isFirstEntry: boolean } {
  const existingLandingUrl = normalizeUrl(safeGetStorage(LANDING_URL_STORAGE_KEY));

  if (existingLandingUrl) {
    return {
      landingUrl: existingLandingUrl,
      isFirstEntry: false
    };
  }

  safeSetStorage(LANDING_URL_STORAGE_KEY, pageUrl);

  return {
    landingUrl: pageUrl,
    isFirstEntry: true
  };
}

function getQueue(): PendingEvent[] {
  const { queueStorageKey } = readTrackerConfig();
  const rawValue = safeGetStorage(queueStorageKey);

  if (!rawValue) {
    return [];
  }

  try {
    const parsed = JSON.parse(rawValue) as PendingEvent[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function setQueue(queue: PendingEvent[]): void {
  const { queueStorageKey } = readTrackerConfig();
  safeSetStorage(queueStorageKey, JSON.stringify(queue.slice(-20)));
}

function scheduleFlush(delayMs: number): void {
  if (retryTimer !== null) {
    window.clearTimeout(retryTimer);
  }

  retryTimer = window.setTimeout(() => {
    retryTimer = null;
    void flushQueue();
  }, Math.max(delayMs, 0));
}

function scheduleNextQueuedFlush(queue = getQueue()): void {
  if (queue.length === 0) {
    if (retryTimer !== null) {
      window.clearTimeout(retryTimer);
      retryTimer = null;
    }

    return;
  }

  const nextAttemptAt = Math.min(...queue.map((event) => event.nextAttemptAt));
  scheduleFlush(nextAttemptAt - Date.now());
}

function buildRetryDelay(attempt: number): number {
  const { retryBaseDelayMs, maxRetryDelayMs } = readTrackerConfig();
  return Math.min(retryBaseDelayMs * 2 ** attempt, maxRetryDelayMs);
}

function enqueueRetry(payload: TrackingEventPayload, attempt: number): void {
  const queue = getQueue().filter((entry) => entry.payload.clientEventId !== payload.clientEventId);

  queue.push({
    payload,
    attempt,
    nextAttemptAt: Date.now() + buildRetryDelay(attempt)
  });

  setQueue(queue);
  scheduleNextQueuedFlush(queue);
}

async function requestSession(
  pageUrl: string,
  landingUrl: string | null,
  referrerUrl: string | null
): Promise<SessionInfo> {
  const params = new URLSearchParams({
    pageUrl
  });

  if (landingUrl) {
    params.set('landingUrl', landingUrl);
  }

  if (referrerUrl) {
    params.set('referrerUrl', referrerUrl);
  }

  const response = await fetch(`${buildApiUrl('/track/session')}?${params.toString()}`, {
    method: 'GET',
    credentials: 'include',
    headers: {
      accept: 'application/json'
    }
  });

  if (!response.ok) {
    throw new Error(`Session bootstrap failed with status ${response.status}`);
  }

  const body = (await response.json()) as {
    sessionId?: string;
    createdAt?: string | null;
  };

  if (!isAttributionSessionId(body.sessionId)) {
    throw new Error('Session bootstrap returned an invalid session id');
  }

  return persistSessionInfo({
    sessionId: body.sessionId,
    createdAt: typeof body.createdAt === 'string' ? body.createdAt : null
  });
}

function fallbackSession(): SessionInfo {
  return persistSessionInfo(
    readSessionInfo() ?? {
      sessionId: generateUuid(),
      createdAt: new Date().toISOString()
    }
  );
}

function ensureSession(pageUrl: string, landingUrl: string | null, referrerUrl: string | null): Promise<SessionInfo> {
  if (sessionPromise) {
    return sessionPromise;
  }

  const pendingPromise = requestSession(pageUrl, landingUrl, referrerUrl).catch(() => fallbackSession());
  sessionPromise = pendingPromise;

  return pendingPromise.finally(() => {
    if (sessionPromise === pendingPromise) {
      sessionPromise = null;
    }
  });
}

function buildTrackingPayload(sessionId: string, pageUrl: string, referrerUrl: string | null): TrackingEventPayload {
  return {
    eventType: 'page_view',
    occurredAt: new Date().toISOString(),
    sessionId,
    pageUrl,
    referrerUrl,
    shopifyCartToken: null,
    shopifyCheckoutToken: null,
    clientEventId: generateUuid(),
    consentState: readTrackerConfig().consentState ?? 'unknown',
    context: {
      userAgent: window.navigator.userAgent ?? null,
      screen:
        typeof window.screen?.width === 'number' && typeof window.screen?.height === 'number'
          ? `${window.screen.width}x${window.screen.height}`
          : null,
      language: window.navigator.language ?? null
    }
  };
}

async function deliverPayload(payload: TrackingEventPayload): Promise<{ delivered: boolean; retryable: boolean }> {
  try {
    const response = await fetch(buildApiUrl('/track'), {
      method: 'POST',
      credentials: 'include',
      keepalive: true,
      headers: {
        'content-type': 'application/json',
        accept: 'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (response.ok) {
      const body = (await response.json()) as { sessionId?: string };

      if (isAttributionSessionId(body.sessionId)) {
        persistSessionInfo({
          sessionId: body.sessionId,
          createdAt: readSessionInfo()?.createdAt ?? null
        });
      }

      return {
        delivered: true,
        retryable: false
      };
    }

    return {
      delivered: false,
      retryable: TRANSIENT_STATUS_CODES.has(response.status)
    };
  } catch {
    return {
      delivered: false,
      retryable: true
    };
  }
}

async function flushQueue(): Promise<void> {
  if (flushPromise) {
    return flushPromise;
  }

  const pendingFlush = (async () => {
    const now = Date.now();
    const queue = getQueue();
    const remaining: PendingEvent[] = [];

    for (const entry of queue) {
      if (entry.nextAttemptAt > now) {
        remaining.push(entry);
        continue;
      }

      const result = await deliverPayload(entry.payload);

      if (!result.delivered && result.retryable) {
        remaining.push({
          payload: entry.payload,
          attempt: entry.attempt + 1,
          nextAttemptAt: Date.now() + buildRetryDelay(entry.attempt + 1)
        });
      }
    }

    setQueue(remaining);
    scheduleNextQueuedFlush(remaining);
  })();

  flushPromise = pendingFlush;

  return pendingFlush.finally(() => {
    if (flushPromise === pendingFlush) {
      flushPromise = null;
    }
  });
}

async function trackPageView(pageUrl: string, referrerUrl: string | null): Promise<void> {
  if (pageUrl === lastTrackedPageUrl) {
    return;
  }

  lastTrackedPageUrl = pageUrl;
  lastObservedPageUrl = pageUrl;

  const { landingUrl, isFirstEntry } = resolveLandingUrl(pageUrl);
  const session = await ensureSession(pageUrl, isFirstEntry ? landingUrl : null, referrerUrl);
  const payload = buildTrackingPayload(session.sessionId, pageUrl, referrerUrl);

  await flushQueue();

  const result = await deliverPayload(payload);

  if (!result.delivered && result.retryable) {
    enqueueRetry(payload, 0);
  }
}

function handleLocationChange(): void {
  const pageUrl = normalizeUrl(window.location.href);

  if (!pageUrl || pageUrl === lastObservedPageUrl) {
    return;
  }

  const referrerUrl = lastObservedPageUrl ?? normalizeUrl(document.referrer);
  void trackPageView(pageUrl, referrerUrl);
}

function installHistoryTracking(): void {
  if (historyTrackingInstalled) {
    return;
  }

  historyTrackingInstalled = true;
  originalPushState = window.history.pushState.bind(window.history);
  originalReplaceState = window.history.replaceState.bind(window.history);

  window.history.pushState = ((...args: Parameters<History['pushState']>) => {
    originalPushState?.(...args);
    handleLocationChange();
  }) as History['pushState'];

  window.history.replaceState = ((...args: Parameters<History['replaceState']>) => {
    originalReplaceState?.(...args);
    handleLocationChange();
  }) as History['replaceState'];

  window.addEventListener('popstate', handleLocationChange);

  window.addEventListener('online', () => {
    void flushQueue();
  });
}

export function startAttributionTracking(): void {
  if (window.__ROAS_RADAR_ATTRIBUTION_TRACKER_STARTED__) {
    return;
  }

  window.__ROAS_RADAR_ATTRIBUTION_TRACKER_STARTED__ = true;
  installHistoryTracking();

  const pageUrl = normalizeUrl(window.location.href);

  if (!pageUrl) {
    return;
  }

  const referrerUrl = normalizeUrl(document.referrer);
  void trackPageView(pageUrl, referrerUrl);
}

export function AttributionTracker() {
  useEffect(() => {
    const timer = window.setTimeout(() => {
      startAttributionTracking();
    }, 0);

    return () => {
      window.clearTimeout(timer);
    };
  }, []);

  return null;
}

export const __attributionTrackingTestUtils = {
  reset() {
    window.__ROAS_RADAR_ATTRIBUTION_TRACKER_STARTED__ = false;
    sessionPromise = null;
    flushPromise = null;
    lastTrackedPageUrl = null;
    lastObservedPageUrl = null;
    setQueue([]);

    try {
      window.localStorage.removeItem(SESSION_ID_STORAGE_KEY);
      window.localStorage.removeItem(SESSION_CREATED_AT_STORAGE_KEY);
      window.localStorage.removeItem(LANDING_URL_STORAGE_KEY);
    } catch {
      // Ignore storage cleanup failures in tests.
    }

    if (retryTimer !== null) {
      window.clearTimeout(retryTimer);
      retryTimer = null;
    }

    if (historyTrackingInstalled) {
      if (originalPushState) {
        window.history.pushState = originalPushState;
      }

      if (originalReplaceState) {
        window.history.replaceState = originalReplaceState;
      }

      window.removeEventListener('popstate', handleLocationChange);
      historyTrackingInstalled = false;
      originalPushState = null;
      originalReplaceState = null;
    }
  }
};
