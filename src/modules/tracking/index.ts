import {
  logError,
  logInfo,
  logWarning,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency
} from '../../observability/index.js';

function logAttributionCaptureObserved(
  source: 'session_bootstrap' | 'browser_event' | 'attribution_capture',
  payload: unknown,
  fields: Record<string, unknown>
): void {
  logInfo('attribution_capture_observed', {
    source,
    ...summarizeAttributionObservation(payload),
    ...fields
  });
}

// /session success
logAttributionCaptureObserved('session_bootstrap', {
  roas_radar_session_id: result.sessionId,
  landing_url: bootstrapInput.landingUrl ?? bootstrapInput.pageUrl,
  referrer_url: bootstrapInput.referrerUrl,
  page_url: bootstrapInput.pageUrl
}, {
  accepted: true,
  deduplicated: !result.isNewSession,
  requestContextCaptured: result.requestContextCaptured,
  requestContextSource: result.requestContextSource
});

// /attribution success + rejection
logAttributionCaptureObserved('attribution_capture', parsed.capture, {
  accepted: true,
  deduplicated: result.deduplicated,
  touchEventId: result.touchEventId
});

logAttributionCaptureObserved('attribution_capture', req.body as TrackingRequestBody, {
  accepted: false,
  rejectionCode: error.code,
  statusCode: error.statusCode
});

// /track success + rejection
logAttributionCaptureObserved('browser_event', browserResult.sanitizedInput, {
  accepted: true,
  deduplicated: browserResult.deduplicated,
  eventType: input.eventType
});

logInfo('tracking_dual_write_consistency', {
  ...summarizeDualWriteConsistency({
    browserOutcome: browserResult.deduplicated ? 'deduplicated' : 'accepted',
    serverOutcome: serverAttributionResult.ok
      ? serverAttributionResult.deduplicated ? 'deduplicated' : 'accepted'
      : 'failed'
  }),
  sessionId: browserResult.sessionId,
  eventId: browserResult.eventId,
  eventType: input.eventType,
  touchEventId: serverAttributionResult.touchEventId,
  errorCode: serverAttributionResult.errorCode
});
