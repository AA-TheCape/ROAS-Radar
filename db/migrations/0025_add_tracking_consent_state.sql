BEGIN;

ALTER TABLE tracking_events
  ADD COLUMN consent_state text NOT NULL DEFAULT 'unknown',
  ADD CONSTRAINT tracking_events_consent_state_chk
    CHECK (consent_state IN ('granted', 'denied', 'unknown'));

ALTER TABLE session_attribution_touch_events
  ADD COLUMN consent_state text NOT NULL DEFAULT 'unknown',
  ADD CONSTRAINT session_attribution_touch_events_consent_state_chk
    CHECK (consent_state IN ('granted', 'denied', 'unknown'));

UPDATE tracking_events
SET consent_state = CASE
  WHEN raw_payload ->> 'consentState' IN ('granted', 'denied', 'unknown') THEN raw_payload ->> 'consentState'
  WHEN raw_payload ->> 'consent_state' IN ('granted', 'denied', 'unknown') THEN raw_payload ->> 'consent_state'
  ELSE 'unknown'
END;

UPDATE session_attribution_touch_events
SET consent_state = CASE
  WHEN raw_payload ->> 'consentState' IN ('granted', 'denied', 'unknown') THEN raw_payload ->> 'consentState'
  WHEN raw_payload ->> 'consent_state' IN ('granted', 'denied', 'unknown') THEN raw_payload ->> 'consent_state'
  ELSE 'unknown'
END;

COMMIT;
