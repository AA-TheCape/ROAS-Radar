import assert from 'node:assert/strict';
import test from 'node:test';

import {
  normalizeAttributionCaptureV1,
  normalizeAttributionConsentState
} from '../packages/attribution-schema/index.js';

test('attribution consent state defaults to unknown and accepts explicit opt-out', () => {
  assert.equal(normalizeAttributionConsentState(undefined), 'unknown');
  assert.equal(normalizeAttributionConsentState('denied'), 'denied');
  assert.throws(() => normalizeAttributionConsentState('revoked'));
});

test('attribution capture normalization keeps marketing identifiers under denied consent', () => {
  const capture = normalizeAttributionCaptureV1({
    schema_version: 1,
    roas_radar_session_id: '123e4567-e89b-42d3-a456-426614174000',
    occurred_at: '2026-04-23T12:00:00.000Z',
    captured_at: '2026-04-23T12:00:05.000Z',
    landing_url: 'https://example.com/?utm_source=Google&utm_medium=CPC&gclid=ABC123',
    referrer_url: 'https://google.com/search?q=widget',
    page_url: 'https://example.com/products/widget?gclid=ABC123',
    utm_source: 'Google',
    utm_medium: 'CPC',
    utm_campaign: 'Spring',
    utm_content: 'Hero',
    utm_term: 'Widget',
    gclid: 'ABC123',
    gbraid: 'GB-123',
    wbraid: 'WB-456',
    fbclid: null,
    ttclid: null,
    msclkid: null
  });

  assert.equal(capture.utm_source, 'google');
  assert.equal(capture.utm_medium, 'cpc');
  assert.equal(capture.gclid, 'ABC123');
  assert.equal(capture.gbraid, 'GB-123');
  assert.equal(capture.wbraid, 'WB-456');
});
