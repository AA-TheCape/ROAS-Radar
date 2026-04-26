import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
  normalizeAttributionCaptureV1,
  normalizeAttributionConsentState,
  normalizeOrderAttributionBackfillRequest,
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillJobResponseSchema
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

test('attribution capture normalization converts empty strings to null and removes URL fragments', () => {
  const capture = normalizeAttributionCaptureV1({
    schema_version: 1,
    roas_radar_session_id: '123e4567-e89b-42d3-a456-426614174000',
    occurred_at: '2026-04-23T12:00:00Z',
    captured_at: '2026-04-23T12:00:05Z',
    landing_url: ' https://example.com/landing?utm_source=Email#hero ',
    referrer_url: '   ',
    page_url: 'https://example.com/products/widget?gbraid=GB-123#wrapper',
    utm_source: ' Email ',
    utm_medium: ' Newsletter ',
    utm_campaign: '',
    utm_content: '   ',
    utm_term: undefined,
    gclid: '',
    gbraid: ' GB-123 ',
    wbraid: '   ',
    fbclid: null,
    ttclid: undefined,
    msclkid: ''
  });

  assert.equal(capture.landing_url, 'https://example.com/landing?utm_source=Email');
  assert.equal(capture.referrer_url, null);
  assert.equal(capture.page_url, 'https://example.com/products/widget?gbraid=GB-123');
  assert.equal(capture.utm_source, 'email');
  assert.equal(capture.utm_medium, 'newsletter');
  assert.equal(capture.utm_campaign, null);
  assert.equal(capture.utm_content, null);
  assert.equal(capture.gclid, null);
  assert.equal(capture.gbraid, 'GB-123');
  assert.equal(capture.wbraid, null);
  assert.equal(capture.msclkid, null);
});

test('order attribution backfill request normalizes defaults', () => {
  const request = normalizeOrderAttributionBackfillRequest({
    startDate: '2026-04-01',
    endDate: '2026-04-15'
  });

  assert.deepEqual(request, {
    startDate: '2026-04-01',
    endDate: '2026-04-15',
    dryRun: true,
    limit: ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
    webOrdersOnly: true,
    skipShopifyWriteback: false
  });
});

test('order attribution backfill request preserves explicit execution flags at the limit cap', () => {
  const request = normalizeOrderAttributionBackfillRequest({
    startDate: '2026-04-01',
    endDate: '2026-04-15',
    dryRun: false,
    limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
    webOrdersOnly: false,
    skipShopifyWriteback: true
  });

  assert.deepEqual(request, {
    startDate: '2026-04-01',
    endDate: '2026-04-15',
    dryRun: false,
    limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
    webOrdersOnly: false,
    skipShopifyWriteback: true
  });
});

test('order attribution backfill request rejects invalid date windows and oversized limits', () => {
  assert.throws(
    () =>
      normalizeOrderAttributionBackfillRequest({
        startDate: '2026-04-15',
        endDate: '2026-04-01'
      }),
    /Start date must be on or before end date\./
  );

  assert.throws(
    () =>
      normalizeOrderAttributionBackfillRequest({
        startDate: '2026-04-01',
        endDate: '2026-04-15',
        limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT + 1
      }),
    /Limit must be 5000 or less\./
  );
});

test('order attribution backfill request rejects non-positive limits', () => {
  assert.throws(
    () =>
      normalizeOrderAttributionBackfillRequest({
        startDate: '2026-04-01',
        endDate: '2026-04-15',
        limit: 0
      }),
    /Limit must be greater than 0\./
  );
});

test('order attribution backfill responses accept normalized enqueue and job payloads', () => {
  const enqueueResponse = orderAttributionBackfillEnqueueResponseSchema.parse({
    ok: true,
    jobId: '0ed2f8d7-3867-4bad-a91b-487080ec2a47',
    status: 'queued',
    submittedAt: '2026-04-25T12:34:56Z',
    submittedBy: 'admin@example.com',
    options: {
      startDate: '2026-04-01',
      endDate: '2026-04-15',
      dryRun: true,
      limit: 250,
      webOrdersOnly: true,
      skipShopifyWriteback: false
    }
  });

  const jobResponse = orderAttributionBackfillJobResponseSchema.parse({
    ...enqueueResponse,
    status: 'completed',
    startedAt: '2026-04-25T12:35:00Z',
    completedAt: '2026-04-25T12:36:00Z',
    report: {
      scanned: 250,
      recovered: 120,
      unrecoverable: 130,
      writebackCompleted: 120,
      failures: [
        {
          orderId: '12345',
          code: 'shopify_writeback_failed',
          message: 'Writeback failed'
        }
      ]
    },
    error: null
  });

  assert.equal(enqueueResponse.submittedAt, '2026-04-25T12:34:56.000Z');
  assert.equal(jobResponse.startedAt, '2026-04-25T12:35:00.000Z');
  assert.equal(jobResponse.report?.failures[0]?.code, 'shopify_writeback_failed');
});

test('order attribution backfill job responses accept queued and processing payloads without reports', () => {
  const queuedJob = orderAttributionBackfillJobResponseSchema.parse({
    ok: true,
    jobId: '0ed2f8d7-3867-4bad-a91b-487080ec2a47',
    status: 'queued',
    submittedAt: '2026-04-25T12:34:56Z',
    submittedBy: 'admin@example.com',
    startedAt: null,
    completedAt: null,
    options: {
      startDate: '2026-04-01',
      endDate: '2026-04-15',
      dryRun: true,
      limit: 500,
      webOrdersOnly: true,
      skipShopifyWriteback: false
    },
    report: null,
    error: null
  });

  const processingJob = orderAttributionBackfillJobResponseSchema.parse({
    ...queuedJob,
    status: 'processing',
    startedAt: '2026-04-25T12:35:00Z'
  });

  assert.equal(queuedJob.startedAt, null);
  assert.equal(queuedJob.report, null);
  assert.equal(processingJob.startedAt, '2026-04-25T12:35:00.000Z');
  assert.equal(processingJob.completedAt, null);
  assert.equal(processingJob.error, null);
});
