test('summarizeResolverOutcome reports unattributed and non-direct winners deterministically', () => {
  const unattributed = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [],
    winner: null
  });

  assert.equal(unattributed.resolverOutcome, 'unattributed');
  assert.equal(unattributed.winnerMatchSource, 'unattributed');
  assert.equal(unattributed.ga4SkippedDueToPrecedence, false);

  const ga4FallbackWinner = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [{}],
    winner: {
      isDirect: false,
      ingestionSource: null,
      sessionId: null,
      matchSource: 'ga4_fallback',
      source: 'google',
      medium: 'cpc',
      campaign: 'brand',
      clickIdValue: 'GCLID-123'
    }
  });

  assert.equal(ga4FallbackWinner.winnerMatchSource, 'ga4_fallback');
  assert.equal(ga4FallbackWinner.fallbackUsed, true);
  assert.equal(ga4FallbackWinner.hasClickId, true);
  assert.equal(ga4FallbackWinner.ga4SkippedDueToPrecedence, false);
});

test('summarizeGa4IngestionResult reports lag and fill rates for hourly ingestion health', () => {
  const summary = __observabilityTestUtils.summarizeGa4IngestionResult({
    watermarkBefore: '2026-04-27T08:00:00.000Z',
    watermarkAfter: '2026-04-27T09:00:00.000Z',
    processedHours: ['2026-04-27T09:00:00.000Z'],
    extractedRows: 2,
    upsertedRows: 2,
    now: new Date('2026-04-27T12:35:00.000Z'),
    lagAlertThresholdHours: 2,
    rows: [
      { source: 'google', medium: 'cpc', campaign: 'spring', clickIdValue: 'GCLID-123' },
      { source: null, medium: 'email', campaign: null, clickIdValue: null }
    ]
  });

  assert.equal(summary.lagHours, 2);
  assert.equal(summary.lagStatus, 'lagging');
  assert.equal(summary.sourcePresentRows, 1);
  assert.equal(summary.mediumPresentRows, 2);
  assert.equal(summary.campaignPresentRows, 1);
  assert.equal(summary.clickIdPresentRows, 1);
  assert.equal(summary.sourceFillRate, 0.5);
  assert.equal(summary.mediumFillRate, 1);
  assert.equal(summary.campaignFillRate, 0.5);
  assert.equal(summary.clickIdFillRate, 0.5);
});

test('emitOrderAttributionBackfillJobLifecycleLog emits structured lifecycle logs with job ids and failure metadata', () => {
  // ...existing setup...
  assert.equal(entries[0].correlationId, entries[0].jobId);
  // ...
  assert.equal(entries[1].correlationId, entries[1].jobId);
});
