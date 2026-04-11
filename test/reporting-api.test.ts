test('reporting reconciliation returns persisted data quality checks', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM data_quality_check_runs')) {
      return {
        rows: [
          {
            run_date: '2026-04-10',
            check_key: 'shopify_webhook_gaps',
            status: 'failed',
            severity: 'critical',
            discrepancy_count: 3,
            summary: '3 orders are missing webhook receipts.',
            details: {
              sampleMissingOrderIds: ['1001', '1002', '1003']
            },
            checked_at: new Date('2026-04-11T00:15:00.000Z'),
            alert_emitted_at: new Date('2026-04-11T00:15:00.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected SQL in reconciliation test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const address = server.address() as AddressInfo;
    const response = await fetch(
      `http://127.0.0.1:${address.port}/api/reporting/reconciliation?runDate=2026-04-10`,
      {
        headers: buildHeaders()
      }
    );
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.version, '2026-04-11');
    assert.equal(body.tenantId, 'roas-radar');
    assert.equal(body.data.runDate, '2026-04-10');
    assert.equal(body.data.totals.failedChecks, 1);
    assert.equal(body.data.totals.totalDiscrepancies, 3);
    assert.equal(body.data.checks[0].checkKey, 'shopify_webhook_gaps');
    assert.deepEqual(body.data.checks[0].details.sampleMissingOrderIds, ['1001', '1002', '1003']);
  } finally {
    await closeServer(server);
  }
});
