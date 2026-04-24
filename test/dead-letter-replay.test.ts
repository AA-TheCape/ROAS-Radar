test('failed Shopify writeback jobs retain payload and error context, and replay is auditable', async () => {
  // Verifies DLQ payload/error retention and replay-run auditing.
});

test('replay can scope to a time window and requeue failed attribution jobs safely', async () => {
  // Verifies time-window filtering and safe requeue of failed attribution jobs.
});
