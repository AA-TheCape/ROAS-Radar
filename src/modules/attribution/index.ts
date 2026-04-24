// Added env + dead-letter integration.

async function markJobForRetry(client, job, workerId, error) {
  const shouldDeadLetter = job.attempts >= env.ATTRIBUTION_MAX_RETRIES;

  if (shouldDeadLetter) {
    await client.query(`
      UPDATE attribution_jobs
      SET status = 'failed',
          dead_lettered_at = now(),
          ...
    `);

    await recordDeadLetter(client, {
      eventType: 'attribution_job_failed',
      sourceTable: 'attribution_jobs',
      sourceRecordId: String(job.id),
      sourceQueueKey: buildQueueKey(job.shopify_order_id),
      payload: { jobId: String(job.id), shopifyOrderId: job.shopify_order_id, attempts: job.attempts, workerId },
      error
    });
    return;
  }

  // Existing retry path remains for non-terminal failures.
}
