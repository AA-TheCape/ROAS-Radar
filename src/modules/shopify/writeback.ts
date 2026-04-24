export async function enqueueShopifyOrderWriteback(shopifyOrderId, requestedReason, client) {
  // Idempotent queue upsert.
}

export async function processShopifyOrderWritebackQueue(options) {
  // Claims jobs, builds canonical expected attributes, runs writeback processor,
  // retries transient failures, and dead-letters terminal failures.
}

async function markJobForRetry(client, job, workerId, error) {
  const shouldDeadLetter = job.attempts >= env.SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES;
  if (shouldDeadLetter) {
    await recordDeadLetter(client, {
      eventType: 'shopify_writeback_failed',
      sourceTable: 'shopify_order_writeback_jobs',
      sourceRecordId: String(job.id),
      sourceQueueKey: job.queue_key,
      payload: { jobId: String(job.id), shopifyOrderId: job.shopify_order_id, requestedReason: job.requested_reason, attempts: job.attempts, workerId },
      error
    });
    return;
  }
}
