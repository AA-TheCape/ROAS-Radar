function logShopifyWritebackObserved(fields: {
  workerId: string;
  shopifyOrderId: string;
  outcome: 'requeued' | 'up_to_date' | 'skipped' | 'failed';
  reason?: string;
  hasSessionId?: boolean;
}): void {
  logInfo('shopify_writeback_observed', {
    source: 'attribute_reconciliation',
    success: fields.outcome === 'requeued' || fields.outcome === 'up_to_date',
    ...fields
  });
}

if (!capturePayload) {
  skippedOrders += 1;
  logShopifyWritebackObserved({
    workerId: options.workerId,
    shopifyOrderId: candidate.shopify_order_id,
    outcome: 'skipped',
    reason: 'missing_capture_payload',
    hasSessionId: Boolean(candidate.roas_radar_session_id)
  });
  continue;
}

if (!orderAttributesNeedWriteback(customAttributes, expectedAttributes)) {
  upToDateOrders += 1;
  logShopifyWritebackObserved({
    workerId: options.workerId,
    shopifyOrderId: candidate.shopify_order_id,
    outcome: 'up_to_date',
    hasSessionId: Boolean(candidate.roas_radar_session_id)
  });
  continue;
}

logShopifyWritebackObserved({
  workerId: options.workerId,
  shopifyOrderId: candidate.shopify_order_id,
  outcome: 'requeued',
  hasSessionId: Boolean(candidate.roas_radar_session_id)
});
