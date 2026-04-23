const SHOPIFY_ATTRIBUTE_RECONCILIATION_REASON = 'shopify_attribute_reconciliation';

export type ShopifyOrderAttributeReconciliationOptions = {
  workerId: string;
  limit: number;
  lookbackDays?: number;
  now?: Date;
};

export type ShopifyOrderAttributeReconciliationResult = {
  workerId: string;
  lookbackDays: number;
  scannedOrders: number;
  ordersNeedingWriteback: number;
  requeuedOrders: number;
  upToDateOrders: number;
  skippedOrders: number;
  failedOrders: number;
  durationMs: number;
};

function buildOrderAttributeMap(attributes: ShopifyOrderAttribute[]): Map<string, string> {
  return new Map(
    attributes.flatMap((attribute) => {
      const value = normalizeNullableString(attribute.value);
      return value ? [[attribute.key, value] as const] : [];
    })
  );
}

function orderAttributesNeedWriteback(
  existingAttributes: ShopifyOrderAttribute[],
  expectedAttributes: ShopifyOrderAttribute[]
): boolean {
  const existingAttributeMap = buildOrderAttributeMap(existingAttributes);

  return expectedAttributes.some((attribute) => {
    if (!attribute.value) {
      return false;
    }

    return existingAttributeMap.get(attribute.key) !== attribute.value;
  });
}

async function fetchRecentWritebackContexts(
  client: PoolClient,
  windowStart: Date,
  limit: number
): Promise<ShopifyOrderWritebackContextRow[]> {
  const result = await client.query<ShopifyOrderWritebackContextRow>(
    `
      SELECT
        o.shopify_order_id,
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at,
        COALESCE(a.session_id, o.landing_session_id)::text AS roas_radar_session_id,
        s.first_seen_at,
        s.landing_page AS landing_url,
        s.referrer_url AS session_referrer_url,
        s.initial_utm_source,
        s.initial_utm_medium,
        s.initial_utm_campaign,
        s.initial_utm_content,
        s.initial_utm_term,
        s.initial_gclid,
        s.initial_gbraid,
        s.initial_wbraid,
        s.initial_fbclid,
        s.initial_ttclid,
        s.initial_msclkid,
        a.attributed_source,
        a.attributed_medium,
        a.attributed_campaign,
        a.attributed_content,
        a.attributed_term,
        a.attributed_click_id_type,
        a.attributed_click_id_value,
        a.confidence_score::text,
        a.attribution_reason,
        a.attributed_at
      FROM shopify_orders o
      LEFT JOIN attribution_results a
        ON a.shopify_order_id = o.shopify_order_id
      LEFT JOIN tracking_sessions s
        ON s.id = COALESCE(a.session_id, o.landing_session_id)
      WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1::timestamptz
      ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.id DESC
      LIMIT $2
    `,
    [windowStart, Math.max(limit, 0)]
  );

  return result.rows;
}

export async function reconcileRecentShopifyOrderAttributes(
  options: ShopifyOrderAttributeReconciliationOptions
): Promise<ShopifyOrderAttributeReconciliationResult> {
  const startedAt = Date.now();
  const lookbackDays = Math.max(Math.trunc(options.lookbackDays ?? 30), 1);
  const now = options.now ?? new Date();
  const windowStart = new Date(now.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
  const candidates = await withTransaction((client) => fetchRecentWritebackContexts(client, windowStart, options.limit));

  let ordersNeedingWriteback = 0;
  let requeuedOrders = 0;
  let upToDateOrders = 0;
  let skippedOrders = 0;
  let failedOrders = 0;

  if (candidates.length === 0) {
    const emptyReport = {
      workerId: options.workerId,
      lookbackDays,
      scannedOrders: 0,
      ordersNeedingWriteback: 0,
      requeuedOrders: 0,
      upToDateOrders: 0,
      skippedOrders: 0,
      failedOrders: 0,
      durationMs: Date.now() - startedAt
    };

    logInfo('shopify_attribute_reconciliation_completed', emptyReport);
    return emptyReport;
  }

  const shopDomain = await withTransaction(getActiveInstalledShopDomain);
  if (!shopDomain) {
    throw new Error('shopify_installation_not_found');
  }

  const accessToken = await withTransaction((client) => getActiveShopifyAccessToken(client, shopDomain));

  for (const candidate of candidates) {
    try {
      const capturePayload = await withTransaction((client) => buildCapturePayload(client, candidate));

      if (!capturePayload) {
        skippedOrders += 1;
        continue;
      }

      const expectedAttributes = buildCaptureAttributes(capturePayload);
      if (expectedAttributes.length === 0) {
        skippedOrders += 1;
        continue;
      }

      const { customAttributes } = await fetchOrderCustomAttributes(shopDomain, accessToken, candidate.shopify_order_id);
      if (!orderAttributesNeedWriteback(customAttributes, expectedAttributes)) {
        upToDateOrders += 1;
        continue;
      }

      ordersNeedingWriteback += 1;
      await enqueueShopifyOrderWriteback(candidate.shopify_order_id, SHOPIFY_ATTRIBUTE_RECONCILIATION_REASON);
      requeuedOrders += 1;
    } catch (error) {
      failedOrders += 1;
      logError('shopify_attribute_reconciliation_order_failed', error, {
        workerId: options.workerId,
        shopifyOrderId: candidate.shopify_order_id
      });
    }
  }

  const report = {
    workerId: options.workerId,
    lookbackDays,
    scannedOrders: candidates.length,
    ordersNeedingWriteback,
    requeuedOrders,
    upToDateOrders,
    skippedOrders,
    failedOrders,
    durationMs: Date.now() - startedAt
  };

  logInfo('shopify_attribute_reconciliation_completed', report);
  return report;
}
