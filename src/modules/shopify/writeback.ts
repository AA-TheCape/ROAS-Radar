import type { PoolClient } from 'pg';

import { ATTRIBUTION_SCHEMA_VERSION, normalizeAttributionCaptureV1 } from '../../../packages/attribution-schema/index.js';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';

const JOB_STALE_AFTER_MINUTES = 15;
const MAX_RETRY_DELAY_SECONDS = 1_800;
const SHOPIFY_WRITEBACK_NAMESPACE = 'roas_radar';
const SHOPIFY_CAPTURE_METAFIELD_KEY = 'attribution_capture_v1';
const SHOPIFY_RESULT_METAFIELD_KEY = 'attribution_result_v1';
const CANONICAL_ATTRIBUTE_KEYS = [
  'schema_version',
  'roas_radar_session_id',
  'landing_url',
  'referrer_url',
  'page_url',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
  'gclid',
  'gbraid',
  'wbraid',
  'fbclid',
  'ttclid',
  'msclkid'
] as const;

type ShopifyOrderWritebackJobRow = {
  id: number;
  shopify_order_id: string;
  attempts: number;
};

type ShopifyOrderWritebackContextRow = {
  shopify_order_id: string;
  order_occurred_at: Date;
  roas_radar_session_id: string | null;
  first_seen_at: Date | null;
  landing_url: string | null;
  session_referrer_url: string | null;
  initial_utm_source: string | null;
  initial_utm_medium: string | null;
  initial_utm_campaign: string | null;
  initial_utm_content: string | null;
  initial_utm_term: string | null;
  initial_gclid: string | null;
  initial_gbraid: string | null;
  initial_wbraid: string | null;
  initial_fbclid: string | null;
  initial_ttclid: string | null;
  initial_msclkid: string | null;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  attributed_click_id_type: string | null;
  attributed_click_id_value: string | null;
  confidence_score: string | null;
  attribution_reason: string | null;
  attributed_at: Date | null;
};

type TrackingEventRow = {
  page_url: string | null;
  referrer_url: string | null;
};

type ShopifyGraphqlResponse<TData> = {
  data?: TData;
  errors?: Array<{ message: string }>;
};

type ShopifyOrderAttribute = {
  key: string;
  value: string | null;
};

type ShopifyOrderQueryResponse = {
  order: {
    id: string;
    customAttributes: ShopifyOrderAttribute[];
  } | null;
};

type ShopifyOrderUpdateResponse = {
  orderUpdate: {
    userErrors: Array<{ field: string[] | null; message: string }>;
  };
};

type ShopifyMetafieldsSetResponse = {
  metafieldsSet: {
    userErrors: Array<{ field: string[] | null; message: string; code: string | null }>;
  };
};

export type ShopifyOrderWritebackQueueProcessOptions = {
  workerId: string;
  limit: number;
  staleScanLimit?: number;
};

export type ShopifyOrderWritebackQueueProcessResult = {
  workerId: string;
  claimedJobs: number;
  staleJobsEnqueued: number;
  succeededJobs: number;
  failedJobs: number;
  deadLetteredJobs: number;
  durationMs: number;
};

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function execute<T>(client: PoolClient | undefined, callback: (db: PoolClient) => Promise<T>): Promise<T> {
  if (client) {
    return callback(client);
  }

  return withTransaction(callback);
}

export function buildShopifyOrderWritebackQueueKey(shopifyOrderId: string): string {
  return `shopify-order-writeback:${shopifyOrderId}`;
}

export function computeShopifyOrderWritebackRetryDelaySeconds(attempts: number): number {
  const normalizedAttempts = Number.isFinite(attempts) ? Math.max(Math.trunc(attempts), 1) : 1;
  return Math.min(30 * 2 ** (normalizedAttempts - 1), MAX_RETRY_DELAY_SECONDS);
}

function buildShopifyOrderGid(shopifyOrderId: string): string {
  return `gid://shopify/Order/${shopifyOrderId}`;
}

function buildResolvedAttributionPayload(context: ShopifyOrderWritebackContextRow): string | null {
  if (!context.attribution_reason) {
    return null;
  }

  return JSON.stringify({
    schema_version: ATTRIBUTION_SCHEMA_VERSION,
    shopify_order_id: context.shopify_order_id,
    roas_radar_session_id: context.roas_radar_session_id,
    attribution_model: 'last_touch',
    attributed_source: context.attributed_source,
    attributed_medium: context.attributed_medium,
    attributed_campaign: context.attributed_campaign,
    attributed_content: context.attributed_content,
    attributed_term: context.attributed_term,
    attributed_click_id_type: context.attributed_click_id_type,
    attributed_click_id_value: context.attributed_click_id_value,
    confidence_score: context.confidence_score === null ? null : Number(context.confidence_score),
    attribution_reason: context.attribution_reason,
    attributed_at: context.attributed_at?.toISOString() ?? null
  });
}

function buildCaptureAttributes(capturePayload: ReturnType<typeof normalizeAttributionCaptureV1>): ShopifyOrderAttribute[] {
  return CANONICAL_ATTRIBUTE_KEYS.map((key) => {
    const value = capturePayload[key];
    return {
      key,
      value: value === null ? null : String(value)
    };
  }).filter((attribute) => attribute.value !== null);
}

function mergeOrderAttributes(
  existingAttributes: ShopifyOrderAttribute[],
  newAttributes: ShopifyOrderAttribute[]
): Array<{ key: string; value: string }> {
  const replacementKeys = new Set<string>(CANONICAL_ATTRIBUTE_KEYS);
  const merged = existingAttributes
    .filter((attribute) => !replacementKeys.has(attribute.key))
    .filter((attribute): attribute is { key: string; value: string } => Boolean(attribute.value))
    .map((attribute) => ({
      key: attribute.key,
      value: attribute.value
    }));

  for (const attribute of newAttributes) {
    if (!attribute.value) {
      continue;
    }

    merged.push({
      key: attribute.key,
      value: attribute.value
    });
  }

  return merged;
}

async function getActiveInstalledShopDomain(client: PoolClient): Promise<string | null> {
  const result = await client.query<{ shop_domain: string }>(
    `
      SELECT shop_domain
      FROM shopify_app_installations
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `
  );

  return result.rows[0]?.shop_domain ?? null;
}

async function getActiveShopifyAccessToken(client: PoolClient, shopDomain: string): Promise<string> {
  const result = await client.query<{ access_token: string }>(
    `
      SELECT
        pgp_sym_decrypt(access_token_encrypted, $2) AS access_token
      FROM shopify_app_installations
      WHERE shop_domain = $1
        AND status = 'active'
      LIMIT 1
    `,
    [shopDomain, env.SHOPIFY_APP_ENCRYPTION_KEY]
  );

  if (!result.rowCount) {
    throw new Error('shopify_installation_not_found');
  }

  return result.rows[0].access_token;
}

async function callShopifyAdminGraphql<TData>(
  shopDomain: string,
  accessToken: string,
  graphqlQuery: string,
  variables?: Record<string, unknown>
): Promise<TData> {
  const response = await fetch(`https://${shopDomain}/admin/api/${env.SHOPIFY_APP_API_VERSION}/graphql.json`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json',
      'x-shopify-access-token': accessToken
    },
    body: JSON.stringify({
      query: graphqlQuery,
      variables: variables ?? {}
    })
  });

  const payload = (await response.json()) as ShopifyGraphqlResponse<TData>;

  if (!response.ok || payload.errors?.length || !payload.data) {
    throw new Error(
      `shopify_admin_api_failed:${response.status}:${payload.errors?.map((error) => error.message).join(',') ?? 'unknown'}`
    );
  }

  return payload.data;
}

async function fetchOrderCustomAttributes(
  shopDomain: string,
  accessToken: string,
  shopifyOrderId: string
): Promise<{ orderGid: string; customAttributes: ShopifyOrderAttribute[] }> {
  const data = await callShopifyAdminGraphql<ShopifyOrderQueryResponse>(
    shopDomain,
    accessToken,
    `
      query OrderWritebackOrder($id: ID!) {
        order(id: $id) {
          id
          customAttributes {
            key
            value
          }
        }
      }
    `,
    {
      id: buildShopifyOrderGid(shopifyOrderId)
    }
  );

  if (!data.order) {
    throw new Error('shopify_order_not_found');
  }

  return {
    orderGid: data.order.id,
    customAttributes: Array.isArray(data.order.customAttributes) ? data.order.customAttributes : []
  };
}

async function updateOrderCustomAttributes(
  shopDomain: string,
  accessToken: string,
  orderGid: string,
  customAttributes: Array<{ key: string; value: string }>
): Promise<void> {
  const data = await callShopifyAdminGraphql<ShopifyOrderUpdateResponse>(
    shopDomain,
    accessToken,
    `
      mutation UpdateOrderWritebackAttributes($input: OrderInput!) {
        orderUpdate(input: $input) {
          userErrors {
            field
            message
          }
        }
      }
    `,
    {
      input: {
        id: orderGid,
        customAttributes
      }
    }
  );

  if (data.orderUpdate.userErrors.length > 0) {
    throw new Error(
      `shopify_order_update_failed:${data.orderUpdate.userErrors.map((error) => error.message).join(',')}`
    );
  }
}

async function setOrderMetafields(
  shopDomain: string,
  accessToken: string,
  orderGid: string,
  metafields: Array<{ namespace: string; key: string; type: string; value: string }>
): Promise<void> {
  const data = await callShopifyAdminGraphql<ShopifyMetafieldsSetResponse>(
    shopDomain,
    accessToken,
    `
      mutation SetOrderWritebackMetafields($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          userErrors {
            field
            message
            code
          }
        }
      }
    `,
    {
      metafields: metafields.map((metafield) => ({
        ownerId: orderGid,
        ...metafield
      }))
    }
  );

  if (data.metafieldsSet.userErrors.length > 0) {
    throw new Error(
      `shopify_metafields_set_failed:${data.metafieldsSet.userErrors.map((error) => error.message).join(',')}`
    );
  }
}

async function fetchWritebackContext(
  client: PoolClient,
  shopifyOrderId: string
): Promise<ShopifyOrderWritebackContextRow | null> {
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
      WHERE o.shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

async function fetchLatestEventForSession(
  client: PoolClient,
  sessionId: string,
  orderOccurredAt: Date
): Promise<TrackingEventRow | null> {
  const result = await client.query<TrackingEventRow>(
    `
      SELECT
        page_url,
        referrer_url
      FROM tracking_events
      WHERE session_id = $1::uuid
        AND occurred_at <= $2
      ORDER BY occurred_at DESC, id DESC
      LIMIT 1
    `,
    [sessionId, orderOccurredAt]
  );

  return result.rows[0] ?? null;
}

async function buildCapturePayload(
  client: PoolClient,
  context: ShopifyOrderWritebackContextRow
): Promise<ReturnType<typeof normalizeAttributionCaptureV1> | null> {
  if (!context.roas_radar_session_id || !context.first_seen_at) {
    return null;
  }

  const latestEvent = await fetchLatestEventForSession(client, context.roas_radar_session_id, context.order_occurred_at);

  return normalizeAttributionCaptureV1({
    schema_version: ATTRIBUTION_SCHEMA_VERSION,
    roas_radar_session_id: context.roas_radar_session_id,
    occurred_at: context.first_seen_at.toISOString(),
    captured_at: new Date().toISOString(),
    landing_url: context.landing_url,
    referrer_url: latestEvent?.referrer_url ?? context.session_referrer_url,
    page_url: latestEvent?.page_url ?? null,
    utm_source: context.initial_utm_source,
    utm_medium: context.initial_utm_medium,
    utm_campaign: context.initial_utm_campaign,
    utm_content: context.initial_utm_content,
    utm_term: context.initial_utm_term,
    gclid: context.initial_gclid,
    gbraid: context.initial_gbraid,
    wbraid: context.initial_wbraid,
    fbclid: context.initial_fbclid,
    ttclid: context.initial_ttclid,
    msclkid: context.initial_msclkid
  });
}

async function enqueueRetryOrDeadLetter(
  client: PoolClient,
  job: ShopifyOrderWritebackJobRow,
  workerId: string,
  error: unknown
): Promise<'retry' | 'failed'> {
  const errorMessage = (error instanceof Error ? error.message : String(error)).slice(0, 1000);
  const shouldRetry = job.attempts < env.SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES;
  const nextStatus = shouldRetry ? 'retry' : 'failed';

  await client.query(
    `
      UPDATE shopify_order_writeback_jobs
      SET
        status = $2,
        available_at = CASE
          WHEN $2 = 'retry' THEN now() + ($3::int * interval '1 second')
          ELSE available_at
        END,
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        completed_at = CASE WHEN $2 = 'failed' THEN now() ELSE completed_at END,
        dead_lettered_at = CASE WHEN $2 = 'failed' THEN now() ELSE dead_lettered_at END,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $5
    `,
    [job.id, nextStatus, computeShopifyOrderWritebackRetryDelaySeconds(job.attempts), errorMessage, workerId]
  );

  return nextStatus;
}

async function requeueStaleJobs(client: PoolClient, staleScanLimit: number): Promise<number> {
  if (staleScanLimit <= 0) {
    return 0;
  }

  const result = await client.query<{ id: number }>(
    `
      WITH stale_jobs AS (
        SELECT id
        FROM shopify_order_writeback_jobs
        WHERE status = 'processing'
          AND locked_at < now() - ($1::int * interval '1 minute')
        ORDER BY locked_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE shopify_order_writeback_jobs j
      SET
        status = 'retry',
        locked_at = NULL,
        locked_by = NULL,
        available_at = now(),
        last_error = COALESCE(j.last_error, 'job_requeued_after_stale_lock'),
        updated_at = now()
      FROM stale_jobs
      WHERE j.id = stale_jobs.id
      RETURNING j.id
    `,
    [JOB_STALE_AFTER_MINUTES, staleScanLimit]
  );

  return result.rowCount ?? result.rows.length;
}

async function claimJobs(
  client: PoolClient,
  workerId: string,
  limit: number
): Promise<ShopifyOrderWritebackJobRow[]> {
  const result = await client.query<ShopifyOrderWritebackJobRow>(
    `
      WITH candidate_jobs AS (
        SELECT id
        FROM shopify_order_writeback_jobs
        WHERE status IN ('pending', 'retry')
          AND available_at <= now()
        ORDER BY available_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE shopify_order_writeback_jobs j
      SET
        status = 'processing',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1,
        updated_at = now()
      FROM candidate_jobs
      WHERE j.id = candidate_jobs.id
      RETURNING j.id, j.shopify_order_id, j.attempts
    `,
    [workerId, Math.max(limit, 0)]
  );

  return result.rows;
}

async function markJobCompleted(client: PoolClient, jobId: number, workerId: string): Promise<void> {
  await client.query(
    `
      UPDATE shopify_order_writeback_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `,
    [jobId, workerId]
  );
}

async function markOrderMissing(client: PoolClient, jobId: number, workerId: string): Promise<void> {
  await client.query(
    `
      UPDATE shopify_order_writeback_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = 'order_not_found',
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `,
    [jobId, workerId]
  );
}

async function processClaimedJob(client: PoolClient, job: ShopifyOrderWritebackJobRow, workerId: string): Promise<void> {
  const context = await fetchWritebackContext(client, job.shopify_order_id);

  if (!context) {
    await markOrderMissing(client, job.id, workerId);
    return;
  }

  const shopDomain = await getActiveInstalledShopDomain(client);
  if (!shopDomain) {
    throw new Error('shopify_installation_not_found');
  }

  const accessToken = await getActiveShopifyAccessToken(client, shopDomain);
  const { orderGid, customAttributes } = await fetchOrderCustomAttributes(shopDomain, accessToken, job.shopify_order_id);
  const capturePayload = await buildCapturePayload(client, context);

  if (capturePayload) {
    const mergedAttributes = mergeOrderAttributes(customAttributes, buildCaptureAttributes(capturePayload));
    await updateOrderCustomAttributes(shopDomain, accessToken, orderGid, mergedAttributes);
  }

  const resolvedAttributionPayload = buildResolvedAttributionPayload(context);
  if (!resolvedAttributionPayload) {
    throw new Error('attribution_result_not_ready');
  }

  const metafields = [
    ...(capturePayload
      ? [
          {
            namespace: SHOPIFY_WRITEBACK_NAMESPACE,
            key: SHOPIFY_CAPTURE_METAFIELD_KEY,
            type: 'json',
            value: JSON.stringify(capturePayload)
          }
        ]
      : []),
    {
      namespace: SHOPIFY_WRITEBACK_NAMESPACE,
      key: SHOPIFY_RESULT_METAFIELD_KEY,
      type: 'json',
      value: resolvedAttributionPayload
    }
  ];

  await setOrderMetafields(shopDomain, accessToken, orderGid, metafields);
  await markJobCompleted(client, job.id, workerId);

  logInfo('shopify_order_writeback_completed', {
    workerId,
    shopifyOrderId: job.shopify_order_id,
    roasRadarSessionId: context.roas_radar_session_id,
    attributionReason: context.attribution_reason
  });
}

export async function enqueueShopifyOrderWriteback(
  shopifyOrderId: string,
  requestedReason: string,
  client?: PoolClient
): Promise<void> {
  await execute(client, async (db) => {
    await db.query(
      `
        INSERT INTO shopify_order_writeback_jobs (
          queue_key,
          shopify_order_id,
          requested_reason,
          status,
          attempts,
          available_at,
          updated_at
        )
        VALUES ($1, $2, $3, 'pending', 0, now(), now())
        ON CONFLICT (queue_key)
        DO UPDATE SET
          requested_reason = EXCLUDED.requested_reason,
          status = CASE
            WHEN shopify_order_writeback_jobs.status = 'processing' THEN shopify_order_writeback_jobs.status
            ELSE 'pending'
          END,
          available_at = CASE
            WHEN shopify_order_writeback_jobs.status = 'processing' THEN shopify_order_writeback_jobs.available_at
            ELSE now()
          END,
          completed_at = NULL,
          dead_lettered_at = NULL,
          last_error = NULL,
          updated_at = now()
      `,
      [buildShopifyOrderWritebackQueueKey(shopifyOrderId), shopifyOrderId, requestedReason]
    );
  });
}

export async function processShopifyOrderWritebackQueue(
  options: ShopifyOrderWritebackQueueProcessOptions
): Promise<ShopifyOrderWritebackQueueProcessResult> {
  const startedAt = Date.now();

  const initialState = await withTransaction(async (client) => {
    const staleJobsEnqueued = await requeueStaleJobs(client, options.staleScanLimit ?? 0);
    const claimedJobs = await claimJobs(client, options.workerId, options.limit);

    return {
      staleJobsEnqueued,
      claimedJobs
    };
  });

  let succeededJobs = 0;
  let failedJobs = 0;
  let deadLetteredJobs = 0;

  for (const job of initialState.claimedJobs) {
    try {
      await withTransaction(async (client) => {
        await processClaimedJob(client, job, options.workerId);
      });
      succeededJobs += 1;
    } catch (error) {
      const outcome = await withTransaction(async (client) => enqueueRetryOrDeadLetter(client, job, options.workerId, error));
      failedJobs += 1;

      if (outcome === 'failed') {
        deadLetteredJobs += 1;
      }

      logError('shopify_order_writeback_failed', error, {
        workerId: options.workerId,
        shopifyOrderId: job.shopify_order_id,
        attempts: job.attempts,
        deadLettered: outcome === 'failed'
      });
    }
  }

  return {
    workerId: options.workerId,
    claimedJobs: initialState.claimedJobs.length,
    staleJobsEnqueued: initialState.staleJobsEnqueued,
    succeededJobs,
    failedJobs,
    deadLetteredJobs,
    durationMs: Date.now() - startedAt
  };
}

export const __shopifyWritebackTestUtils = {
  buildShopifyOrderWritebackQueueKey,
  buildShopifyOrderGid,
  computeShopifyOrderWritebackRetryDelaySeconds
};
