import express, { type NextFunction, type Request, type Response } from 'express';

import { env } from './config/env.js';
import { checkDatabaseHealth } from './db/pool.js';
import { createAuthRouter, createUserAdminRouter } from './modules/auth/index.js';
import { createAttributionAdminRouter } from './modules/attribution/admin.js';
import { createGoogleAdsAdminRouter, createGoogleAdsPublicRouter } from './modules/google-ads/index.js';
import { createMetaAdsAdminRouter, createMetaAdsPublicRouter } from './modules/meta-ads/index.js';
import { createReportingRouter } from './modules/reporting/index.js';
import { createSettingsRouter } from './modules/settings/index.js';
import { createShopifyAdminRouter, createShopifyPublicRouter, createShopifyWebhookRouter } from './modules/shopify/index.js';
import { createTrackingRouter } from './modules/tracking/index.js';
import { createInternalIdentityRouter } from './modules/identity/read-api.js';
import { createRequestLoggingMiddleware, logHttpError } from './observability/index.js';

export function createApp() {
  const app = express();
  const serviceName = process.env.K_SERVICE ?? 'roas-radar-api';

  app.disable('x-powered-by');
  app.use(createRequestLoggingMiddleware(serviceName));
  app.use((req, res, next) => {
    const origin = req.header('origin');

    res.setHeader('Access-Control-Allow-Origin', origin ?? '*');
    if (origin) {
      res.setHeader('Vary', 'Origin');
    }

    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'authorization,content-type,x-roas-radar-tenant-id');

    if (req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }

    next();
  });

  app.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  app.get('/readyz', async (_req, res) => {
    try {
      const status = await checkDatabaseHealth();
      res.status(200).json(status);
    } catch (error) {
      res.status(503).json({
        ok: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  app.use(
    '/webhooks/shopify',
    express.raw({ type: '*/*', limit: env.SHOPIFY_WEBHOOK_BODY_LIMIT }),
    createShopifyWebhookRouter()
  );
  app.use(
    '/track',
    express.text({ type: 'text/plain', limit: env.TRACKING_BODY_LIMIT }),
    express.json({ type: 'application/json', limit: env.TRACKING_BODY_LIMIT }),
    createTrackingRouter()
  );
  app.use(express.json({ limit: env.API_JSON_BODY_LIMIT }));
  app.use('/api/auth', createAuthRouter());
  app.use('/api/settings', createSettingsRouter());
  app.use('/api/reporting', createReportingRouter());
  app.use('/api/internal/identity', createInternalIdentityRouter());
  app.use('/api/admin/users', createUserAdminRouter());
  app.use('/api/admin/attribution', createAttributionAdminRouter());
  app.use('/shopify', createShopifyPublicRouter());
  app.use('/api/admin/shopify', createShopifyAdminRouter());
  app.use('/meta-ads', createMetaAdsPublicRouter());
  app.use('/api/admin/meta-ads', createMetaAdsAdminRouter());
  app.use('/google-ads', createGoogleAdsPublicRouter());
  app.use('/api/admin/google-ads', createGoogleAdsAdminRouter());

  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const statusCode =
      typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number'
        ? error.statusCode
        : typeof error === 'object' &&
            error !== null &&
            'type' in error &&
            (error as { type?: unknown }).type === 'entity.too.large'
          ? 413
        : 500;
    const code =
      typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string'
        ? error.code
        : typeof error === 'object' &&
            error !== null &&
            'type' in error &&
            (error as { type?: unknown }).type === 'entity.too.large'
          ? 'payload_too_large'
        : 'internal_server_error';
    const message = error instanceof Error ? error.message : 'Unexpected error';
    const details =
      typeof error === 'object' && error !== null && 'details' in error ? (error as { details?: unknown }).details : undefined;

    if (statusCode >= 500) {
      logHttpError('http_request_failed', error, _req, {
        responseStatusCode: statusCode
      });
    }

    res.status(statusCode).json({
      error: code,
      message,
      ...(details === undefined ? {} : { details })
    });
  });

  return app;
}
