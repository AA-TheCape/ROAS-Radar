import express, { type NextFunction, type Request, type Response } from 'express';

import { checkDatabaseHealth } from './db/pool.js';
import { createGoogleAdsAdminRouter } from './modules/google-ads/index.js';
import { createMetaAdsAdminRouter, createMetaAdsPublicRouter } from './modules/meta-ads/index.js';
import { createReportingRouter } from './modules/reporting/index.js';
import { createShopifyAdminRouter, createShopifyPublicRouter, createShopifyWebhookRouter } from './modules/shopify/index.js';
import { createTrackingRouter } from './modules/tracking/index.js';

export function createApp() {
  const app = express();

  app.disable('x-powered-by');

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

  app.use('/webhooks/shopify', express.raw({ type: '*/*', limit: '2mb' }), createShopifyWebhookRouter());
  app.use(express.json({ limit: '1mb' }));

  app.use('/track', createTrackingRouter());
  app.use('/api/reporting', createReportingRouter());
  app.use('/shopify', createShopifyPublicRouter());
  app.use('/api/admin/shopify', createShopifyAdminRouter());
  app.use('/meta-ads', createMetaAdsPublicRouter());
  app.use('/api/admin/meta-ads', createMetaAdsAdminRouter());
  app.use('/api/admin/google-ads', createGoogleAdsAdminRouter());

  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const statusCode =
      typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number'
        ? error.statusCode
        : 500;
    const code =
      typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string'
        ? error.code
        : 'internal_server_error';
    const message = error instanceof Error ? error.message : 'Unexpected error';
    const details =
      typeof error === 'object' && error !== null && 'details' in error ? (error as { details?: unknown }).details : undefined;

    if (statusCode >= 500) {
      process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    }

    res.status(statusCode).json({
      error: code,
      message,
      ...(details === undefined ? {} : { details })
    });
  });

  return app;
}
