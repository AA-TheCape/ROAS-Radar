import { randomUUID } from 'node:crypto';

import express from 'express';
import { ZodError } from 'zod';

import { createMetaAdsAdminRouter, createMetaAdsPublicRouter } from './modules/meta-ads/index.js';
import { createReportingRouter } from './modules/reporting/index.js';
import {
  createShopifyAdminRouter,
  createShopifyPublicRouter,
  createShopifyWebhookRouter
} from './modules/shopify/index.js';
import { createTrackingRouter } from './modules/tracking/index.js';

type HttpErrorShape = {
  statusCode: number;
  code: string;
  details?: unknown;
};

function isHttpErrorShape(error: unknown): error is HttpErrorShape {
  return (
    typeof error === 'object' &&
    error !== null &&
    'statusCode' in error &&
    typeof error.statusCode === 'number' &&
    'code' in error &&
    typeof error.code === 'string'
  );
}

export function createApp() {
  const app = express();
  const trackingBodyParser = express.text({
    type: ['application/json', 'text/plain'],
    limit: '1mb'
  });

  app.disable('x-powered-by');
  app.set('trust proxy', true);

  app.use((req, res, next) => {
    const requestId = req.header('x-request-id') ?? randomUUID();
    res.setHeader('x-request-id', requestId);
    next();
  });

  app.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  app.use('/shopify', createShopifyPublicRouter());
  app.use('/meta-ads', createMetaAdsPublicRouter());
  app.use('/webhooks/shopify', express.raw({ type: 'application/json', limit: '2mb' }), createShopifyWebhookRouter());
  app.use('/track', trackingBodyParser, createTrackingRouter());
  app.use('/api/track', trackingBodyParser, createTrackingRouter());
  app.use(express.json({ limit: '1mb' }));
  app.use('/api/reporting', createReportingRouter());
  app.use('/api/shopify', createShopifyAdminRouter());
  app.use('/api/meta-ads', createMetaAdsAdminRouter());

  app.use((error: unknown, req: express.Request, res: express.Response, _next: express.NextFunction) => {
    if (error instanceof ZodError) {
      res.status(400).json({
        error: {
          code: 'validation_failed',
          message: 'Validation failed',
          details: error.flatten(),
          requestId: res.getHeader('x-request-id') ?? req.header('x-request-id') ?? null
        }
      });
      return;
    }

    if (isHttpErrorShape(error)) {
      res.status(error.statusCode).json({
        error: {
          code: error.code,
          message: error instanceof Error ? error.message : 'Request failed',
          details: error.details ?? null,
          requestId: res.getHeader('x-request-id') ?? req.header('x-request-id') ?? null
        }
      });
      return;
    }

    const message = error instanceof Error ? error.message : 'Internal server error';
    process.stderr.write(`${message}\n`);
    res.status(500).json({
      error: {
        code: 'internal_server_error',
        message,
        requestId: res.getHeader('x-request-id') ?? req.header('x-request-id') ?? null
      }
    });
  });

  return app;
}
