import { randomUUID } from 'node:crypto';

import express from 'express';
import { ZodError } from 'zod';

import { createReportingRouter } from './modules/reporting/index.js';
import { createShopifyRouter } from './modules/shopify/index.js';
import { createTrackingRouter } from './modules/tracking/index.js';

export function createApp() {
  const app = express();

  app.disable('x-powered-by');

  app.use((req, res, next) => {
    const requestId = req.header('x-request-id') ?? randomUUID();
    res.setHeader('x-request-id', requestId);
    next();
  });

  app.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  app.use('/webhooks/shopify', express.raw({ type: 'application/json', limit: '2mb' }), createShopifyRouter());
  app.use(express.json({ limit: '1mb' }));
  app.use('/track', createTrackingRouter());
  app.use('/api/reporting', createReportingRouter());

  app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    if (error instanceof ZodError) {
      res.status(400).json({
        error: 'Validation failed',
        details: error.flatten()
      });
      return;
    }

    const message = error instanceof Error ? error.message : 'Internal server error';
    process.stderr.write(`${message}\n`);
    res.status(500).json({ error: message });
  });

  return app;
}
