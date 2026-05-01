import { createServer as createHttpServer } from 'node:http';
import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { createApp } from './app.js';
import { startMetaAdsOrderValueScheduler } from './modules/meta-ads/index.js';
export function createServer(port = 0) {
    const app = createApp();
    const server = createHttpServer(app);
    server.listen(port);
    return server;
}
export async function closeServer(server) {
    await new Promise((resolve, reject) => {
        server.close((error) => {
            if (error) {
                reject(error);
                return;
            }
            resolve();
        });
    });
}
const isEntrypoint = process.argv[1] && import.meta.url.endsWith(process.argv[1]);
if (isEntrypoint) {
    const server = createServer(env.PORT);
    const stopMetaAdsOrderValueScheduler = startMetaAdsOrderValueScheduler();
    const shutdown = async () => {
        stopMetaAdsOrderValueScheduler();
        await closeServer(server).catch(() => undefined);
        await pool.end().catch(() => undefined);
        process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}
