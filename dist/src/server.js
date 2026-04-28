"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createServer = createServer;
exports.closeServer = closeServer;
const node_http_1 = require("node:http");
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const app_js_1 = require("./app.js");
function createServer(port = 0) {
    const app = (0, app_js_1.createApp)();
    const server = (0, node_http_1.createServer)(app);
    server.listen(port);
    return server;
}
async function closeServer(server) {
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
    const server = createServer(env_js_1.env.PORT);
    const shutdown = async () => {
        await closeServer(server).catch(() => undefined);
        await pool_js_1.pool.end().catch(() => undefined);
        process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}
