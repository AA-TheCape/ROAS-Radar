import { createReadStream, existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const distDir = path.join(__dirname, 'dist');
const port = Number(process.env.PORT ?? '8080');

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'application/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.ico', 'image/x-icon']
]);

function writeJson(response, statusCode, body) {
  response.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(body));
}

function writeRuntimeConfig(response) {
  const payload = {
    apiBaseUrl: process.env.DASHBOARD_API_BASE_URL ?? process.env.VITE_API_BASE_URL ?? 'http://localhost:3000',
    reportingToken: process.env.DASHBOARD_REPORTING_API_TOKEN ?? process.env.VITE_REPORTING_API_TOKEN ?? '',
    reportingTenantId: process.env.DASHBOARD_REPORTING_TENANT_ID ?? process.env.VITE_REPORTING_TENANT_ID ?? 'roas-radar'
  };

  response.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(payload));
}

async function serveFile(response, relativePath) {
  const resolvedPath = path.join(distDir, relativePath);
  const safePath = path.normalize(resolvedPath);

  if (!safePath.startsWith(distDir)) {
    writeJson(response, 403, { error: 'forbidden' });
    return;
  }

  if (!existsSync(safePath)) {
    const indexHtml = await readFile(path.join(distDir, 'index.html'));
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    response.end(indexHtml);
    return;
  }

  const extension = path.extname(safePath);
  response.writeHead(200, {
    'content-type': contentTypes.get(extension) ?? 'application/octet-stream'
  });

  createReadStream(safePath).pipe(response);
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);

  if (url.pathname === '/healthz') {
    writeJson(response, 200, { ok: true });
    return;
  }

  if (url.pathname === '/config.json') {
    writeRuntimeConfig(response);
    return;
  }

  const relativePath = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  serveFile(response, relativePath).catch((error) => {
    writeJson(response, 500, {
      error: error instanceof Error ? error.message : 'unexpected_error'
    });
  });
});

server.listen(port, () => {
  process.stdout.write(`dashboard listening on ${port}\n`);
});
