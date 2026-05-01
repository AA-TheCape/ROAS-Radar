import { createReadStream, existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const distDir = path.join(__dirname, 'dist');
const port = Number(process.env.PORT ?? '8080');
const apiBaseUrl = (process.env.DASHBOARD_API_BASE_URL ?? process.env.VITE_API_BASE_URL ?? 'http://localhost:3000').replace(
  /\/$/,
  ''
);

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'application/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml']
]);

function shouldProxyToApi(pathname) {
  return (
    pathname.startsWith('/api/') ||
    pathname.startsWith('/google-ads/') ||
    pathname.startsWith('/meta-ads/') ||
    pathname.startsWith('/shopify/') ||
    pathname === '/track'
  );
}

function writeJson(response, statusCode, body) {
  response.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(body));
}

function writeRuntimeConfig(response) {
  const payload = {
    apiBaseUrl: '',
    reportingToken: process.env.DASHBOARD_REPORTING_API_TOKEN ?? process.env.VITE_REPORTING_API_TOKEN ?? '',
    reportingTenantId: process.env.DASHBOARD_REPORTING_TENANT_ID ?? process.env.VITE_REPORTING_TENANT_ID ?? '1'
  };

  response.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(payload));
}

function copyHeaders(sourceHeaders, response) {
  for (const [key, value] of sourceHeaders.entries()) {
    if (key.toLowerCase() === 'transfer-encoding') {
      continue;
    }

    response.setHeader(key, value);
  }
}

async function readRequestBody(request) {
  const chunks = [];

  for await (const chunk of request) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }

  return chunks.length ? Buffer.concat(chunks) : undefined;
}

async function proxyApiRequest(request, response, url) {
  const upstreamUrl = `${apiBaseUrl}${url.pathname}${url.search}`;
  const headers = new Headers();

  for (const [key, value] of Object.entries(request.headers)) {
    if (value === undefined || key.toLowerCase() === 'host') {
      continue;
    }

    if (Array.isArray(value)) {
      for (const entry of value) {
        headers.append(key, entry);
      }
      continue;
    }

    headers.set(key, value);
  }

  const body =
    request.method && ['GET', 'HEAD'].includes(request.method.toUpperCase()) ? undefined : await readRequestBody(request);

  const upstreamResponse = await fetch(upstreamUrl, {
    method: request.method ?? 'GET',
    headers,
    body
  });

  copyHeaders(upstreamResponse.headers, response);
  response.statusCode = upstreamResponse.status;

  if (!upstreamResponse.body) {
    response.end();
    return;
  }

  const responseBody = Buffer.from(await upstreamResponse.arrayBuffer());
  response.end(responseBody);
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

  if (shouldProxyToApi(url.pathname)) {
    proxyApiRequest(request, response, url).catch((error) => {
      writeJson(response, 502, {
        error: error instanceof Error ? error.message : 'unexpected_proxy_error'
      });
    });
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
