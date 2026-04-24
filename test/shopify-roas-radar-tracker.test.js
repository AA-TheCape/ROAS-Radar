import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const trackerScriptPath = path.resolve(__dirname, "../shopify/theme/assets/roas-radar-tracker.js");
const trackerScriptSource = await fs.readFile(trackerScriptPath, "utf8");

function createStorage(seed) {
  const data = seed || new Map();

  return {
    data,
    getItem(key) {
      return data.has(key) ? data.get(key) : null;
    },
    setItem(key, value) {
      data.set(key, String(value));
    },
    removeItem(key) {
      data.delete(key);
    }
  };
}

function createDocument(cookieJar, state) {
  const listeners = new Map();
  const dispatchedEvents = [];
  const cookieAssignments = [];

  const document = {
    referrer: "https://www.google.com/search?q=roas",
    readyState: state || "complete",
    addEventListener(name, handler) {
      listeners.set(name, handler);
    },
    dispatchEvent(event) {
      dispatchedEvents.push(event);
      return true;
    },
    querySelectorAll() {
      return [];
    },
    createElement(tagName) {
      return {
        tagName,
        type: "",
        name: "",
        value: ""
      };
    },
    __listeners: listeners,
    __dispatchedEvents: dispatchedEvents,
    __cookieAssignments: cookieAssignments
  };

  Object.defineProperty(document, "cookie", {
    get() {
      return Array.from(cookieJar.entries())
        .map(function ([key, value]) {
          return key + "=" + encodeURIComponent(value);
        })
        .join("; ");
    },
    set(value) {
      cookieAssignments.push(value);
      const firstSegment = String(value).split(";")[0];
      const separatorIndex = firstSegment.indexOf("=");
      const key = firstSegment.slice(0, separatorIndex);
      const cookieValue = decodeURIComponent(firstSegment.slice(separatorIndex + 1));
      cookieJar.set(key, cookieValue);
    }
  });

  return document;
}

function createXmlHttpRequest(log, statusCode) {
  function MockXMLHttpRequest() {
    this.readyState = 0;
    this.status = 0;
    this.onreadystatechange = null;
    this.onerror = null;
  }

  MockXMLHttpRequest.prototype.open = function (method, url) {
    this.method = method;
    this.url = url;
  };

  MockXMLHttpRequest.prototype.setRequestHeader = function () {
    return;
  };

  MockXMLHttpRequest.prototype.send = function (body) {
    log.push({
      method: this.method,
      url: this.url,
      body
    });

    this.readyState = 4;
    this.status = statusCode;

    if (typeof this.onreadystatechange === "function") {
      this.onreadystatechange();
    }
  };

  return MockXMLHttpRequest;
}

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function runTracker(overrides = {}) {
  const cookieJar = overrides.cookieJar || new Map();
  const localStorage = createStorage(overrides.localStorageData);
  const sessionStorage = createStorage(overrides.sessionStorageData);
  const beaconCalls = [];
  const fetchCalls = [];
  const xhrCalls = [];
  const document = createDocument(cookieJar, overrides.documentReadyState);
  const navigator = {
    userAgent: "Mozilla/5.0 Test Browser",
    language: "en-US",
    sendBeacon: overrides.sendBeacon
      ? function (url, body) {
          beaconCalls.push({ url, body });
          return overrides.sendBeacon(url, body);
        }
      : undefined
  };
  const windowObject = {
    ROASRadarConfig: overrides.config || {},
    location: Object.assign(
      {
        origin: "https://store.example.com",
        pathname: "/products/widget",
        search: "?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123",
        protocol: "https:"
      },
      overrides.location || {}
    ),
    document,
    navigator,
    localStorage,
    sessionStorage,
    screen: {
      width: 1440,
      height: 900
    },
    crypto: {
      randomUUID: overrides.randomUUID || (() => "123e4567-e89b-42d3-a456-426614174000")
    },
    fetch: overrides.fetch
      ? function (url, options) {
          fetchCalls.push({ url, options });
          return overrides.fetch(url, options);
        }
      : undefined,
    XMLHttpRequest: overrides.XMLHttpRequest || createXmlHttpRequest(xhrCalls, 204),
    CustomEvent: function CustomEvent(type, init) {
      this.type = type;
      this.detail = init ? init.detail : undefined;
    },
    URL,
    setTimeout,
    clearTimeout,
    Promise,
    Math,
    Date,
    JSON,
    console
  };

  windowObject.window = windowObject;
  windowObject.self = windowObject;

  const context = vm.createContext({
    window: windowObject,
    document,
    navigator,
    localStorage,
    sessionStorage,
    console,
    URL,
    setTimeout,
    clearTimeout,
    Promise,
    Math,
    Date,
    JSON
  });

  vm.runInContext(trackerScriptSource, context, { filename: trackerScriptPath });

  if (document.readyState === "loading") {
    const handler = document.__listeners.get("DOMContentLoaded");
    if (handler) {
      handler();
    }
  }

  await flushAsyncWork();

  return {
    cookieJar,
    localStorageData: localStorage.data,
    sessionStorageData: sessionStorage.data,
    document,
    beaconCalls,
    fetchCalls,
    xhrCalls
  };
}

function createJsonResponse(body, ok = true, statusCode = 200) {
  return {
    ok,
    status: statusCode,
    json: async function () {
      return body;
    }
  };
}

function createBootstrapFetch(sessionId, createdAt, extraHandler) {
  return async function (url, options) {
    if (String(url).startsWith("/track/session")) {
      return createJsonResponse({
        ok: true,
        sessionId,
        createdAt,
        isNewSession: true
      });
    }

    if (typeof extraHandler === "function") {
      return extraHandler(url, options);
    }

    return createJsonResponse({}, true, 200);
  };
}

test("persists a 365-day _hba_id cookie and reuses it across sessions", async () => {
  const cookieJar = new Map();
  const createdAt = "2026-04-23T12:00:00.000Z";
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const firstRun = await runTracker({
    cookieJar,
    fetch: createBootstrapFetch(sessionId, createdAt),
    sendBeacon: () => true
  });

  assert.equal(cookieJar.get("_hba_id"), sessionId);
  assert.ok(
    firstRun.document.__cookieAssignments.some((value) => value.includes("Max-Age=31536000")),
    "expected cookie max age to cover 365 days"
  );
  assert.equal(firstRun.localStorageData.get("_hba_id"), sessionId);
  assert.equal(firstRun.localStorageData.get("roas_radar_session_created_at"), createdAt);

  const secondRun = await runTracker({
    cookieJar,
    localStorageData: firstRun.localStorageData,
    sessionStorageData: firstRun.sessionStorageData,
    fetch: createBootstrapFetch(sessionId, createdAt),
    sendBeacon: () => true
  });

  const secondPayload = JSON.parse(secondRun.beaconCalls[0].body);
  assert.equal(secondPayload.sessionId, sessionId);
  assert.equal(secondPayload.consentState, "unknown");
  assert.equal(cookieJar.get("_hba_id"), sessionId);
});

test("emits the required tracking payload fields on page load", async () => {
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const run = await runTracker({
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z"),
    sendBeacon: () => true
  });

  assert.equal(run.beaconCalls.length, 1);
  assert.equal(run.fetchCalls.length, 1);
  assert.match(run.fetchCalls[0].url, /^\/track\/session\?/);

  const call = run.beaconCalls[0];
  const payload = JSON.parse(call.body);

  assert.equal(call.url, "/track");
  assert.equal(payload.eventType, "page_view");
  assert.match(payload.occurredAt, /^\d{4}-\d{2}-\d{2}T/);
  assert.equal(payload.sessionId, sessionId);
  assert.equal(
    payload.pageUrl,
    "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123"
  );
  assert.equal(payload.referrerUrl, "https://www.google.com/search?q=roas");
  assert.equal(payload.shopifyCartToken, null);
  assert.equal(payload.shopifyCheckoutToken, null);
  assert.match(payload.clientEventId, /^[0-9a-f-]{36}$/i);
  assert.equal(payload.consentState, "unknown");
  assert.equal(payload.schema_version, 1);
  assert.equal(payload.roas_radar_session_id, sessionId);
  assert.equal(
    payload.landing_url,
    "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123"
  );
  assert.equal(payload.referrer_url, "https://www.google.com/search?q=roas");
  assert.equal(
    payload.page_url,
    "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123"
  );
  assert.equal(payload.utm_source, "google");
  assert.equal(payload.utm_medium, "cpc");
  assert.equal(payload.utm_campaign, "spring-sale");
  assert.equal(payload.utm_content, null);
  assert.equal(payload.utm_term, null);
  assert.equal(payload.gclid, "abc123");
  assert.equal(payload.gbraid, null);
  assert.equal(payload.wbraid, null);
  assert.equal(payload.fbclid, null);
  assert.equal(payload.ttclid, null);
  assert.equal(payload.msclkid, null);
  assert.equal(payload.context.userAgent, "Mozilla/5.0 Test Browser");
  assert.equal(payload.context.screen, "1440x900");
  assert.equal(payload.context.language, "en-US");
  const storedCapture = JSON.parse(run.localStorageData.get("roas_radar_attribution_capture_v1"));
  assert.equal(storedCapture.roas_radar_session_id, sessionId);
  assert.equal(storedCapture.utm_source, "google");
});

test("captures explicit consent state without suppressing the tracking payload", async () => {
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const run = await runTracker({
    config: {
      consentState: "denied"
    },
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z"),
    sendBeacon: () => true
  });

  const payload = JSON.parse(run.beaconCalls[0].body);
  assert.equal(payload.consentState, "denied");
  assert.equal(
    payload.pageUrl,
    "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123"
  );
});

test("falls back to fetch when sendBeacon is unsupported or returns false", async () => {
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const run = await runTracker({
    sendBeacon: () => false,
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z", async function (url) {
      if (url === "/track") {
        return { ok: true };
      }

      return createJsonResponse({}, true, 200);
    })
  });

  assert.equal(run.beaconCalls.length, 1);
  assert.equal(run.fetchCalls.length, 2);
  assert.match(run.fetchCalls[0].url, /^\/track\/session\?/);
  assert.equal(run.fetchCalls[1].url, "/track");
  assert.equal(run.fetchCalls[1].options.keepalive, true);
});

test("falls back to XMLHttpRequest when sendBeacon and fetch are unavailable", async () => {
  const run = await runTracker();

  assert.equal(run.beaconCalls.length, 0);
  assert.equal(run.fetchCalls.length, 0);
  assert.equal(run.xhrCalls.length, 1);
  assert.equal(run.xhrCalls[0].url, "/track");
});

test("queues failed payloads and retries them on the next page load", async () => {
  const cookieJar = new Map();
  const localStorageData = new Map();
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const firstRun = await runTracker({
    cookieJar,
    localStorageData,
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z", async function (url) {
      if (url === "/track") {
        throw new Error("network down");
      }

      return createJsonResponse({}, true, 200);
    }),
    XMLHttpRequest: createXmlHttpRequest([], 500)
  });

  const queued = JSON.parse(firstRun.localStorageData.get("roas_radar_pending_track_events"));
  assert.equal(queued.length, 1);

  const secondRun = await runTracker({
    cookieJar,
    localStorageData: firstRun.localStorageData,
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z", async function (url) {
      if (url === "/track") {
        return { ok: true };
      }

      return createJsonResponse({}, true, 200);
    })
  });

  assert.equal(secondRun.fetchCalls.length, 3);
  assert.equal(secondRun.localStorageData.get("roas_radar_pending_track_events"), "[]");
});

test("falls back to a client-generated session when the bootstrap endpoint is unavailable", async () => {
  const run = await runTracker({
    fetch: async function () {
      throw new Error("bootstrap down");
    },
    sendBeacon: () => true
  });

  const payload = JSON.parse(run.beaconCalls[0].body);
  assert.match(payload.sessionId, /^[0-9a-f-]{36}$/i);
  assert.equal(payload.consentState, "unknown");
  assert.equal(run.localStorageData.get("_hba_id"), payload.sessionId);
  assert.match(run.localStorageData.get("roas_radar_session_created_at"), /^\d{4}-\d{2}-\d{2}T/);
});

test("preserves first-touch canonical attribution fields across later page views", async () => {
  const cookieJar = new Map();
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  const firstRun = await runTracker({
    cookieJar,
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z"),
    sendBeacon: () => true,
    location: {
      search: "?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123"
    }
  });

  const secondRun = await runTracker({
    cookieJar,
    localStorageData: firstRun.localStorageData,
    sessionStorageData: firstRun.sessionStorageData,
    fetch: createBootstrapFetch(sessionId, "2026-04-23T12:00:00.000Z"),
    sendBeacon: () => true,
    location: {
      pathname: "/collections/sale",
      search: ""
    }
  });

  const payload = JSON.parse(secondRun.beaconCalls[0].body);
  assert.equal(payload.landing_url, "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123");
  assert.equal(payload.page_url, "https://store.example.com/collections/sale");
  assert.equal(payload.utm_source, "google");
  assert.equal(payload.utm_medium, "cpc");
  assert.equal(payload.utm_campaign, "spring-sale");
  assert.equal(payload.gbraid, "GBRAID-123");
});
