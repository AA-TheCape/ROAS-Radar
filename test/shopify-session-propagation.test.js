import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const scriptPath = path.resolve(__dirname, "../shopify/theme/assets/roas-radar-session-propagation.js");
const scriptSource = await fs.readFile(scriptPath, "utf8");

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

function createForm(action) {
  const hiddenInputs = new Map();

  return {
    action,
    children: [],
    getAttribute(name) {
      if (name === "action") {
        return action;
      }

      return null;
    },
    querySelector(selector) {
      const nameMatch = selector.match(/name="([^"]+)"/);
      if (!nameMatch) {
        return null;
      }

      return hiddenInputs.get(nameMatch[1]) || null;
    },
    appendChild(node) {
      this.children.push(node);
      hiddenInputs.set(node.name, node);
    }
  };
}

function createDocument(cookieJar, forms, state) {
  const listeners = new Map();
  const dispatchedEvents = [];

  const document = {
    referrer: "https://www.google.com/search?q=roas",
    readyState: state || "complete",
    addEventListener(name, handler) {
      const existing = listeners.get(name) || [];
      existing.push(handler);
      listeners.set(name, existing);
    },
    dispatchEvent(event) {
      dispatchedEvents.push(event);
      return true;
    },
    querySelectorAll() {
      return forms || [];
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
    __emit(name, event) {
      const handlers = listeners.get(name) || [];
      for (const handler of handlers) {
        handler(event);
      }
    }
  };

  Object.defineProperty(document, "cookie", {
    get() {
      return Array.from(cookieJar.entries())
        .map(([key, value]) => key + "=" + encodeURIComponent(value))
        .join("; ");
    },
    set(value) {
      const firstSegment = String(value).split(";")[0];
      const separatorIndex = firstSegment.indexOf("=");
      const key = firstSegment.slice(0, separatorIndex);
      const cookieValue = decodeURIComponent(firstSegment.slice(separatorIndex + 1));
      cookieJar.set(key, cookieValue);
    }
  });

  return document;
}

function createConsoleCapture() {
  const entries = [];

  function record(level, args) {
    entries.push({
      level,
      args
    });
  }

  return {
    entries,
    console: {
      log(...args) {
        record("log", args);
      },
      info(...args) {
        record("info", args);
      },
      warn(...args) {
        record("warn", args);
      }
    }
  };
}

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function runPropagation(overrides = {}) {
  const cookieJar = overrides.cookieJar || new Map();
  const localStorage = createStorage(overrides.localStorageData);
  const sessionStorage = createStorage(overrides.sessionStorageData);
  const forms = overrides.forms || [];
  const document = createDocument(cookieJar, forms, overrides.documentReadyState);
  const consoleCapture = createConsoleCapture();
  const timeoutCalls = [];

  let timeoutId = 0;
  function fakeSetTimeout(fn, delay) {
    timeoutCalls.push(delay);
    timeoutId += 1;
    Promise.resolve().then(fn);
    return timeoutId;
  }

  function fakeClearTimeout() {
    return;
  }

  const fetchCalls = [];
  const fetchImpl =
    overrides.fetch ||
    (async function (url) {
      if (String(url).startsWith("/track/session")) {
        return createJsonResponse({
          sessionId: "123e4567-e89b-42d3-a456-426614174000"
        });
      }

      return createJsonResponse({}, true, 200);
    });

  function wrappedFetch(url, options) {
    fetchCalls.push({ url, options });
    return fetchImpl(url, options);
  }

  function MockXMLHttpRequest() {
    this.open = function () {
      return;
    };
    this.send = function () {
      return;
    };
  }

  const windowObject = {
    ROASRadarConfig: overrides.config || {},
    location: Object.assign(
      {
        origin: "https://store.example.com",
        pathname: "/products/widget",
        search: "?utm_source=google&utm_medium=cpc",
        protocol: "https:"
      },
      overrides.location || {}
    ),
    document,
    localStorage,
    sessionStorage,
    crypto: {
      randomUUID:
        overrides.randomUUID ||
        (() => "123e4567-e89b-42d3-a456-426614174000")
    },
    fetch: wrappedFetch,
    XMLHttpRequest: overrides.XMLHttpRequest || MockXMLHttpRequest,
    CustomEvent: function CustomEvent(type, init) {
      this.type = type;
      this.detail = init ? init.detail : undefined;
    },
    setTimeout: fakeSetTimeout,
    clearTimeout: fakeClearTimeout,
    Promise,
    Math,
    Date,
    JSON,
    URL,
    console: consoleCapture.console
  };

  windowObject.window = windowObject;
  windowObject.self = windowObject;

  const context = vm.createContext({
    window: windowObject,
    document,
    localStorage,
    sessionStorage,
    console: consoleCapture.console,
    setTimeout: fakeSetTimeout,
    clearTimeout: fakeClearTimeout,
    Promise,
    Math,
    Date,
    JSON,
    URL
  });

  vm.runInContext(scriptSource, context, { filename: scriptPath });

  if (document.readyState === "loading") {
    document.__emit("DOMContentLoaded");
  }

  await flushAsyncWork();

  return {
    cookieJar,
    localStorageData: localStorage.data,
    sessionStorageData: sessionStorage.data,
    document,
    fetchCalls,
    timeoutCalls,
    consoleEntries: consoleCapture.entries,
    windowObject
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

test("writes cart attributes when the first cart mutation request is observed", async () => {
  const calls = [];
  let bootstrapAttempts = 0;
  const form = createForm("/cart");

  const run = await runPropagation({
    forms: [form],
    documentReadyState: "loading",
    fetch: async function (url, options) {
      calls.push({ url, options });

      if (String(url).startsWith("/track/session")) {
        bootstrapAttempts += 1;
        if (bootstrapAttempts === 1) {
          throw new Error("bootstrap unavailable during init");
        }

        return createJsonResponse({
          sessionId: "123e4567-e89b-42d3-a456-426614174111"
        });
      }

      if (url === "/cart/add.js") {
        return createJsonResponse({}, true, 200);
      }

      if (url === "/cart/update.js") {
        return createJsonResponse({}, true, 200);
      }

      return createJsonResponse({}, true, 200);
    }
  });

  await run.windowObject.fetch("/cart/add.js", { method: "POST" });
  await flushAsyncWork();

  const syncCall = calls.find((call) => call.url === "/cart/update.js");
  assert.ok(syncCall, "expected cart attribute sync to run after first cart mutation");
  assert.equal(syncCall.options.headers["X-ROAS-Radar-Cart-Sync"], "1");
  assert.equal(run.localStorageData.get("roas_radar_session_id"), "123e4567-e89b-42d3-a456-426614174111");
  const body = JSON.parse(syncCall.options.body);
  assert.deepEqual(body.attributes, {
    schema_version: "1",
    roas_radar_session_id: "123e4567-e89b-42d3-a456-426614174111",
    landing_url: "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc",
    referrer_url: "https://www.google.com/search?q=roas",
    page_url: "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc",
    utm_source: "google",
    utm_medium: "cpc"
  });
  assert.equal(form.querySelector('input[type="hidden"][name="attributes[schema_version]"]').value, "1");
  assert.equal(
    form.querySelector('input[type="hidden"][name="attributes[roas_radar_session_id]"]').value,
    "123e4567-e89b-42d3-a456-426614174111"
  );
});

test("retries transient Shopify failures with backoff and stable correlation IDs", async () => {
  const correlationIds = [];
  let syncAttempts = 0;

  const run = await runPropagation({
    fetch: async function (url, options) {
      if (String(url).startsWith("/track/session")) {
        return createJsonResponse({
          sessionId: "123e4567-e89b-42d3-a456-426614174222"
        });
      }

      if (url === "/cart/update.js") {
        syncAttempts += 1;
        correlationIds.push(options.headers["X-ROAS-Radar-Correlation-Id"]);

        if (syncAttempts < 3) {
          return createJsonResponse({}, false, 503);
        }

        return createJsonResponse({}, true, 200);
      }

      return createJsonResponse({}, true, 200);
    }
  });

  const syncCalls = run.fetchCalls.filter((call) => call.url === "/cart/update.js");
  assert.equal(syncCalls.length, 3);
  assert.deepEqual(run.timeoutCalls.slice(0, 2), [250, 500]);
  assert.equal(new Set(correlationIds).size, 1);

  const retryLogs = run.consoleEntries.filter((entry) => {
    return entry.level === "warn" && entry.args[1] && entry.args[1].event === "cart_attribute_sync_retry_scheduled";
  });
  assert.equal(retryLogs.length, 2);
  assert.equal(retryLogs[0].args[1].correlationId, correlationIds[0]);
  assert.equal(retryLogs[1].args[1].correlationId, correlationIds[0]);
});

test("avoids duplicate cart sync writes for an already-synced session fingerprint", async () => {
  const syncCalls = [];
  const syncedSessionId = "123e4567-e89b-42d3-a456-426614174333";
  const syncedFingerprint = JSON.stringify({
    schema_version: "1",
    roas_radar_session_id: syncedSessionId,
    landing_url: "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc",
    referrer_url: "https://www.google.com/search?q=roas",
    page_url: "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc",
    utm_source: "google",
    utm_medium: "cpc"
  });

  const run = await runPropagation({
    localStorageData: new Map([
      [
        "roas_radar_cart_attribute_sync_state",
        JSON.stringify({
          fingerprint: syncedFingerprint,
          syncedAt: "2026-04-23T12:00:00.000Z",
          correlationId: "cart-sync-existing"
        })
      ]
    ]),
    fetch: async function (url, options) {
      if (String(url).startsWith("/track/session")) {
        return createJsonResponse({
          sessionId: syncedSessionId
        });
      }

      if (url === "/cart/update.js") {
        syncCalls.push({ url, options });
        return createJsonResponse({}, true, 200);
      }

      return createJsonResponse({}, true, 200);
    }
  });

  run.document.__emit("roas-radar:session-ready", {
    detail: {
      sessionId: syncedSessionId
    }
  });
  await flushAsyncWork();

  assert.equal(syncCalls.length, 0);
});

test("reuses stored canonical attribution fields when later pages lose campaign params", async () => {
  const sessionId = "123e4567-e89b-42d3-a456-426614174444";
  const run = await runPropagation({
    localStorageData: new Map([
      [
        "roas_radar_attribution_capture_v1",
        JSON.stringify({
          schema_version: 1,
          roas_radar_session_id: sessionId,
          landing_url:
            "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
          referrer_url: "https://www.google.com/search?q=roas",
          page_url:
            "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
          utm_source: "google",
          utm_medium: "cpc",
          utm_campaign: "spring-sale",
          gbraid: "GBRAID-123"
        })
      ]
    ]),
    location: {
      pathname: "/collections/sale",
      search: ""
    },
    fetch: async function (url, options) {
      if (String(url).startsWith("/track/session")) {
        return createJsonResponse({
          sessionId: sessionId
        });
      }

      if (url === "/cart/update.js") {
        return createJsonResponse({}, true, 200);
      }

      return createJsonResponse({}, true, 200);
    }
  });

  const syncCall = run.fetchCalls.find((call) => call.url === "/cart/update.js");
  const body = JSON.parse(syncCall.options.body);
  assert.equal(
    body.attributes.landing_url,
    "https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123"
  );
  assert.equal(body.attributes.page_url, "https://store.example.com/collections/sale");
  assert.equal(body.attributes.utm_campaign, "spring-sale");
  assert.equal(body.attributes.gbraid, "GBRAID-123");
});
