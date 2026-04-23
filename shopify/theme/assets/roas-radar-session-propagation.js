(function () {
  "use strict";

  var DEFAULTS = {
    cookieName: "roas_radar_session_id",
    trackingCookieName: "_hba_id",
    cartAttributeKey: "roas_radar_session_id",
    landingPathAttributeKey: "roas_radar_landing_path",
    sessionStorageKey: "roas_radar_session_id",
    trackingSessionStorageKey: "_hba_id",
    sessionBootstrapEndpoint: "/track/session",
    syncEndpoint: "/cart/update.js",
    syncStateStorageKey: "roas_radar_cart_attribute_sync_state",
    syncMaxAttempts: 4,
    syncRetryBaseDelayMs: 250,
    syncRetryMaxDelayMs: 4000,
    syncTimeoutMs: 5000,
    checkoutSelectors: [
      "form[action^='/cart'] [name='checkout']",
      "button[name='checkout']",
      "a[href^='/checkout']",
      "input[name='checkout']"
    ],
    cartFormSelectors: [
      "form[action^='/cart']",
      "form[action='/cart']"
    ],
    cartMutationPaths: ["/cart/add", "/cart/add.js", "/cart/change", "/cart/change.js", "/cart/update", "/cart/update.js", "/cart/clear", "/cart/clear.js"]
  };

  var config = assign({}, DEFAULTS, window.ROASRadarConfig || {});
  var syncInFlight = null;
  var sessionBootstrapPromise = null;
  var originalFetch = typeof window.fetch === "function" ? window.fetch.bind(window) : null;
  var originalXmlHttpRequest = window.XMLHttpRequest;

  function assign(target) {
    for (var i = 1; i < arguments.length; i += 1) {
      var source = arguments[i] || {};
      var keys = Object.keys(source);
      for (var j = 0; j < keys.length; j += 1) {
        target[keys[j]] = source[keys[j]];
      }
    }
    return target;
  }

  function readCookie(name) {
    var escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    var match = document.cookie.match(new RegExp("(?:^|; )" + escaped + "=([^;]*)"));
    return match ? decodeURIComponent(match[1]) : null;
  }

  function readStorage(key, storage) {
    try {
      return storage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function writeStorage(key, value, storage) {
    try {
      storage.setItem(key, value);
    } catch (error) {
      return;
    }
  }

  function safeParseJson(value, fallback) {
    if (!value) {
      return fallback;
    }

    try {
      return JSON.parse(value);
    } catch (error) {
      return fallback;
    }
  }

  function sanitizeSessionId(value) {
    if (!value || typeof value !== "string") {
      return null;
    }

    var trimmed = value.trim();
    return /^[0-9a-f-]{36}$/i.test(trimmed) ? trimmed : null;
  }

  function generateUuid() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }

    var bytes = [];
    for (var i = 0; i < 16; i += 1) {
      bytes.push(Math.floor(Math.random() * 256));
    }

    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;

    var hex = [];
    for (var j = 0; j < bytes.length; j += 1) {
      var value = bytes[j].toString(16);
      hex.push(value.length === 1 ? "0" + value : value);
    }

    return [
      hex.slice(0, 4).join(""),
      hex.slice(4, 6).join(""),
      hex.slice(6, 8).join(""),
      hex.slice(8, 10).join(""),
      hex.slice(10, 16).join("")
    ].join("-");
  }

  function resolveSessionId() {
    var candidates = [
      window.__ROAS_RADAR_TRACKING_SESSION_ID,
      window.__ROAS_RADAR_SESSION_ID,
      readCookie(config.trackingCookieName),
      readCookie(config.cookieName),
      readStorage(config.trackingSessionStorageKey, window.sessionStorage),
      readStorage(config.trackingSessionStorageKey, window.localStorage),
      readStorage(config.sessionStorageKey, window.sessionStorage),
      readStorage(config.sessionStorageKey, window.localStorage)
    ];

    for (var i = 0; i < candidates.length; i += 1) {
      var sessionId = sanitizeSessionId(candidates[i]);
      if (sessionId) {
        return sessionId;
      }
    }

    return null;
  }

  function persistSessionId(sessionId) {
    if (!sessionId) {
      return;
    }

    window.__ROAS_RADAR_SESSION_ID = sessionId;
    window.__ROAS_RADAR_TRACKING_SESSION_ID = sessionId;
    writeStorage(config.sessionStorageKey, sessionId, window.sessionStorage);
    writeStorage(config.sessionStorageKey, sessionId, window.localStorage);
    writeStorage(config.trackingSessionStorageKey, sessionId, window.sessionStorage);
    writeStorage(config.trackingSessionStorageKey, sessionId, window.localStorage);
  }

  function buildAttributeName(attributeKey) {
    return "attributes[" + attributeKey + "]";
  }

  function upsertHiddenInput(form, name, value) {
    if (!form || !name) {
      return;
    }

    var selector = "input[type='hidden'][name=\"" + cssEscape(name) + "\"]";
    var input = form.querySelector(selector);

    if (!input) {
      input = document.createElement("input");
      input.type = "hidden";
      input.name = name;
      form.appendChild(input);
    }

    input.value = value;
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === "function") {
      return window.CSS.escape(value);
    }

    return String(value).replace(/["\\]/g, "\\$&");
  }

  function getLandingPath() {
    return window.location.pathname + window.location.search;
  }

  function updateCartForms(sessionId) {
    var forms = document.querySelectorAll(config.cartFormSelectors.join(","));
    var sessionFieldName = buildAttributeName(config.cartAttributeKey);
    var landingPathFieldName = buildAttributeName(config.landingPathAttributeKey);
    var landingPath = getLandingPath();

    for (var i = 0; i < forms.length; i += 1) {
      upsertHiddenInput(forms[i], sessionFieldName, sessionId);
      upsertHiddenInput(forms[i], landingPathFieldName, landingPath);
    }
  }

  function buildPayload(sessionId) {
    var attributes = {};
    attributes[config.cartAttributeKey] = sessionId;
    attributes[config.landingPathAttributeKey] = getLandingPath();

    return JSON.stringify({ attributes: attributes });
  }

  function buildSyncFingerprint(sessionId) {
    return sessionId + "::" + getLandingPath();
  }

  function readSyncState() {
    return (
      safeParseJson(readStorage(config.syncStateStorageKey, window.localStorage), null) || {
        fingerprint: null,
        syncedAt: null,
        correlationId: null
      }
    );
  }

  function writeSyncState(state) {
    writeStorage(config.syncStateStorageKey, JSON.stringify(state), window.localStorage);
  }

  function isAlreadySynced(sessionId) {
    var state = readSyncState();
    return state.fingerprint === buildSyncFingerprint(sessionId);
  }

  function createCorrelationId() {
    return "cart-sync-" + generateUuid();
  }

  function logSync(level, eventName, fields) {
    var payload = assign(
      {
        component: "roas-radar-session-propagation",
        event: eventName
      },
      fields || {}
    );
    var logger = console && console[level] ? console[level] : console.log;

    if (typeof logger === "function") {
      logger("[ROAS Radar]", payload);
    }
  }

  function dispatchSyncEvent(eventName, detail) {
    if (typeof window.CustomEvent !== "function" || !document || typeof document.dispatchEvent !== "function") {
      return;
    }

    document.dispatchEvent(
      new window.CustomEvent(eventName, {
        detail: detail
      })
    );
  }

  function sleep(ms) {
    return new Promise(function (resolve) {
      window.setTimeout(resolve, ms);
    });
  }

  function computeRetryDelay(attempt) {
    var boundedAttempt = Math.max(0, attempt - 1);
    return Math.min(config.syncRetryBaseDelayMs * Math.pow(2, boundedAttempt), config.syncRetryMaxDelayMs);
  }

  function classifySyncError(response, error) {
    if (response) {
      if (response.status === 408 || response.status === 409 || response.status === 423 || response.status === 425 || response.status === 429) {
        return true;
      }

      if (response.status >= 500) {
        return true;
      }

      return false;
    }

    return !!error;
  }

  function parseUrlPath(value) {
    if (!value) {
      return "";
    }

    try {
      var base = window.location && window.location.origin ? window.location.origin : "https://example.invalid";
      return new URL(String(value), base).pathname;
    } catch (error) {
      return "";
    }
  }

  function isCartMutationPath(value) {
    var path = parseUrlPath(value);

    if (!path) {
      return false;
    }

    for (var i = 0; i < config.cartMutationPaths.length; i += 1) {
      if (path === config.cartMutationPaths[i]) {
        return true;
      }
    }

    return false;
  }

  function isSyncEndpoint(value) {
    return parseUrlPath(value) === parseUrlPath(config.syncEndpoint);
  }

  function shouldTriggerSyncForForm(form) {
    if (!form || !form.getAttribute) {
      return false;
    }

    return isCartMutationPath(form.getAttribute("action"));
  }

  function shouldTriggerSyncForRequest(url, options) {
    if (!isCartMutationPath(url) || isSyncEndpoint(url)) {
      return false;
    }

    var headers = options && options.headers;
    if (headers && typeof headers === "object") {
      if (typeof headers.get === "function" && headers.get("X-ROAS-Radar-Cart-Sync") === "1") {
        return false;
      }

      if (headers["X-ROAS-Radar-Cart-Sync"] === "1") {
        return false;
      }
    }

    return true;
  }

  function syncCartAttributesWithRetry(sessionId, reason) {
    if (!sessionId) {
      return Promise.resolve(false);
    }

    var fingerprint = buildSyncFingerprint(sessionId);
    if (isAlreadySynced(sessionId)) {
      return Promise.resolve(true);
    }

    if (syncInFlight && syncInFlight.fingerprint === fingerprint) {
      return syncInFlight.promise;
    }

    var correlationId = createCorrelationId();
    var attempt = 0;
    var promise = (function runAttempt() {
      attempt += 1;

      logSync("info", "cart_attribute_sync_attempt", {
        correlationId: correlationId,
        reason: reason,
        attempt: attempt,
        sessionId: sessionId
      });

      return originalFetch(config.syncEndpoint, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-ROAS-Radar-Correlation-Id": correlationId,
          "X-ROAS-Radar-Cart-Sync": "1"
        },
        body: buildPayload(sessionId)
      })
        .then(function (response) {
          if (!response.ok) {
            var isTransient = classifySyncError(response, null);
            if (isTransient && attempt < config.syncMaxAttempts) {
              var retryDelay = computeRetryDelay(attempt);

              logSync("warn", "cart_attribute_sync_retry_scheduled", {
                correlationId: correlationId,
                reason: reason,
                attempt: attempt,
                retryInMs: retryDelay,
                status: response.status,
                sessionId: sessionId
              });

              return sleep(retryDelay).then(runAttempt);
            }

            throw new Error("ROAS Radar cart sync failed with status " + response.status);
          }

          writeSyncState({
            fingerprint: fingerprint,
            syncedAt: new Date().toISOString(),
            correlationId: correlationId
          });

          logSync("info", "cart_attribute_sync_succeeded", {
            correlationId: correlationId,
            reason: reason,
            attempt: attempt,
            sessionId: sessionId
          });

          dispatchSyncEvent("roas-radar:session-synced", {
            sessionId: sessionId,
            cartAttributeKey: config.cartAttributeKey,
            correlationId: correlationId,
            reason: reason,
            attemptCount: attempt
          });

          return true;
        })
        .catch(function (error) {
          var isTransient = classifySyncError(null, error);
          if (isTransient && attempt < config.syncMaxAttempts) {
            var retryDelay = computeRetryDelay(attempt);

            logSync("warn", "cart_attribute_sync_retry_scheduled", {
              correlationId: correlationId,
              reason: reason,
              attempt: attempt,
              retryInMs: retryDelay,
              error: error && error.message ? error.message : String(error),
              sessionId: sessionId
            });

            return sleep(retryDelay).then(runAttempt);
          }

          logSync("warn", "cart_attribute_sync_failed", {
            correlationId: correlationId,
            reason: reason,
            attempt: attempt,
            error: error && error.message ? error.message : String(error),
            sessionId: sessionId
          });

          return false;
        });
    })();

    syncInFlight = {
      fingerprint: fingerprint,
      promise: promise.finally(function () {
        syncInFlight = null;
      })
    };

    return syncInFlight.promise;
  }

  function buildSessionBootstrapUrl() {
    if (!window.location || !config.sessionBootstrapEndpoint) {
      return null;
    }

    var endpoint = String(config.sessionBootstrapEndpoint);
    var separator = endpoint.indexOf("?") === -1 ? "?" : "&";
    var pageUrl = window.location.origin + window.location.pathname + window.location.search;
    var query = [
      "pageUrl=" + encodeURIComponent(pageUrl),
      "landingUrl=" + encodeURIComponent(pageUrl),
      "referrerUrl=" + encodeURIComponent(document.referrer || "")
    ].join("&");

    return endpoint + separator + query;
  }

  function bootstrapSessionId() {
    if (sessionBootstrapPromise) {
      return sessionBootstrapPromise;
    }

    if (!originalFetch) {
      return Promise.resolve(resolveSessionId());
    }

    var url = buildSessionBootstrapUrl();
    if (!url) {
      return Promise.resolve(resolveSessionId());
    }

    sessionBootstrapPromise = originalFetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: {
        Accept: "application/json"
      }
    })
      .then(function (response) {
        if (!response.ok || typeof response.json !== "function") {
          throw new Error("ROAS Radar session bootstrap failed with status " + response.status);
        }

        return response.json();
      })
      .then(function (payload) {
        var sessionId = sanitizeSessionId(payload && payload.sessionId);
        if (!sessionId) {
          return resolveSessionId();
        }

        persistSessionId(sessionId);
        return sessionId;
      })
      .catch(function () {
        return resolveSessionId();
      })
      .finally(function () {
        sessionBootstrapPromise = null;
      });

    return sessionBootstrapPromise;
  }

  function ensureCartSessionSync(sessionId, reason) {
    if (!sessionId) {
      return Promise.resolve(false);
    }

    persistSessionId(sessionId);
    updateCartForms(sessionId);
    return syncCartAttributesWithRetry(sessionId, reason || "unknown");
  }

  function syncFromFirstMutation(reason) {
    return bootstrapSessionId().then(function (sessionId) {
      return ensureCartSessionSync(sessionId, reason);
    });
  }

  function attachCheckoutListeners() {
    document.addEventListener(
      "click",
      function (event) {
        var target = event.target;
        if (!target || !target.closest) {
          return;
        }

        var checkoutTarget = target.closest(config.checkoutSelectors.join(","));
        if (!checkoutTarget) {
          return;
        }

        syncFromFirstMutation("checkout_click");
      },
      { passive: true }
    );

    document.addEventListener(
      "submit",
      function (event) {
        var form = event && event.target;
        var reason = shouldTriggerSyncForForm(form) ? "cart_form_submit" : "checkout_submit";
        syncFromFirstMutation(reason);
      },
      true
    );

    document.addEventListener("roas-radar:session-ready", function (event) {
      var detail = event && event.detail ? event.detail : null;
      ensureCartSessionSync(sanitizeSessionId(detail && detail.sessionId), "session_ready");
    });
  }

  function patchFetch() {
    if (!originalFetch) {
      return;
    }

    window.fetch = function (url, options) {
      if (shouldTriggerSyncForRequest(url, options)) {
        syncFromFirstMutation("cart_fetch_request");
      }

      return originalFetch(url, options);
    };
  }

  function patchXmlHttpRequest() {
    if (typeof originalXmlHttpRequest !== "function") {
      return;
    }

    function PatchedXmlHttpRequest() {
      var xhr = new originalXmlHttpRequest();
      var originalOpen = xhr.open;
      var originalSend = xhr.send;
      var requestUrl = null;
      var shouldSync = false;

      xhr.open = function (method, url) {
        requestUrl = url;
        shouldSync = isCartMutationPath(url) && !isSyncEndpoint(url);
        return originalOpen.apply(xhr, arguments);
      };

      xhr.send = function () {
        if (shouldSync) {
          syncFromFirstMutation("cart_xhr_request");
        }

        return originalSend.apply(xhr, arguments);
      };

      return xhr;
    }

    PatchedXmlHttpRequest.UNSENT = originalXmlHttpRequest.UNSENT;
    PatchedXmlHttpRequest.OPENED = originalXmlHttpRequest.OPENED;
    PatchedXmlHttpRequest.HEADERS_RECEIVED = originalXmlHttpRequest.HEADERS_RECEIVED;
    PatchedXmlHttpRequest.LOADING = originalXmlHttpRequest.LOADING;
    PatchedXmlHttpRequest.DONE = originalXmlHttpRequest.DONE;
    PatchedXmlHttpRequest.prototype = originalXmlHttpRequest.prototype;

    window.XMLHttpRequest = PatchedXmlHttpRequest;
  }

  function init() {
    attachCheckoutListeners();
    patchFetch();
    patchXmlHttpRequest();

    var sessionId = resolveSessionId();
    if (sessionId) {
      ensureCartSessionSync(sessionId, "init_existing_session");
      return;
    }

    bootstrapSessionId().then(function (bootstrappedSessionId) {
      ensureCartSessionSync(bootstrappedSessionId, "init_bootstrap");
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
