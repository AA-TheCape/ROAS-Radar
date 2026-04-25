(function () {
  "use strict";

  var DEFAULTS = {
    attributionSchemaVersion: 1,
    cookieName: "roas_radar_session_id",
    trackingCookieName: "_hba_id",
    cartAttributeKey: "roas_radar_session_id",
    sessionStorageKey: "roas_radar_session_id",
    trackingSessionStorageKey: "_hba_id",
    attributionStorageKey: "roas_radar_attribution_capture_v1",
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

  function normalizeString(value) {
    if (typeof value !== "string") {
      return null;
    }

    var trimmed = value.trim();
    return trimmed ? trimmed : null;
  }

  function normalizeLowercaseString(value) {
    var normalized = normalizeString(value);
    return normalized ? normalized.toLowerCase() : null;
  }

  function normalizeUrl(value, baseUrl) {
    var normalized = normalizeString(value);
    if (!normalized) {
      return null;
    }

    try {
      var url = baseUrl ? new URL(normalized, baseUrl) : new URL(normalized);
      if (url.protocol !== "http:" && url.protocol !== "https:") {
        return null;
      }

      url.hash = "";
      return url.toString();
    } catch (error) {
      return null;
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

  function getPageUrl() {
    return window.location.origin + window.location.pathname + window.location.search;
  }

  function parseAttributionParameters(urlValue) {
    var pageUrl = normalizeUrl(urlValue);
    var empty = {
      utm_source: null,
      utm_medium: null,
      utm_campaign: null,
      utm_content: null,
      utm_term: null,
      gclid: null,
      gbraid: null,
      wbraid: null,
      fbclid: null,
      ttclid: null,
      msclkid: null
    };

    if (!pageUrl) {
      return empty;
    }

    try {
      var url = new URL(pageUrl);
      return {
        utm_source: normalizeLowercaseString(url.searchParams.get("utm_source")),
        utm_medium: normalizeLowercaseString(url.searchParams.get("utm_medium")),
        utm_campaign: normalizeLowercaseString(url.searchParams.get("utm_campaign")),
        utm_content: normalizeLowercaseString(url.searchParams.get("utm_content")),
        utm_term: normalizeLowercaseString(url.searchParams.get("utm_term")),
        gclid: normalizeString(url.searchParams.get("gclid")),
        gbraid: normalizeString(url.searchParams.get("gbraid")),
        wbraid: normalizeString(url.searchParams.get("wbraid")),
        fbclid: normalizeString(url.searchParams.get("fbclid")),
        ttclid: normalizeString(url.searchParams.get("ttclid")),
        msclkid: normalizeString(url.searchParams.get("msclkid"))
      };
    } catch (error) {
      return empty;
    }
  }

  function readStoredAttributionCapture() {
    var storedCapture =
      window.__ROAS_RADAR_ATTRIBUTION_CAPTURE_V1 ||
      safeParseJson(readStorage(config.attributionStorageKey, window.localStorage), null) ||
      safeParseJson(readStorage(config.attributionStorageKey, window.sessionStorage), null);

    return storedCapture && typeof storedCapture === "object" ? storedCapture : null;
  }

  function persistAttributionCapture(capture) {
    if (!capture || !capture.roas_radar_session_id) {
      return null;
    }

    var serialized = JSON.stringify(capture);
    window.__ROAS_RADAR_ATTRIBUTION_CAPTURE_V1 = capture;
    writeStorage(config.attributionStorageKey, serialized, window.localStorage);
    writeStorage(config.attributionStorageKey, serialized, window.sessionStorage);
    return capture;
  }

  function buildAttributionCapture(sessionId, attribution) {
    var storedCapture = readStoredAttributionCapture() || {};
    var currentPageUrl = normalizeUrl(getPageUrl());
    var currentReferrerUrl = normalizeUrl(document.referrer || "");
    var currentParameters = parseAttributionParameters(currentPageUrl);
    var incomingCapture = attribution && typeof attribution === "object" ? attribution : {};
    var storedLandingUrl = normalizeUrl(incomingCapture.landing_url) || normalizeUrl(storedCapture.landing_url);
    var landingUrl = storedLandingUrl || currentPageUrl;
    var landingParameters = parseAttributionParameters(landingUrl);

    return {
      schema_version: config.attributionSchemaVersion,
      roas_radar_session_id: sessionId,
      landing_url: landingUrl,
      referrer_url:
        normalizeUrl(incomingCapture.referrer_url) || normalizeUrl(storedCapture.referrer_url) || currentReferrerUrl,
      page_url: currentPageUrl,
      utm_source:
        normalizeLowercaseString(incomingCapture.utm_source) ||
        normalizeLowercaseString(storedCapture.utm_source) ||
        currentParameters.utm_source ||
        landingParameters.utm_source,
      utm_medium:
        normalizeLowercaseString(incomingCapture.utm_medium) ||
        normalizeLowercaseString(storedCapture.utm_medium) ||
        currentParameters.utm_medium ||
        landingParameters.utm_medium,
      utm_campaign:
        normalizeLowercaseString(incomingCapture.utm_campaign) ||
        normalizeLowercaseString(storedCapture.utm_campaign) ||
        currentParameters.utm_campaign ||
        landingParameters.utm_campaign,
      utm_content:
        normalizeLowercaseString(incomingCapture.utm_content) ||
        normalizeLowercaseString(storedCapture.utm_content) ||
        currentParameters.utm_content ||
        landingParameters.utm_content,
      utm_term:
        normalizeLowercaseString(incomingCapture.utm_term) ||
        normalizeLowercaseString(storedCapture.utm_term) ||
        currentParameters.utm_term ||
        landingParameters.utm_term,
      gclid: normalizeString(incomingCapture.gclid) || normalizeString(storedCapture.gclid) || currentParameters.gclid || landingParameters.gclid,
      gbraid: normalizeString(incomingCapture.gbraid) || normalizeString(storedCapture.gbraid) || currentParameters.gbraid || landingParameters.gbraid,
      wbraid: normalizeString(incomingCapture.wbraid) || normalizeString(storedCapture.wbraid) || currentParameters.wbraid || landingParameters.wbraid,
      fbclid: normalizeString(incomingCapture.fbclid) || normalizeString(storedCapture.fbclid) || currentParameters.fbclid || landingParameters.fbclid,
      ttclid: normalizeString(incomingCapture.ttclid) || normalizeString(storedCapture.ttclid) || currentParameters.ttclid || landingParameters.ttclid,
      msclkid: normalizeString(incomingCapture.msclkid) || normalizeString(storedCapture.msclkid) || currentParameters.msclkid || landingParameters.msclkid
    };
  }

  function buildCartAttributes(sessionId, attribution) {
    var capture = persistAttributionCapture(buildAttributionCapture(sessionId, attribution));
    var attributes = {
      schema_version: String(capture.schema_version),
      roas_radar_session_id: capture.roas_radar_session_id
    };
    var keys = [
      "landing_url",
      "referrer_url",
      "page_url",
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_content",
      "utm_term",
      "gclid",
      "gbraid",
      "wbraid",
      "fbclid",
      "ttclid",
      "msclkid"
    ];

    for (var index = 0; index < keys.length; index += 1) {
      var key = keys[index];
      if (capture[key]) {
        attributes[key] = capture[key];
      }
    }

    return attributes;
  }

  function updateCartForms(sessionId, attribution) {
    var forms = document.querySelectorAll(config.cartFormSelectors.join(","));
    var attributes = buildCartAttributes(sessionId, attribution);
    var attributeKeys = Object.keys(attributes);

    for (var i = 0; i < forms.length; i += 1) {
      for (var attributeIndex = 0; attributeIndex < attributeKeys.length; attributeIndex += 1) {
        var attributeKey = attributeKeys[attributeIndex];
        upsertHiddenInput(forms[i], buildAttributeName(attributeKey), attributes[attributeKey]);
      }
    }
  }

  function buildPayload(sessionId, attribution) {
    var attributes = buildCartAttributes(sessionId, attribution);
    return JSON.stringify({ attributes: attributes });
  }

  function buildSyncFingerprint(sessionId, attribution) {
    return JSON.stringify(buildCartAttributes(sessionId, attribution));
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

  function isAlreadySynced(sessionId, attribution) {
    var state = readSyncState();
    return state.fingerprint === buildSyncFingerprint(sessionId, attribution);
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

  function syncCartAttributesWithRetry(sessionId, reason, attribution) {
    if (!sessionId) {
      return Promise.resolve(false);
    }

    var fingerprint = buildSyncFingerprint(sessionId, attribution);
    if (isAlreadySynced(sessionId, attribution)) {
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
        body: buildPayload(sessionId, attribution)
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
        if (payload && payload.attribution && typeof payload.attribution === "object") {
          persistAttributionCapture(buildAttributionCapture(sessionId, payload.attribution));
        }
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

  function ensureCartSessionSync(sessionId, reason, attribution) {
    if (!sessionId) {
      return Promise.resolve(false);
    }

    persistSessionId(sessionId);
    updateCartForms(sessionId, attribution);
    return syncCartAttributesWithRetry(sessionId, reason || "unknown", attribution);
  }

  function syncFromFirstMutation(reason) {
    return bootstrapSessionId().then(function (sessionId) {
      return ensureCartSessionSync(sessionId, reason, null);
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
      ensureCartSessionSync(
        sanitizeSessionId(detail && detail.sessionId),
        "session_ready",
        detail && detail.attribution ? detail.attribution : null
      );
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
