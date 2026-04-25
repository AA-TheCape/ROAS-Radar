(function () {
  "use strict";

  var DEFAULTS = {
    attributionSchemaVersion: 1,
    cookieName: "_hba_id",
    legacyCookieName: "roas_radar_session_id",
    storageKey: "_hba_id",
    legacyStorageKey: "roas_radar_session_id",
    sessionCreatedAtStorageKey: "roas_radar_session_created_at",
    attributionStorageKey: "roas_radar_attribution_capture_v1",
    queueStorageKey: "roas_radar_pending_track_events",
    endpoint: "/track",
    sessionBootstrapEndpoint: "/track/session",
    cookieDays: 365,
    cookiePath: "/",
    eventType: "page_view",
    maxQueueSize: 10
  };

  var config = assign({}, DEFAULTS, window.ROASRadarConfig || {});
  var hasTrackedPageLoad = false;
  var sessionBootstrapPromise = null;

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
    if (!name) {
      return null;
    }

    var escaped = String(name).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    var match = document.cookie.match(new RegExp("(?:^|; )" + escaped + "=([^;]*)"));
    return match ? decodeURIComponent(match[1]) : null;
  }

  function writeCookie(name, value, days, path) {
    if (!name || !value) {
      return;
    }

    var expiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000);
    var cookie = [
      name + "=" + encodeURIComponent(value),
      "Expires=" + expiresAt.toUTCString(),
      "Max-Age=" + String(days * 24 * 60 * 60),
      "Path=" + (path || "/"),
      "SameSite=Lax"
    ];

    if (window.location && window.location.protocol === "https:") {
      cookie.push("Secure");
    }

    document.cookie = cookie.join("; ");
  }

  function readStorage(key, storage) {
    if (!key || !storage || typeof storage.getItem !== "function") {
      return null;
    }

    try {
      return storage.getItem(key);
    } catch (error) {
      return null;
    }
  }

  function writeStorage(key, value, storage) {
    if (!key || !storage || typeof storage.setItem !== "function") {
      return;
    }

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

  function isUuid(value) {
    return (
      typeof value === "string" &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
    );
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
      readCookie(config.cookieName),
      readCookie(config.legacyCookieName),
      readStorage(config.storageKey, window.localStorage),
      readStorage(config.storageKey, window.sessionStorage),
      readStorage(config.legacyStorageKey, window.localStorage),
      readStorage(config.legacyStorageKey, window.sessionStorage)
    ];

    for (var i = 0; i < candidates.length; i += 1) {
      if (isUuid(candidates[i])) {
        return candidates[i];
      }
    }

    return null;
  }

  function persistSessionId(sessionId, createdAt) {
    if (!isUuid(sessionId)) {
      return null;
    }

    window.__ROAS_RADAR_TRACKING_SESSION_ID = sessionId;
    window.__ROAS_RADAR_SESSION_ID = sessionId;

    if (createdAt) {
      window.__ROAS_RADAR_SESSION_CREATED_AT = createdAt;
    }

    writeCookie(config.cookieName, sessionId, config.cookieDays, config.cookiePath);
    writeStorage(config.storageKey, sessionId, window.localStorage);
    writeStorage(config.storageKey, sessionId, window.sessionStorage);
    writeStorage(config.legacyStorageKey, sessionId, window.localStorage);
    writeStorage(config.legacyStorageKey, sessionId, window.sessionStorage);

    if (createdAt) {
      writeStorage(config.sessionCreatedAtStorageKey, createdAt, window.localStorage);
      writeStorage(config.sessionCreatedAtStorageKey, createdAt, window.sessionStorage);
    }

    return sessionId;
  }

  function getPersistedSessionCreatedAt() {
    var candidates = [
      window.__ROAS_RADAR_SESSION_CREATED_AT,
      readStorage(config.sessionCreatedAtStorageKey, window.localStorage),
      readStorage(config.sessionCreatedAtStorageKey, window.sessionStorage)
    ];

    for (var i = 0; i < candidates.length; i += 1) {
      if (typeof candidates[i] === "string" && candidates[i]) {
        return candidates[i];
      }
    }

    return null;
  }

  function dispatchSessionReady(sessionId, createdAt, source, attribution) {
    if (typeof window.CustomEvent !== "function" || !document || typeof document.dispatchEvent !== "function") {
      return;
    }

    document.dispatchEvent(
      new window.CustomEvent("roas-radar:session-ready", {
        detail: {
          sessionId: sessionId,
          createdAt: createdAt || null,
          source: source || "unknown",
          attribution: attribution || null
        }
      })
    );
  }

  function getQueue() {
    return safeParseJson(readStorage(config.queueStorageKey, window.localStorage), []);
  }

  function setQueue(queue) {
    writeStorage(config.queueStorageKey, JSON.stringify(queue), window.localStorage);
  }

  function enqueuePayload(payload) {
    var queue = getQueue();
    queue.push(payload);

    if (queue.length > config.maxQueueSize) {
      queue = queue.slice(queue.length - config.maxQueueSize);
    }

    setQueue(queue);
  }

  function buildPageUrl() {
    if (!window.location) {
      return "";
    }

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

  function buildAttributionCapture(sessionId, occurredAt, capturedAt) {
    var storedCapture = readStoredAttributionCapture() || {};
    var currentPageUrl = normalizeUrl(buildPageUrl());
    var currentReferrerUrl = normalizeUrl(document.referrer || "");
    var currentParameters = parseAttributionParameters(currentPageUrl);
    var storedLandingUrl = normalizeUrl(storedCapture.landing_url);
    var landingUrl = storedLandingUrl || currentPageUrl;
    var landingParameters = parseAttributionParameters(landingUrl);

    return {
      schema_version: config.attributionSchemaVersion,
      roas_radar_session_id: sessionId,
      occurred_at: occurredAt,
      captured_at: capturedAt,
      landing_url: landingUrl,
      referrer_url: normalizeUrl(storedCapture.referrer_url) || currentReferrerUrl,
      page_url: currentPageUrl,
      utm_source: storedCapture.utm_source || currentParameters.utm_source || landingParameters.utm_source,
      utm_medium: storedCapture.utm_medium || currentParameters.utm_medium || landingParameters.utm_medium,
      utm_campaign: storedCapture.utm_campaign || currentParameters.utm_campaign || landingParameters.utm_campaign,
      utm_content: storedCapture.utm_content || currentParameters.utm_content || landingParameters.utm_content,
      utm_term: storedCapture.utm_term || currentParameters.utm_term || landingParameters.utm_term,
      gclid: storedCapture.gclid || currentParameters.gclid || landingParameters.gclid,
      gbraid: storedCapture.gbraid || currentParameters.gbraid || landingParameters.gbraid,
      wbraid: storedCapture.wbraid || currentParameters.wbraid || landingParameters.wbraid,
      fbclid: storedCapture.fbclid || currentParameters.fbclid || landingParameters.fbclid,
      ttclid: storedCapture.ttclid || currentParameters.ttclid || landingParameters.ttclid,
      msclkid: storedCapture.msclkid || currentParameters.msclkid || landingParameters.msclkid
    };
  }

  function normalizeConsentState(value) {
    return value === "granted" || value === "denied" || value === "unknown" ? value : null;
  }

  function readConsentSignal(value) {
    if (typeof value === "function") {
      try {
        return readConsentSignal(value());
      } catch (error) {
        return null;
      }
    }

    return typeof value === "boolean" ? value : null;
  }

  function resolveConsentState() {
    var configured = normalizeConsentState(config.consentState);
    if (configured) {
      return configured;
    }

    var privacy = window.Shopify && window.Shopify.customerPrivacy;
    if (!privacy) {
      return "unknown";
    }

    var signals = [
      readConsentSignal(privacy.marketingAllowed),
      readConsentSignal(privacy.analyticsProcessingAllowed),
      readConsentSignal(privacy.userCanBeTracked)
    ];

    for (var index = 0; index < signals.length; index += 1) {
      if (signals[index] === false) {
        return "denied";
      }
    }

    for (var signalIndex = 0; signalIndex < signals.length; signalIndex += 1) {
      if (signals[signalIndex] === true) {
        return "granted";
      }
    }

    return "unknown";
  }

  function buildPayload(sessionId) {
    var occurredAt = new Date().toISOString();
    var capture = persistAttributionCapture(buildAttributionCapture(sessionId, occurredAt, occurredAt));

    return {
      eventType: config.eventType,
      occurredAt: occurredAt,
      sessionId: sessionId,
      pageUrl: capture.page_url,
      referrerUrl: capture.referrer_url,
      shopifyCartToken: null,
      shopifyCheckoutToken: null,
      clientEventId: generateUuid(),
      consentState: resolveConsentState(),
      schema_version: capture.schema_version,
      roas_radar_session_id: capture.roas_radar_session_id,
      occurred_at: capture.occurred_at,
      captured_at: capture.captured_at,
      landing_url: capture.landing_url,
      referrer_url: capture.referrer_url,
      page_url: capture.page_url,
      utm_source: capture.utm_source,
      utm_medium: capture.utm_medium,
      utm_campaign: capture.utm_campaign,
      utm_content: capture.utm_content,
      utm_term: capture.utm_term,
      gclid: capture.gclid,
      gbraid: capture.gbraid,
      wbraid: capture.wbraid,
      fbclid: capture.fbclid,
      ttclid: capture.ttclid,
      msclkid: capture.msclkid,
      context: {
        userAgent: (window.navigator && window.navigator.userAgent) || "",
        screen:
          window.screen && typeof window.screen.width === "number" && typeof window.screen.height === "number"
            ? String(window.screen.width) + "x" + String(window.screen.height)
            : undefined,
        language: (window.navigator && window.navigator.language) || undefined
      }
    };
  }

  function buildSessionBootstrapUrl() {
    if (!window.location || !config.sessionBootstrapEndpoint) {
      return null;
    }

    var endpoint = String(config.sessionBootstrapEndpoint);
    var separator = endpoint.indexOf("?") === -1 ? "?" : "&";
    var pageUrl = buildPageUrl();
    var query = [
      "pageUrl=" + encodeURIComponent(pageUrl),
      "landingUrl=" + encodeURIComponent(pageUrl),
      "referrerUrl=" + encodeURIComponent(document.referrer || "")
    ].join("&");

    return endpoint + separator + query;
  }

  function requestServerSession() {
    if (typeof window.fetch !== "function") {
      return Promise.resolve(null);
    }

    var url = buildSessionBootstrapUrl();
    if (!url) {
      return Promise.resolve(null);
    }

    return window
      .fetch(url, {
        method: "GET",
        credentials: "same-origin",
        headers: {
          Accept: "application/json"
        }
      })
      .then(function (response) {
        if (!response || !response.ok || typeof response.json !== "function") {
          return null;
        }

        return response.json().catch(function () {
          return null;
        });
      })
      .then(function (payload) {
        if (!payload || !isUuid(payload.sessionId)) {
          return null;
        }

        return {
          sessionId: payload.sessionId,
          createdAt: typeof payload.createdAt === "string" ? payload.createdAt : null,
          source: payload.isNewSession ? "server_created" : "server_reused"
        };
      })
      .catch(function () {
        return null;
      });
  }

  function ensureSessionIdentity() {
    if (sessionBootstrapPromise) {
      return sessionBootstrapPromise;
    }

    sessionBootstrapPromise = requestServerSession()
      .then(function (serverSession) {
        if (serverSession && isUuid(serverSession.sessionId)) {
          persistSessionId(serverSession.sessionId, serverSession.createdAt);
          dispatchSessionReady(
            serverSession.sessionId,
            serverSession.createdAt,
            serverSession.source,
            persistAttributionCapture(
              buildAttributionCapture(
                serverSession.sessionId,
                serverSession.createdAt || new Date().toISOString(),
                new Date().toISOString()
              )
            )
          );
          return serverSession;
        }

        var existingSessionId = resolveSessionId();
        if (existingSessionId) {
          var existingCreatedAt = getPersistedSessionCreatedAt();
          persistSessionId(existingSessionId, existingCreatedAt);
          dispatchSessionReady(
            existingSessionId,
            existingCreatedAt,
            "client_reused",
            persistAttributionCapture(
              buildAttributionCapture(
                existingSessionId,
                existingCreatedAt || new Date().toISOString(),
                new Date().toISOString()
              )
            )
          );
          return {
            sessionId: existingSessionId,
            createdAt: existingCreatedAt,
            source: "client_reused"
          };
        }

        var fallbackSessionId = generateUuid();
        var fallbackCreatedAt = new Date().toISOString();
        persistSessionId(fallbackSessionId, fallbackCreatedAt);
        dispatchSessionReady(
          fallbackSessionId,
          fallbackCreatedAt,
          "client_fallback",
          persistAttributionCapture(buildAttributionCapture(fallbackSessionId, fallbackCreatedAt, fallbackCreatedAt))
        );
        return {
          sessionId: fallbackSessionId,
          createdAt: fallbackCreatedAt,
          source: "client_fallback"
        };
      })
      .finally(function () {
        sessionBootstrapPromise = null;
      });

    return sessionBootstrapPromise;
  }

  function tryBeacon(body) {
    if (!window.navigator || typeof window.navigator.sendBeacon !== "function") {
      return false;
    }

    try {
      return window.navigator.sendBeacon(config.endpoint, body);
    } catch (error) {
      return false;
    }
  }

  function tryFetch(body) {
    if (typeof window.fetch !== "function") {
      return Promise.resolve(false);
    }

    return window
      .fetch(config.endpoint, {
        method: "POST",
        keepalive: true,
        credentials: "omit",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: body
      })
      .then(function (response) {
        return !!(response && response.ok);
      })
      .catch(function () {
        return false;
      });
  }

  function tryXmlHttpRequest(body) {
    if (typeof window.XMLHttpRequest !== "function") {
      return Promise.resolve(false);
    }

    return new Promise(function (resolve) {
      try {
        var request = new window.XMLHttpRequest();
        request.open("POST", config.endpoint, true);
        request.setRequestHeader("Content-Type", "application/json");
        request.setRequestHeader("Accept", "application/json");
        request.onreadystatechange = function () {
          if (request.readyState !== 4) {
            return;
          }

          resolve(request.status >= 200 && request.status < 300);
        };
        request.onerror = function () {
          resolve(false);
        };
        request.send(body);
      } catch (error) {
        resolve(false);
      }
    });
  }

  function dispatchTrackedEvent(payload, delivered) {
    if (typeof window.CustomEvent !== "function" || !document || typeof document.dispatchEvent !== "function") {
      return;
    }

    document.dispatchEvent(
      new window.CustomEvent("roas-radar:page-tracked", {
        detail: {
          payload: payload,
          delivered: delivered
        }
      })
    );
  }

  function deliverPayload(payload) {
    var body = JSON.stringify(payload);

    if (tryBeacon(body)) {
      dispatchTrackedEvent(payload, true);
      return Promise.resolve(true);
    }

    return tryFetch(body).then(function (fetchDelivered) {
      if (fetchDelivered) {
        dispatchTrackedEvent(payload, true);
        return true;
      }

      return tryXmlHttpRequest(body).then(function (xhrDelivered) {
        dispatchTrackedEvent(payload, xhrDelivered);
        return xhrDelivered;
      });
    });
  }

  function flushQueuedPayloads() {
    var queue = getQueue();
    if (!queue.length) {
      return Promise.resolve();
    }

    var remaining = [];
    var chain = Promise.resolve();

    for (var i = 0; i < queue.length; i += 1) {
      (function (payload) {
        chain = chain.then(function () {
          return deliverPayload(payload).then(function (delivered) {
            if (!delivered) {
              remaining.push(payload);
            }
          });
        });
      })(queue[i]);
    }

    return chain.then(function () {
      setQueue(remaining);
    });
  }

  function trackPageLoad() {
    if (hasTrackedPageLoad) {
      return;
    }

    hasTrackedPageLoad = true;

    ensureSessionIdentity()
      .then(function (session) {
        var payload = buildPayload(session.sessionId);

        return flushQueuedPayloads()
          .then(function () {
            return deliverPayload(payload);
          })
          .then(function (delivered) {
            if (!delivered) {
              enqueuePayload(payload);
            }
          });
      })
      .catch(function () {
        var fallbackSessionId = persistSessionId(resolveSessionId() || generateUuid(), new Date().toISOString());
        var payload = buildPayload(fallbackSessionId);
        enqueuePayload(payload);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", trackPageLoad);
  } else {
    trackPageLoad();
  }
})();
