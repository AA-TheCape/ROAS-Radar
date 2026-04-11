(function () {
  "use strict";

  var DEFAULTS = {
    cookieName: "_hba_id",
    legacyCookieName: "roas_radar_session_id",
    storageKey: "_hba_id",
    legacyStorageKey: "roas_radar_session_id",
    queueStorageKey: "roas_radar_pending_track_events",
    endpoint: "/track",
    cookieDays: 365,
    cookiePath: "/",
    eventType: "page_view",
    maxQueueSize: 10
  };

  var config = assign({}, DEFAULTS, window.ROASRadarConfig || {});
  var hasTrackedPageLoad = false;

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

    return generateUuid();
  }

  function persistSessionId(sessionId) {
    if (!isUuid(sessionId)) {
      return null;
    }

    window.__ROAS_RADAR_TRACKING_SESSION_ID = sessionId;
    window.__ROAS_RADAR_SESSION_ID = sessionId;

    writeCookie(config.cookieName, sessionId, config.cookieDays, config.cookiePath);
    writeStorage(config.storageKey, sessionId, window.localStorage);
    writeStorage(config.storageKey, sessionId, window.sessionStorage);
    writeStorage(config.legacyStorageKey, sessionId, window.localStorage);
    writeStorage(config.legacyStorageKey, sessionId, window.sessionStorage);

    return sessionId;
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

  function buildPayload(sessionId) {
    return {
      eventType: config.eventType,
      occurredAt: new Date().toISOString(),
      sessionId: sessionId,
      pageUrl: buildPageUrl(),
      referrerUrl: document.referrer || null,
      shopifyCartToken: null,
      shopifyCheckoutToken: null,
      clientEventId: generateUuid(),
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
          "Accept": "application/json"
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

    var sessionId = persistSessionId(resolveSessionId());
    var payload = buildPayload(sessionId);

    flushQueuedPayloads()
      .then(function () {
        return deliverPayload(payload);
      })
      .then(function (delivered) {
        if (!delivered) {
          enqueuePayload(payload);
        }
      })
      .catch(function () {
        enqueuePayload(payload);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", trackPageLoad);
  } else {
    trackPageLoad();
  }
})();
