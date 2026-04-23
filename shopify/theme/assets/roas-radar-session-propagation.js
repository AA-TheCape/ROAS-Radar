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
    checkoutSelectors: [
      "form[action^='/cart'] [name='checkout']",
      "button[name='checkout']",
      "a[href^='/checkout']",
      "input[name='checkout']"
    ],
    cartFormSelectors: [
      "form[action^='/cart']",
      "form[action='/cart']"
    ]
  };

  var config = assign({}, DEFAULTS, window.ROASRadarConfig || {});
  var syncInFlight = null;
  var lastSyncedSessionId = null;
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

  function sanitizeSessionId(value) {
    if (!value || typeof value !== "string") {
      return null;
    }

    var trimmed = value.trim();
    return /^[0-9a-f-]{36}$/i.test(trimmed) ? trimmed : null;
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

  function updateCartForms(sessionId) {
    var forms = document.querySelectorAll(config.cartFormSelectors.join(","));
    var sessionFieldName = buildAttributeName(config.cartAttributeKey);
    var landingPathFieldName = buildAttributeName(config.landingPathAttributeKey);
    var landingPath = window.location.pathname + window.location.search;

    for (var i = 0; i < forms.length; i += 1) {
      upsertHiddenInput(forms[i], sessionFieldName, sessionId);
      upsertHiddenInput(forms[i], landingPathFieldName, landingPath);
    }
  }

  function buildPayload(sessionId) {
    var attributes = {};
    attributes[config.cartAttributeKey] = sessionId;
    attributes[config.landingPathAttributeKey] = window.location.pathname + window.location.search;

    return JSON.stringify({ attributes: attributes });
  }

  function syncCartAttributes(sessionId) {
    if (!sessionId) {
      return Promise.resolve(false);
    }

    if (lastSyncedSessionId === sessionId) {
      return Promise.resolve(true);
    }

    if (syncInFlight) {
      return syncInFlight;
    }

    syncInFlight = fetch(config.syncEndpoint, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: buildPayload(sessionId)
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("ROAS Radar cart sync failed with status " + response.status);
        }

        lastSyncedSessionId = sessionId;
        document.dispatchEvent(
          new CustomEvent("roas-radar:session-synced", {
            detail: {
              sessionId: sessionId,
              cartAttributeKey: config.cartAttributeKey
            }
          })
        );
        return true;
      })
      .catch(function (error) {
        console.warn(error);
        return false;
      })
      .finally(function () {
        syncInFlight = null;
      });

    return syncInFlight;
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

    if (typeof window.fetch !== "function") {
      return Promise.resolve(resolveSessionId());
    }

    var url = buildSessionBootstrapUrl();
    if (!url) {
      return Promise.resolve(resolveSessionId());
    }

    sessionBootstrapPromise = window
      .fetch(url, {
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

  function ensureCartSessionSync(sessionId) {
    if (!sessionId) {
      return;
    }

    persistSessionId(sessionId);
    updateCartForms(sessionId);
    syncCartAttributes(sessionId);
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

        bootstrapSessionId().then(ensureCartSessionSync);
      },
      { passive: true }
    );

    document.addEventListener(
      "submit",
      function () {
        bootstrapSessionId().then(ensureCartSessionSync);
      },
      true
    );

    document.addEventListener("roas-radar:session-ready", function (event) {
      var detail = event && event.detail ? event.detail : null;
      ensureCartSessionSync(sanitizeSessionId(detail && detail.sessionId));
    });
  }

  function init() {
    attachCheckoutListeners();

    var sessionId = resolveSessionId();
    if (sessionId) {
      ensureCartSessionSync(sessionId);
      return;
    }

    bootstrapSessionId().then(ensureCartSessionSync);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
