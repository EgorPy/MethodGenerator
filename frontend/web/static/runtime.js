(function () {
    const STATE = {
        lastAction: null,
        lastResult: null,
        lastError: null,
        redirectTo: null,
        loading: false,
    };

    function buildUrl(actionUrl) {
        const base = String(window.BACKEND_URL || "").replace(/\/$/, "");
        const path = String(actionUrl || "");

        if (!base) return path;
        if (path.startsWith("http://") || path.startsWith("https://")) return path;
        if (!path.startsWith("/")) return base + "/" + path;
        return base + path;
    }

    function getValueByBind(bind) {
        const el = document.querySelector(`[data-bind="${bind}"]`);
        if (!el) return undefined;

        const tag = (el.tagName || "").toLowerCase();
        const type = (el.getAttribute("type") || "").toLowerCase();

        if (tag === "input") {
            if (type === "checkbox") return el.checked;
            return el.value;
        }

        if (tag === "select") {
            return el.value;
        }

        return el.value;
    }

    function collectPayload(actionConfig) {
        const payload = {};
        const fields = actionConfig.payload || [];

        for (const name of fields) {
            payload[name] = getValueByBind(name);
        }

        return payload;
    }

    function getByPath(obj, path) {
        if (!path) return undefined;
        const parts = String(path).split(".");
        let cur = obj;

        for (const p of parts) {
            if (cur == null) return undefined;
            cur = cur[p];
        }

        return cur;
    }

    async function callEndpoint(endpointKey) {
        if (!window.ACTIONS) throw new Error("window.ACTIONS is not defined");
        const action = window.ACTIONS[endpointKey];
        if (!action) throw new Error(`Unknown endpoint '${endpointKey}'`);

        const method = (action.method || "GET").toUpperCase();
        const url = buildUrl(action.url);
        const encoding = action.encoding || "json";
        const payload = collectPayload(action);

        const options = { method, headers: {}, credentials: "include" };

        if (method !== "GET") {
            if (encoding === "json") {
                options.headers["Content-Type"] = "application/json";
                options.body = JSON.stringify(payload);
            } else {
                const form = new FormData();
                for (const [k, v] of Object.entries(payload)) if (v !== undefined) form.append(k, v);
                options.body = form;
            }
        }

        const res = await fetch(url, options);
        const contentType = res.headers.get("content-type") || "";
        const isJson = contentType.includes("application/json");
        let data;

        try {
            data = isJson ? await res.json() : await res.text();
        } catch {
            data = null;
        }

        if (!res.ok) {
            let message = "Request failed";

            if (res.status === 404) message = "Not found (404)";
            else if (res.status === 401) message = "Unauthorized (401)";
            else if (res.status === 403) message = "Forbidden (403)";
            else if (res.status >= 500) message = "Server error. Try again later.";

            if (isJson && data && typeof data.error === "string") {
                message = data.error;
            }

            throw new Error(message);
        }

        return data;
    }

    function applyRedirectRules(endpointKey, result) {
        if (endpointKey === "auth.login") return "/profile";
        if (endpointKey === "auth.register") return "/profile";
        if (endpointKey === "auth.logout") return "/";

        if (result && typeof result === "object" && typeof result.redirect === "string") {
            return result.redirect;
        }

        return null;
    }

    function applyState(endpointKey, result) {
        STATE.lastAction = endpointKey;
        STATE.lastResult = result;
        STATE.lastError = null;

        const redirect = applyRedirectRules(endpointKey, result);
        STATE.redirectTo = redirect;
    }

    function applyError(endpointKey, error) {
        STATE.lastAction = endpointKey;
        STATE.lastResult = null;
        STATE.lastError = String(error && error.message ? error.message : error);
        STATE.redirectTo = null;
    }

    function setVisible(el, visible) {
        el.style.display = visible ? "" : "none";
    }

    function renderErrorBox() {
        const errBox = document.querySelector(`[data-ui="error"]`);
        if (!errBox) return;

        if (STATE.lastError) {
            errBox.textContent = STATE.lastError;
            setVisible(errBox, true);
        } else {
            errBox.textContent = "";
            setVisible(errBox, false);
        }
    }

    function renderLoading() {
        const loadingEls = document.querySelectorAll(`[data-ui="loading"], [data-show="loading"]`);
        for (const el of loadingEls) {
            setVisible(el, STATE.loading);
        }
    }

    function renderShowRules() {
        const els = document.querySelectorAll(`[data-show]`);
        for (const el of els) {
            const rule = el.getAttribute("data-show");

            if (rule === "error") setVisible(el, !!STATE.lastError);
            else if (rule === "success") setVisible(el, !!STATE.lastResult && !STATE.lastError);
            else if (rule === "loading") setVisible(el, !!STATE.loading);
            else if (rule === "result") setVisible(el, !!STATE.lastResult);
            else setVisible(el, false);
        }
    }

    function renderTextBindings() {
        const els = document.querySelectorAll(`[data-text]`);
        for (const el of els) {
            const path = el.getAttribute("data-text");
            const value = getByPath(STATE, path);

            if (value === undefined || value === null) el.textContent = "";
            else el.textContent = String(value);
        }
    }

    function renderValueBindings() {
        const els = document.querySelectorAll(`[data-value]`);
        for (const el of els) {
            const path = el.getAttribute("data-value");
            const value = getByPath(STATE, path);

            const tag = (el.tagName || "").toLowerCase();
            const type = (el.getAttribute("type") || "").toLowerCase();

            if (tag === "input" && type === "checkbox") {
                el.checked = !!value;
            } else if ("value" in el) {
                el.value = value === undefined || value === null ? "" : String(value);
            }
        }
    }

    function renderButtonsDisabled() {
        const buttons = document.querySelectorAll("button[data-endpoint]");
        for (const btn of buttons) {
            btn.disabled = STATE.loading;
        }
    }

    function renderRedirect() {
        if (STATE.redirectTo) {
            window.location.href = STATE.redirectTo;
        }
    }

    function render() {
        renderLoading();
        renderButtonsDisabled();
        renderErrorBox();
        renderShowRules();
        renderTextBindings();
        renderValueBindings();
        renderRedirect();
    }

    async function runAction(endpointKey) {
        STATE.loading = true;
        render();

        try {
            const result = await callEndpoint(endpointKey);
            applyState(endpointKey, result);
        } catch (e) {
            applyError(endpointKey, e);
        }

        STATE.loading = false;
        render();
    }

    function bindButtons() {
        const buttons = document.querySelectorAll("button[data-endpoint]");
        for (const btn of buttons) {
            btn.addEventListener("click", async () => {
                const endpoint = btn.getAttribute("data-endpoint");
                if (!endpoint) return;
                await runAction(endpoint);
            });
        }
    }

    document.addEventListener("DOMContentLoaded", () => {
        bindButtons();
        render();
    });

    window.__UIRUNTIME__ = {
        STATE,
        runAction,
    };
})();
