(function () {
    const STATE = {
        lastAction: null,
        lastResult: null,
        lastError: null,
        redirectTo: null,
        loading: false,
    };

    const ERROR_MESSAGES = {
        400: "Неверные данные. Пожалуйста, проверьте форму",
        401: "Неверный логин или пароль",
        403: "Доступ запрещён",
        404: "Сервис не найден",
        422: "Неверный формат данных",
        500: "Внутренняя ошибка сервера",
        "default": "Произошла неизвестная ошибка"
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

        if (tag === "select") return el.value;
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

    function extractErrorMessage(data, fallback) {
        if (!data) return fallback;

        if (typeof data === "string" && data.trim()) return data;

        if (typeof data === "object") {
            if (typeof data.error === "string" && data.error.trim()) return data.error;
            if (typeof data.message === "string" && data.message.trim()) return data.message;

            if (typeof data.detail === "string" && data.detail.trim()) return data.detail;

            if (Array.isArray(data.detail)) {
                const msgs = data.detail
                    .map(x => (x && typeof x.msg === "string" ? x.msg : null))
                    .filter(Boolean);

                if (msgs.length) return msgs.join(", ");
            }
        }

        return fallback;
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
            } else if (encoding === "form") {
                const formBody = new URLSearchParams();
                for (const [k, v] of Object.entries(payload)) if (v !== undefined) formBody.append(k, v);
                options.headers["Content-Type"] = "application/x-www-form-urlencoded";
                options.body = formBody.toString();
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
            const err = new Error();
            err.status = res.status;
            err.data = data;
            throw err;
        }

        return data;
    }


    function applyState(endpointKey, result) {
        STATE.lastAction = endpointKey;
        STATE.lastResult = result && result.data !== undefined ? result.data : result;
        STATE.lastError = null;

        const action = window.ACTIONS[endpointKey];
        STATE.redirectTo = (action && action.redirectOnSuccess) ? action.redirectOnSuccess : "self";
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
        for (const el of loadingEls) setVisible(el, STATE.loading);
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
            el.textContent = value === undefined || value === null ? "" : String(value);
        }
    }

    function renderValueBindings() {
        const els = document.querySelectorAll(`[data-value]`);
        for (const el of els) {
            const path = el.getAttribute("data-value");
            const value = getByPath(STATE, path);

            const tag = (el.tagName || "").toLowerCase();
            const type = (el.getAttribute("type") || "").toLowerCase();

            if (tag === "input" && type === "checkbox") el.checked = !!value;
            else if ("value" in el) el.value = value === undefined || value === null ? "" : String(value);
        }
    }

    function renderButtonsDisabled() {
        const buttons = document.querySelectorAll("button[data-endpoint]");
        for (const btn of buttons) btn.disabled = STATE.loading;
    }

    function renderRedirect() {
        if (!STATE.redirectTo) return;

        if (STATE.redirectTo === "self") {
            window.location.href = window.location.pathname;
            return;
        }

        window.location.href = STATE.redirectTo;
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
        try {
            const action = window.ACTIONS[endpointKey];
            if (!action) throw new Error(`Unknown endpoint '${endpointKey}'`);

            const payload = collectPayload(action);
            const emptyFields = Object.entries(payload).filter(([k, v]) => v === undefined || v === "");
            if (emptyFields.length) {
                applyError(endpointKey, `Пожалуйста, заполните: ${emptyFields.map(f => f[0]).join(", ")}`);
                render();
                return;
            }

            const result = await callEndpoint(endpointKey);

            if ((result && result.code >= 200 && result.code < 300) || (action && action.redirectOnSuccess)) {
                applyState(endpointKey, result);
            }
        } catch (e) {
            const code = e.status || "default";
            const msg = ERROR_MESSAGES[code] || ERROR_MESSAGES["default"];
            applyError(endpointKey, msg);
        }

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

    window.__UIRUNTIME__ = { STATE, runAction };
})();
