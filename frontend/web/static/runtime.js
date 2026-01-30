(function () {
    const STATE = {
        lastAction: null,
        lastResult: null,
        lastError: null,
        redirectTo: null,
    };

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

    async function callEndpoint(endpointKey) {
        if (!window.ACTIONS) throw new Error("window.ACTIONS is not defined");
        const action = window.ACTIONS[endpointKey];
        if (!action) throw new Error(`Unknown endpoint '${endpointKey}'`);

        const method = (action.method || "GET").toUpperCase();
        const url = action.url;
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
            let message = "Unknown error";
            if (isJson && data && typeof data.error === "string") message = data.error;
            else if (typeof data === "string") message = "404 not found";
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
        STATE.lastError = String(error);
        STATE.redirectTo = null;
    }

    function render() {
        const errBox = document.querySelector(`[data-ui="error"]`);
        if (errBox) {
            if (STATE.lastError) {
                errBox.style.display = "block";
                errBox.textContent = STATE.lastError;
            } else {
                errBox.style.display = "none";
                errBox.textContent = "";
            }
        }

        if (STATE.redirectTo) {
            window.location.href = STATE.redirectTo;
        }
    }

    async function runAction(endpointKey) {
        try {
            const result = await callEndpoint(endpointKey);
            applyState(endpointKey, result);
        } catch (e) {
            applyError(endpointKey, e);
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

    document.addEventListener("DOMContentLoaded", bindButtons);

    window.__UIRUNTIME__ = {
        STATE,
        runAction,
    };
})();
