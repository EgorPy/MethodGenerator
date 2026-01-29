(function () {
    const STORAGE_KEY = "APP_STATE";

    function loadState() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            if (!raw) {
                return {
                    form: {},
                    auth: {
                        isAuthenticated: false,
                        token: null,
                        user: null
                    },
                    api: {
                        lastAction: null,
                        lastStatus: null,
                        lastOk: null,
                        lastError: null,
                        lastResponse: null
                    }
                };
            }
            return JSON.parse(raw);
        } catch {
            return {
                form: {},
                auth: {
                    isAuthenticated: false,
                    token: null,
                    user: null
                },
                api: {
                    lastAction: null,
                    lastStatus: null,
                    lastOk: null,
                    lastError: null,
                    lastResponse: null
                }
            };
        }
    }

    function saveState(state) {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    }

    function getMeta(name) {
        const el = document.querySelector(`meta[name="${name}"]`);
        return el ? el.getAttribute("content") : null;
    }

    function redirectTo(path) {
        if (!path) return;
        if (window.location.pathname === path) return;
        window.location.href = path;
    }

    function applyRouteGuard(state) {
        const mode = getMeta("route:auth");
        const redirect = getMeta("route:redirect");

        if (!mode) return;

        if (mode === "guest" && state.auth.isAuthenticated) {
            redirectTo(redirect || "/profile");
        }

        if (mode === "auth" && !state.auth.isAuthenticated) {
            redirectTo(redirect || "/login");
        }
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

    async function callEndpoint(endpointKey, state) {
        if (!window.ACTIONS) {
            throw new Error("window.ACTIONS is not defined");
        }

        const action = window.ACTIONS[endpointKey];
        if (!action) {
            throw new Error(`Unknown endpoint '${endpointKey}'`);
        }

        state.api.lastAction = endpointKey;
        state.api.lastStatus = "loading";
        state.api.lastOk = null;
        state.api.lastError = null;
        state.api.lastResponse = null;
        saveState(state);

        const method = (action.method || "GET").toUpperCase();
        const url = action.url;
        const encoding = action.encoding || "json";

        const payload = collectPayload(action);

        const options = {
            method,
            headers: {},
            credentials: "include"
        };

        if (state.auth.token) {
            options.headers["Authorization"] = `Bearer ${state.auth.token}`;
        }

        if (method !== "GET") {
            if (encoding === "json") {
                options.headers["Content-Type"] = "application/json";
                options.body = JSON.stringify(payload);
            } else {
                const form = new FormData();
                for (const [k, v] of Object.entries(payload)) {
                    if (v !== undefined) form.append(k, v);
                }
                options.body = form;
            }
        }

        const res = await fetch(url, options);

        const contentType = res.headers.get("content-type") || "";
        const isJson = contentType.includes("application/json");

        const data = isJson ? await res.json() : await res.text();

        state.api.lastResponse = data;

        if (!res.ok) {
            state.api.lastStatus = "error";
            state.api.lastOk = false;
            state.api.lastError = typeof data === "string" ? data : JSON.stringify(data);
            saveState(state);
            throw new Error(state.api.lastError);
        }

        state.api.lastStatus = "success";
        state.api.lastOk = true;
        state.api.lastError = null;

        if (endpointKey === "auth.login") {
            state.auth.isAuthenticated = true;
            state.auth.token = data?.token || null;
        }

        if (endpointKey === "auth.logout") {
            state.auth.isAuthenticated = false;
            state.auth.token = null;
            state.auth.user = null;
        }

        if (endpointKey === "auth.get_me") {
            state.auth.user = data;
        }

        saveState(state);

        applyRouteGuard(state);

        return data;
    }

    function bindButtons(state) {
        const buttons = document.querySelectorAll("button[data-endpoint]");
        for (const btn of buttons) {
            btn.addEventListener("click", async (e) => {
                e.preventDefault();

                const endpoint = btn.getAttribute("data-endpoint");
                if (!endpoint) return;

                try {
                    const result = await callEndpoint(endpoint, state);
                    console.log("API result:", result);
                } catch (e) {
                    console.error("API error:", e);
                }
            });
        }
    }

    function init() {
        const state = loadState();
        window.STATE = state;

        applyRouteGuard(state);
        bindButtons(state);
    }

    document.addEventListener("DOMContentLoaded", init);
})();
