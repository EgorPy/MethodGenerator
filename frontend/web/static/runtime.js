(function () {
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
        if (!window.ACTIONS) {
            throw new Error("window.ACTIONS is not defined");
        }

        const action = window.ACTIONS[endpointKey];
        if (!action) {
            throw new Error(`Unknown endpoint '${endpointKey}'`);
        }

        const method = (action.method || "GET").toUpperCase();
        const url = action.url;
        const encoding = action.encoding || "json";

        const payload = collectPayload(action);

        const options = {
            method,
            headers: {},
            credentials: "include",
        };

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

        if (!res.ok) {
            throw new Error(typeof data === "string" ? data : JSON.stringify(data));
        }

        return data;
    }

    function bindButtons() {
        const buttons = document.querySelectorAll("button[data-endpoint]");
        for (const btn of buttons) {
            btn.addEventListener("click", async () => {
                const endpoint = btn.getAttribute("data-endpoint");
                if (!endpoint) return;

                try {
                    const result = await callEndpoint(endpoint);
                    console.log("API result:", result);
                } catch (e) {
                    console.error("API error:", e);
                }
            });
        }
    }

    document.addEventListener("DOMContentLoaded", bindButtons);
})();
