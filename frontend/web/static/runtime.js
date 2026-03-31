(function () {
    const STATE = {
        lastAction: null,
        lastResult: {},
        lastError: null,
        redirectTo: null,
        loading: false,
        errorTimeoutId: null,
    };

    const ERROR_MESSAGES = {
        400: "Wrong data. Please, check form",
        401: "Wrong login or password",
        403: "Forbidden",
        404: "Service not found",
        422: "Wrong data format",
        500: "Service unavailable",
        "default": "Unknown error occured"
    };

    function buildUrl(action) {
        const base = String(window.BACKEND_URL || "").replace(/\/$/, "");
        const url = String(action.url || "");
        if (!base) return url;
        if (url.startsWith("http://") || url.startsWith("https://")) return url;
        return base + (url.startsWith("/") ? url : "/" + url);
    }

    async function callEndpoint(endpointKey, payloadOverride) {
        if (!window.ACTIONS) throw new Error("window.ACTIONS is not defined");
        const action = window.ACTIONS[endpointKey];
        if (!action) throw new Error(`Unknown endpoint '${endpointKey}'`);

        const method = (action.method || "GET").toUpperCase();
        const url = buildUrl(action);

        let bodyData = {};
        if (payloadOverride) {
            bodyData = payloadOverride;
        } else if (method !== "GET") {
            bodyData = action.payload || {};
        }

        const res = await fetch(url, {
            method,
            headers: {"Content-Type": "application/json"},
            credentials: "include",
            body: method !== "GET" ? JSON.stringify(bodyData) : undefined
        });

        if (!res.ok) {
            const err = new Error();
            err.status = res.status;
            throw err;
        }

        return await res.json();
    }

    function applyError(endpointKey, error) {
        STATE.lastAction = endpointKey;
        STATE.lastError = String(error && error.message ? error.message : error);
        setTimeout(() => {
            STATE.lastError = null;
            render();
        }, 3500);
    }

    function render() {
        document.querySelectorAll(`[data-text]`).forEach(el => {
            const path = el.getAttribute("data-text");
            el.textContent = STATE[path] ?? "";
        });
    }

    window.__UIRenderer__ = {
        renderNode(node) {
            if (!node) return null;
            let el;

            switch (node.type) {
                case "container":
                    el = document.createElement("div");
                    el.style.display = "flex";
                    el.style.flexDirection = node.layout === "horizontal" ? "row" : "column";
                    if (node.gap) el.style.gap = node.gap + "px";
                    if (node.children) {
                        node.children.forEach(child => {
                            const childEl = this.renderNode(child);
                            if (childEl) el.appendChild(childEl);
                        });
                    }
                    break;

                case "h1":
                    el = document.createElement("h1");
                    el.textContent = node.props?.text || "";
                    break;

                case "h2":
                    el = document.createElement("h2");
                    el.textContent = node.props?.text || "";
                    break;

                case "text_input":
                    el = document.createElement("input");
                    el.type = "text";
                    el.placeholder = node.props?.placeholder || "";
                    if (node.bind) el.dataset.bind = node.bind;
                    break;

                case "button":
                    el = document.createElement("button");
                    el.textContent = node.props?.text || "Button";
                    if (node.endpoint) {
                        el.addEventListener("click", async () => {
                            // если node.payload есть — используем его
                            let payload = node.payload ? {...node.payload} : {};

                            // если payload пустой, собираем данные из input с data-bind
                            if (!payload || Object.keys(payload).length === 0) {
                                document.querySelectorAll("[data-bind]").forEach(input => {
                                    payload[input.dataset.bind] = input.value;
                                });
                            }

                            try {
                                await window.__UIRUNTIME__.runAction(node.endpoint, payload);
                                await window.__UIRUNTIME__.runContainers();
                            } catch (e) {
                                const code = e.status || "default";
                                const msg = ERROR_MESSAGES[code] || ERROR_MESSAGES["default"];
                                applyError(node.endpoint, msg);
                            }
                        });
                    }
                    break;

                default:
                    console.warn("Unknown node type:", node.type);
                    return null;
            }

            return el;
        }
    };

    async function renderContainer(container) {
        const endpoint = container.getAttribute("data-endpoint");
        if (!endpoint) return;

        try {
            const data = await callEndpoint(endpoint);
            container.dataset.loaded = "true";
            STATE.lastResult[endpoint] = data;

            container.innerHTML = "";

            const nodes = Array.isArray(data) ? data : (data.children || []);
            nodes.forEach(child => {
                const html = window.__UIRenderer__.renderNode(child);
                if (html) container.appendChild(html);
            });
        } catch (e) {
            const code = e.status || "default";
            const msg = ERROR_MESSAGES[code] || ERROR_MESSAGES["default"];
            applyError(endpoint, msg);
        }
    }

    async function runContainers() {
        const containers = document.querySelectorAll("div[data-endpoint]");
        for (const container of containers) await renderContainer(container);
    }

    async function runAction(endpoint, payload) {
        return await callEndpoint(endpoint, payload);
    }

    function bindExistingButtons() {
        document.querySelectorAll("button[data-endpoint]").forEach(btn => {
            if (btn.dataset.bound) return; // чтобы не привязывать дважды
            btn.dataset.bound = "true";

            btn.addEventListener("click", async () => {
                const endpoint = btn.getAttribute("data-endpoint");
                let payload = {};
                document.querySelectorAll("[data-bind]").forEach(input => {
                    payload[input.dataset.bind] = input.value;
                });

                try {
                    await window.__UIRUNTIME__.runAction(endpoint, payload);
                    await window.__UIRUNTIME__.runContainers();
                } catch (e) {
                    const code = e.status || "default";
                    const msg = ERROR_MESSAGES[code] || ERROR_MESSAGES["default"];
                    applyError(endpoint, msg);
                }
            });
        });
    }

    document.addEventListener("DOMContentLoaded", async () => {
        await runContainers();
        bindExistingButtons();
        render();
    });

    window.__UIRUNTIME__ = {STATE, runContainers, runAction};
})();
