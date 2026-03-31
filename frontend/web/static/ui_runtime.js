(function () {
    const Layout = {
        NONE: "none",
        VERTICAL: "vertical",
        HORIZONTAL: "horizontal",
        CENTER: "center",
        GRID: "grid"
    };

    const Align = {
        START: "start",
        CENTER: "center",
        END: "end",
        STRETCH: "stretch"
    };

    const Justify = {
        START: "start",
        CENTER: "center",
        END: "end",
        SPACE_BETWEEN: "space-between",
        SPACE_AROUND: "space-around"
    };

    function toKebabCase(s) {
        return String(s).replace(/[A-Z]/g, m => "-" + m.toLowerCase());
    }

    function applyRuntimeAttrs(node, el) {
        if (node.bind) el.dataset.bind = node.bind;
        if (node.action) el.dataset.action = node.action;
        if (node.endpoint) el.dataset.endpoint = node.endpoint;
    }

    function applyLayout(node, el) {
        const layout = node.layout || Layout.NONE;

        if (layout === Layout.NONE) return;

        if (layout === Layout.GRID) {
            el.style.display = "grid";
            if (node.gap != null) el.style.gap = node.gap + "px";
            return;
        }

        if (layout === Layout.CENTER) {
            el.style.display = "flex";
            el.style.flexDirection = "column";
            el.style.alignItems = "center";
            el.style.justifyContent = "center";
            if (node.gap != null) el.style.gap = node.gap + "px";
            return;
        }

        el.style.display = "flex";
        el.style.flexDirection = layout === Layout.HORIZONTAL ? "row" : "column";

        if (node.gap != null) el.style.gap = node.gap + "px";

        if (node.align) {
            el.style.alignItems = {
                [Align.START]: "flex-start",
                [Align.CENTER]: "center",
                [Align.END]: "flex-end",
                [Align.STRETCH]: "stretch"
            }[node.align] || "";
        }

        if (node.justify) {
            el.style.justifyContent = {
                [Justify.START]: "flex-start",
                [Justify.CENTER]: "center",
                [Justify.END]: "flex-end",
                [Justify.SPACE_BETWEEN]: "space-between",
                [Justify.SPACE_AROUND]: "space-around"
            }[node.justify] || "";
        }
    }

    function applySizing(node, el) {
        if (node.width != null) el.style.width = node.width + "px";
        if (node.height != null) el.style.height = node.height + "px";

        if (node.max_width != null) el.style.maxWidth = node.max_width + "px";
        if (node.min_width != null) el.style.minWidth = node.min_width + "px";

        if (node.grow != null) el.style.flexGrow = String(node.grow);
        if (node.shrink != null) el.style.flexShrink = String(node.shrink);
        if (node.basis != null) el.style.flexBasis = String(node.basis);
    }

    function applyProps(node, el, defaultClass) {
        const props = node.props || {};

        const extraClass = props.class;
        if (defaultClass && extraClass) el.className = defaultClass + " " + extraClass;
        else if (defaultClass) el.className = defaultClass;
        else if (extraClass) el.className = extraClass;

        if (props.style && typeof props.style === "object") {
            for (const [k, v] of Object.entries(props.style)) {
                el.style[toKebabCase(k)] = String(v);
            }
        }

        for (const [k, v] of Object.entries(props)) {
            if (k === "style" || k === "class") continue;

            if (k === "text") continue;
            if (k === "options") continue;

            if (v === true) {
                el.setAttribute(toKebabCase(k), "");
                continue;
            }

            if (v === false || v == null) continue;

            el.setAttribute(toKebabCase(k), String(v));
        }
    }

    function renderDropdown(node) {
        const el = document.createElement("select");
        const options = (node.props && node.props.options) ? node.props.options : [];
        for (const opt of options) {
            const o = document.createElement("option");
            o.textContent = String(opt);
            el.appendChild(o);
        }
        return el;
    }

    function renderNode(node) {
        if (!node || typeof node !== "object") return null;

        const type = String(node.type || "").toLowerCase();
        let el = null;

        switch (type) {
            case "container":
                el = document.createElement("div");
                applyLayout(node, el);
                break;

            case "text_input":
                el = document.createElement("input");
                el.type = "text";
                break;

            case "checkbox":
                el = document.createElement("input");
                el.type = "checkbox";
                break;

            case "radiobutton":
                el = document.createElement("input");
                el.type = "radio";
                break;

            case "dropdown":
                el = renderDropdown(node);
                break;

            case "button":
                el = document.createElement("button");
                el.type = "button";
                el.textContent = (node.props && node.props.text) ? String(node.props.text) : "Submit";
                break;

            case "h1":
                el = document.createElement("h1");
                el.textContent = (node.props && node.props.text) ? String(node.props.text) : "";
                break;

            case "h2":
                el = document.createElement("h2");
                el.textContent = (node.props && node.props.text) ? String(node.props.text) : "";
                break;

            case "h3":
                el = document.createElement("h3");
                el.textContent = (node.props && node.props.text) ? String(node.props.text) : "";
                break;

            case "a":
                el = document.createElement("a");
                el.textContent = (node.props && node.props.text) ? String(node.props.text) : "";
                break;

            case "img_input":
                el = document.createElement("input");
                el.type = "file";
                el.accept = "image/*";
                break;

            case "img_output":
                el = document.createElement("img");
                break;

            default:
                console.warn("[UIRenderer] Unknown node type:", type, node);
                return null;
        }

        applySizing(node, el);
        applyProps(node, el, type);
        applyRuntimeAttrs(node, el);

        if (node.children && Array.isArray(node.children) && node.children.length > 0) {
            for (const child of node.children) {
                const childEl = renderNode(child);
                if (childEl) el.appendChild(childEl);
            }
        }

        return el;
    }

    window.__UIJSON__ = {
        renderNode
    };
})();
