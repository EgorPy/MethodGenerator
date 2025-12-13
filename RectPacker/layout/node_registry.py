from dominate import tags

registry = {}


def register(name):
    """ Decorator registry for HTML element generator. """

    def decorator(func):
        registry[name] = func
        return func

    return decorator


def apply_class_and_style(node, default_class):
    """
    Returns a dict with _class and style for dominate
    Uses node.props['style'] for customization
    """
    style = node.props.pop("style", {}) if node.props else {}
    return {
        "_class": default_class,
        "style": "; ".join(f"{k}:{v}" for k, v in style.items())
    }


@register("text_input")
def render_text_input(node, props):
    attrs = apply_class_and_style(node, "text-input")
    return tags.input_(_type="text", **(props or {}), **attrs)


@register("checkbox")
def render_checkbox(node, props):
    attrs = apply_class_and_style(node, "checkbox")
    return tags.input_(_type="checkbox", **(props or {}), **attrs)


@register("radiobutton")
def render_radiobutton(node, props):
    attrs = apply_class_and_style(node, "radiobutton")
    return tags.input_(_type="radio", **(props or {}), **attrs)


@register("dropdown")
def render_dropdown(node, props):
    attrs = apply_class_and_style(node, "dropdown")
    s = tags.select(**(props or {}), **attrs)
    for opt in (props or {}).get("options", []):
        s.add(tags.option(opt))
    return s


@register("button")
def render_button(node, props):
    attrs = apply_class_and_style(node, "button")
    text = (props or {}).get("text", "Submit")
    return tags.button(text, **attrs)


@register("h1")
def render_h1(node, props):
    attrs = apply_class_and_style(node, "h1")
    return tags.h1((props or {}).get("text", ""), **attrs)


@register("h3")
def render_h3(node, props):
    attrs = apply_class_and_style(node, "h3")
    return tags.h3((props or {}).get("text", ""), **attrs)


@register("img_input")
def render_img_input(node, props):
    attrs = apply_class_and_style(node, "img-input")
    return tags.input_(_type="file", accept="image/*", **(props or {}), **attrs)


@register("img_output")
def render_img_output(node, props):
    attrs = apply_class_and_style(node, "img-output")
    return tags.img(src=(props or {}).get("src", ""), **attrs)


@register("container")
def render_container(node, props):
    attrs = apply_class_and_style(node, "container")
    return tags.div(**attrs)
