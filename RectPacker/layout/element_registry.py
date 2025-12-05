import dominate.tags as tags

registry = {}


def register(name):
    """
    Decorator registry for HTML element generator.
    """

    def decorator(func):
        registry[name] = func
        return func

    return decorator


@register("text_input")
def render_text_input(el):
    return tags.input_(_type="text", placeholder=el.get("placeholder", ""), **make_style(el))


@register("checkbox")
def render_checkbox(el):
    return tags.input_(_type="checkbox", **make_style(el))


@register("radiobutton")
def render_radiobutton(el):
    return tags.input_(_type="radio", **make_style(el))


@register("dropdown")
def render_dropdown(el):
    s = tags.select(**make_style(el))
    for opt in el.get("options", []):
        s.add(tags.option(opt))
    return s


@register("text_output")
def render_text_output(el):
    return tags.div(el.get("text", ""), **make_style(el))


@register("img_input")
def render_img_input(el):
    return tags.input_(_type="file", accept="image/*", **make_style(el))


@register("img_output")
def render_img_output(el):
    return tags.img(src=el.get("src", ""), alt=el.get("alt", ""), **make_style(el))


def make_style(el):
    """
    Generates inline-style for DOM element.
    """

    style = []

    if "pwidth" in el: style.append(f"width:{el['pwidth']}%;")
    if "min_width" in el: style.append(f"min-width:{el['min_width']}px;")
    if "max_width" in el: style.append(f"max-width:{el['max_width']}px;")

    if "pheight" in el: style.append(f"height:{el['pheight']}%;")
    if "min_height" in el: style.append(f"min-height:{el['min_height']}px;")
    if "max_height" in el: style.append(f"max-height:{el['max_height']}px;")

    if style:
        return {"style": " ".join(style)}
    return {}
