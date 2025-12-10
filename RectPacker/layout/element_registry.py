from RectPacker.layout.rect_packer import Rect
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


def local_xy(rect: Rect):
    parent = rect.parent
    if parent is None:
        return rect.x, rect.y

    # Если parent.centered_x включен, child.x уже глобальный → вычитаем parent.x
    if getattr(parent, "center_x", False):
        return rect.x - parent.x, rect.y

    # Если center_x выключен, child.x уже локальный
    return rect.x, rect.y


@register("text_input")
def render_text_input(rect, props):
    return tags.input_(_type="text", **make_style(rect))


@register("checkbox")
def render_checkbox(rect, props):
    return tags.input_(_type="checkbox", **make_style(rect))


@register("radiobutton")
def render_radiobutton(rect, props):
    return tags.input_(_type="radio", **make_style(rect))


@register("dropdown")
def render_dropdown(rect, props):
    s = tags.select(**make_style(rect))
    for opt in props.get("options", []):
        s.add(tags.option(opt))
    return s


@register("h1")
def render_h1(rect, props):
    return tags.h1(props.get("text", ""), **make_style(rect))


@register("h3")
def render_h3(rect, props):
    return tags.h3(props.get("text", ""), **make_style(rect))


@register("img_input")
def render_img_input(rect, props):
    return tags.input_(_type="file", accept="image/*", **make_style(rect))


@register("img_output")
def render_img_output(rect, props):
    return tags.img(src=props.get("src", ""), **make_style(rect))


@register("container")
def render_container(rect, props):
    return tags.div(**make_style(rect))


def make_style(rect: Rect):
    x, y = local_xy(rect)

    return {
        "style": (
            f"position:absolute;"
            f"left:{x}px;"
            f"top:{y}px;"
            f"width:{rect.width}px;"
            f"height:{rect.height}px;"
            f"box-sizing:border-box;"
        )
    }
