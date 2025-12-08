from RectPacker.layout.element_registry import registry
from RectPacker.layout.rect_packer import Rect
from dominate.tags import style, link, meta
from dominate import document


def layout(rect, centered):
    if not rect.children:
        return

    # У всех детей задан parent = rect (UIElementRect делает это сам)

    Rect.pack_rects(rect, rect.children, centered=centered)

    for child in rect.children:
        layout(child, centered)


def render_with_children(rect):
    renderer = registry.get(rect.type_)
    if not renderer:
        raise ValueError(f"No renderer for type '{rect.type_}'")

    tag = renderer(rect, rect.props)

    for child in rect.children:
        tag.add(render_with_children(child))

    return tag


def generate_page(rects, screen_width, screen_height, centered: bool = False):
    root = Rect(screen_width, screen_height, margin_x=0, margin_y=0)

    top_level = []
    for r in rects:
        if r.parent is None:
            r.parent = root
            root.children.append(r)
            top_level.append(r)

    layout(root, centered=centered)

    doc = document(title="Generated Layout")

    with doc.head:
        link(rel="stylesheet", href="../frontend/web/static/style.css")
        meta(name="viewport", content="width=device-width, initial-scale=1.0")
        meta(charset="UTF-8")
        style("""
            html, body {
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100%;
                overflow: hidden;
                position: relative;
            }
        """)

    with doc:
        for child in root.children:
            doc.add(render_with_children(child))

    return doc.render()


def save_html(path, html_text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
