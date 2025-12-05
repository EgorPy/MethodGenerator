from RectPacker.layout.element_registry import registry
from RectPacker.layout.rect_packer import Rect
from dominate.tags import style
from dominate import document


def generate_page(rects, screen_width, screen_height):
    parent = Rect(screen_width, screen_height)

    for r in rects:
        r.parent = parent

    Rect.pack_rects(parent, rects)

    doc = document(title="Generated Layout")

    with doc.head:
        style("""
            body {
                margin: 0;
                padding: 0;
                position: relative;
                width: 100vw;
                height: 100vh;
            }
        """)

    with doc:
        for rect in rects:
            renderer = registry.get(rect.type_)
            if not renderer:
                raise ValueError(f"No renderer for type '{rect.type_}'")
            doc.add(renderer(rect, rect.props))

    return doc.render()


def save_html(path, html_text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
