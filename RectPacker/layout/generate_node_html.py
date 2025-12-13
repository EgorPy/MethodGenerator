from RectPacker.layout.node_registry import registry
from RectPacker.layout.ui_node import UINode

from dominate.tags import style, link, meta
from dominate import document
import os

# TODO: Add logging
# TODO: All directories move to configs

CSS_DIR = "../../frontend/web/static"  # directory with CSS modules


def render_with_children(node: UINode):
    """
    Renders UINode with children nodes.
    """

    renderer = registry.get(node.type_)
    if not renderer:
        raise ValueError(f"No renderer for type '{node.type_}'")

    tag = renderer(node, node.props)

    for child in node.children:
        tag.add(render_with_children(child))

    return tag


def generate_page_from_ui_tree(nodes: list[UINode], title: str = "Generated UI"):
    """
    HTML page generation from a list of root UINode's.
    Automatically includes CSS files for element types if they exist.
    """

    doc = document(title=title)

    available_css_files = set(f for f in os.listdir(CSS_DIR) if f.endswith(".css"))
    included_css = set()

    def scan_node_for_css(node: UINode):
        css_name = f"{node.type_}.css"
        if css_name in available_css_files:
            included_css.add(css_name)
        for child in node.children:
            scan_node_for_css(child)

    for node in nodes:
        scan_node_for_css(node)

    with doc.head:
        for f in included_css:
            link(rel="stylesheet", href=os.path.join(CSS_DIR, f))

        link(rel="stylesheet", href=os.path.join(CSS_DIR, "style.css"))
        meta(name="viewport", content="width=device-width, initial-scale=1.0")
        meta(charset="UTF-8")
        style("""
            html, body {
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100%;
            }

            body {
                display: flex;
                justify-content: center;
                align-items: center;
            }

            .container {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }
        """)

    with doc:
        for node in nodes:
            doc.add(render_with_children(node))

    return doc.render()


def save_html(path: str, html_text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    print(f"HTML saved: {path}")
