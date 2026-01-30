from RectPacker.layout.ui_node import UINode, ElementType
from RectPacker.layout.node_registry import registry
from core.logger import logger

from dominate.tags import style, link, meta, script, div, form
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

    input_children = any(
        child.type_ in {
            ElementType.TEXT_INPUT,
            ElementType.CHECKBOX,
            ElementType.RADIOBUTTON,
            ElementType.DROPDOWN,
            ElementType.IMG_INPUT,
        } for child in node.children
    )
    if input_children and node.type_ != ElementType.CONTAINER:
        f = form()
        f.add(tag)
        tag = f

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
        """
        Collects CSS files for:
        - base element class (node.type_.value)
        - extra classes from props.class
        """

        classes = set()
        # element base class
        classes.add(node.type_.value)

        # additional classes
        props = node.props or {}
        extra = props.get("class")
        if extra:
            for cls in extra.split():
                classes.add(cls)

        # check the existence of css files
        for cls in classes:
            css_name = f"{cls}.css"
            if css_name in available_css_files:
                included_css.add(css_name)

        # recursion
        for child in node.children:
            scan_node_for_css(child)

    for node in nodes:
        scan_node_for_css(node)

    logger.info(f"Generating HTML page '{title}'")
    logger.debug(f"Included CSS modules: {sorted(list(included_css))}")

    with doc.head:
        for f in included_css:
            link(rel="stylesheet", href=f"../../frontend/web/static/{f}")

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
        """)

        script(src="../../frontend/web/static/actions.js")
        script(src="../../frontend/web/static/runtime.js")

    with doc:
        for node in nodes:
            doc.add(render_with_children(node))

    html = doc.render()
    logger.info("HTML generation completed")
    return html


def save_html(path: str, html_text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    logger.info(f"HTML saved: {path}")
