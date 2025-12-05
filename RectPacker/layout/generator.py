from element_registry import registry
from dominate.tags import style
from dominate import document


def generate_html(elements):
    """
    elements — список словарей:
      { "type": "text_input", "min_width": 250, ... }

    Возвращает строку HTML.
    """

    doc = document(title="Generated Page")

    with doc.head:
        style("""
            body {
                margin: 0;
                padding: 12px;
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                gap: 12px;
                width: 100vw;
                height: 100vh;
                box-sizing: border-box;
            }
            input, select, img, div {
                box-sizing: border-box;
            }
            img {
                width: 100%;
                height: auto;
            }
        """)

    with doc:
        for el in elements:
            t = el.get("type")
            if t not in registry:
                raise ValueError(f"Unknown element type: {t}")
            node = registry[t](el)
            doc.add(node)

    return doc.render()


def save_html(filename, html_text):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_text)
