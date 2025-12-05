from RectPacker.layout.generate_html import generate_page, save_html
from RectPacker.layout.ui_rect import UIElementRect

elements = [
    UIElementRect(
        type_="text_input",
        width=200, height=40,
        pwidth=50,  # ← адаптивно
        min_width=200,
        props={}
    ),

    UIElementRect(
        type_="dropdown",
        width=300, height=40,
        pwidth=30,  # ← адаптивно
        props={"options": ["A", "B", "C"]}
    ),

    UIElementRect(
        type_="text_output",
        width=400, height=100,
        pwidth=80,
        props={"text": "Hello adaptive world"}
    ),

    UIElementRect(
        type_="img_output",
        width=200, height=150,
        pwidth=25,
        props={"src": "https://via.placeholder.com/150"}
    )
]

html = generate_page(elements, screen_width=1536, screen_height=864)
save_html("page.html", html)

print("page.html создан.")
