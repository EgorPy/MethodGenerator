from core.html_generation import generate_page, save_html
from core.html_generation import UIElementRect

elements = [
    UIElementRect(
        margin_x=0,
        margin_y=0,
        type_="text_input",
        width=0, height=40,
        pwidth=50,  # ← адаптивно
        min_width=200,
        props={}
    ),

    UIElementRect(
        margin_x=0,
        margin_y=0,
        type_="dropdown",
        width=300, height=40,
        pwidth=30,  # ← адаптивно
        props={"options": ["A", "B", "C"]}
    ),

    UIElementRect(
        margin_x=0,
        margin_y=0,
        type_="text_output",
        width=400, height=100,
        pwidth=80,
        props={"text": "Hello adaptive world"}
    ),

    UIElementRect(
        margin_x=0,
        margin_y=0,
        type_="img_output",
        width=200, height=150,
        pwidth=25,
        props={"src": "https://picsum.photos/200/150"}
    )
]

html = generate_page(elements, 1536, 864)
save_html("page.html", html)
print("Created page.html")
