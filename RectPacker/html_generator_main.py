from layout.generate_html import generate_page, save_html
from layout.ui_rect import UIElementRect

elements = [
    UIElementRect(
        margin_x=0,
        margin_y=10,
        type_="text_input",
        width=0, height=50,
        pwidth=50,
        min_width=200,
        max_width=400,
        props={}
    ) for i in range(5)

    # UIElementRect(
    #     margin_x=0,
    #     margin_y=0,
    #     type_="dropdown",
    #     width=300, height=40,
    #     pwidth=30,  # ← адаптивно
    #     props={"options": ["A", "B", "C"]}
    # ),
    #
    # UIElementRect(
    #     margin_x=0,
    #     margin_y=0,
    #     type_="text_output",
    #     width=400, height=100,
    #     pwidth=80,
    #     props={"text": "Hello adaptive world"}
    # ),
    #
    # UIElementRect(
    #     margin_x=0,
    #     margin_y=0,
    #     type_="img_output",
    #     width=200, height=150,
    #     pwidth=25,
    #     props={"src": "https://picsum.photos/200/150"}
    # )
]
e = UIElementRect(
    margin_x=0,
    margin_y=10,
    type_="container",
    width=750, height=50,
    min_width=200,
    max_width=750,
    props={}
)
elements.insert(0, e)
UIElementRect(
    parent=e,
    margin_x=0,
    margin_y=10,
    type_="h3",
    width=300, height=30,
    # pwidth=50,
    min_width=200,
    max_width=300,
    props={"text": "Есть аккаунт?"}
)
UIElementRect(
    parent=e,
    margin_x=0,
    margin_y=10,
    type_="h3",
    width=300, height=30,
    # pwidth=50,
    min_width=200,
    max_width=300,
    props={"text": "Вход"}
)
elements.insert(0, UIElementRect(
    margin_x=0,
    margin_y=10,
    type_="h1",
    width=0, height=50,
    pwidth=50,
    min_width=200,
    max_width=350,
    props={"text": "Регистрация аккаунта"}
))

html = generate_page(elements, 1536, 864, centered=True)
save_html("page.html", html)
print("Created page.html")
