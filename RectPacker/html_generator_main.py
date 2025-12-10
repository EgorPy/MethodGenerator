from layout.generate_html import generate_page, save_html
from layout.ui_rect import UIElementRect

elements = []

e = UIElementRect(
    margin_x=0,
    margin_y=0,
    type_="container",
    width=1536, height=864,
    props={"center_x": True}
)
elements.append(e)  # обязательно добавить корень в список

e1 = UIElementRect(
    parent=e,
    margin_y=0,
    margin_x=0,
    type_="container",
    width=200, height=400,
    props={"center_x": True}
)
elements.append(e1)  # добавляем контейнер-потомка в общий список,
# если generate_page ожидает плоский список

# создаём и сохраняем объекты-потомки; присваиваем переменным и кладём в elements
child1 = UIElementRect(
    parent=e1,
    margin_x=0,
    margin_y=10,
    type_="h3",
    width=150, height=30,
    min_width=50,
    max_width=300,
    props={"text": "Есть аккаунт?"}
)
# elements.append(child1)

child2 = UIElementRect(
    parent=e1,
    margin_x=0,
    margin_y=10,
    type_="h3",
    width=150, height=30,
    min_width=50,
    max_width=300,
    props={"text": "Вход"}
)

# elements.append(child2)

html = generate_page(elements, 1536, 864)
save_html("page.html", html)
print("Created page.html")
