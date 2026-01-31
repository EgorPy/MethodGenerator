from RectPacker.legacy.generate_rect_html import generate_page, save_html

# Здесь могли бы подключаться Rect и layout-алгоритмы, если нужно.

elements = [
    {"type": "text_input", "placeholder": "Введите имя", "min_width": 300},
    {"type": "checkbox"},
    {"type": "radiobutton"},
    {"type": "dropdown", "options": ["A", "B", "C"], "min_width": 200},
    {"type": "text_output", "text": "Результат"},
    {"type": "img_input"},
    {"type": "img_output", "src": "../frontend/web/static/favicon.ico"}
]

html = generate_page(elements, 1536, 864)
save_html("generated.html", html)

print("Страница generated.html создана.")
