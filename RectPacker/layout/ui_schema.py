from RectPacker.layout.ui_node import Layout, Align, Justify, ElementType, Action

UI_SCHEMA = {
    "layout": [e.value for e in Layout],
    "align": [e.value for e in Align],
    "justify": [e.value for e in Justify],
    "elements": [e.value for e in ElementType],
    "actions": [e.value for e in Action],
}
