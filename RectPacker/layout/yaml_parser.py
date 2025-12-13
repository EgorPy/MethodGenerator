from RectPacker.layout.ui_enums import ElementType, Layout, Align, Justify, Action
from RectPacker.layout.generate_node_html import generate_page_from_ui_tree, save_html
from RectPacker.layout.ui_node import UINode

from typing import Any, Callable, List, Optional, Dict
import yaml


def load_yaml(path: str) -> dict:
    """ Load file to dict """

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_enum(enum_cls, value: Optional[str]):
    """ Parse string to Enum. None returns None. """

    if value is None:
        return None
    try:
        return enum_cls(value)
    except ValueError:
        raise ValueError(f"Invalid value '{value}' for enum {enum_cls.__name__}")


class ValidationError(Exception):
    pass


class ValidatorRegistry:
    """
    UINode validation registry.
    Validators are called recursively for every node.
    """

    def __init__(self):
        self.validators: List[Callable[[UINode], None]] = []

    def register(self, func: Callable[[UINode], None]):
        """ Add validator. """

        self.validators.append(func)
        return func

    def validate_node(self, node: UINode):
        for v in self.validators:
            v(node)
        for child in node.children:
            self.validate_node(child)


validator_registry = ValidatorRegistry()


@validator_registry.register
def bind_only_for_inputs(node: UINode):
    input_types = {ElementType.TEXT_INPUT, ElementType.CHECKBOX, ElementType.RADIOBUTTON,
                   ElementType.DROPDOWN, ElementType.IMG_INPUT}
    if node.bind is not None and node.type_ not in input_types:
        raise ValidationError(f"'bind' cannot be used for type {node.type_}")


@validator_registry.register
def action_only_for_buttons(node: UINode):
    if node.action is not None and node.type_ != ElementType.BUTTON:
        raise ValidationError(f"'action' can be used only for button, but found {node.type_}")


@validator_registry.register
def children_only_for_containers(node: UINode):
    if node.children and node.type_ != ElementType.CONTAINER:
        allowed = {ElementType.CONTAINER}
        if node.type_ not in allowed:
            raise ValidationError(f"Only container can have children, but {node.type_} have children")


def parse_ui_node(data: Dict[str, Any]) -> UINode:
    if not isinstance(data, dict):
        raise TypeError("Every node must be a dict")

    if "type" not in data:
        raise ValueError("A node must have key 'type'")

    node = UINode(
        type_=parse_enum(ElementType, data["type"]),
        props=data.get("props"),
        layout=parse_enum(Layout, data.get("layout")) or Layout.NONE,
        gap=data.get("gap"),
        align=parse_enum(Align, data.get("align")),
        justify=parse_enum(Justify, data.get("justify")),
        grow=data.get("grow"),
        shrink=data.get("shrink"),
        basis=data.get("basis"),
        max_width=data.get("max_width"),
        min_width=data.get("min_width"),
        bind=data.get("bind"),
        action=parse_enum(Action, data.get("action")),
        endpoint=data.get("endpoint"),
    )

    children = data.get("children", [])
    if not isinstance(children, list):
        raise TypeError("'children' must be a list")

    for child_data in children:
        node.add(parse_ui_node(child_data))

    return node


def parse_ui_yaml(path: str, validate: bool = True) -> UINode:
    """
    Loads YAML file and converts it to UINode.
    If validate=True, validates the node tree.
    """

    data = load_yaml(path)
    root = parse_ui_node(data)

    if validate:
        validator_registry.validate_node(root)

    return root


def generate_html_from_yaml(path: str, html_path: str, screen_width: int = 1536,
                            screen_height: int = 864, validate: bool = True):
    """
    Loads YAML → parses → validates → generates HTML and saves it.
    """
    root_node = parse_ui_yaml(path, validate=validate)

    html_text = generate_page_from_ui_tree([root_node])

    save_html(html_path, html_text)
    print(f"HTML успешно создан: {html_path}")


generate_html_from_yaml("../../example.yaml", "page.html")
