from enum import Enum


class Layout(str, Enum):
    NONE = "none"
    VERTICAL = "vertical"
    HORIZONTAL = "horizontal"
    CENTER = "center"
    GRID = "grid"


class Align(str, Enum):
    START = "start"
    CENTER = "center"
    END = "end"
    STRETCH = "stretch"


class Justify(str, Enum):
    START = "start"
    CENTER = "center"
    END = "end"
    SPACE_BETWEEN = "space-between"


class ElementType(str, Enum):
    CONTAINER = "container"
    TEXT_INPUT = "text_input"
    CHECKBOX = "checkbox"
    RADIOBUTTON = "radiobutton"
    DROPDOWN = "dropdown"
    H1 = "h1"
    H2 = "h2"
    H3 = "h3"
    IMG_INPUT = "img_input"
    IMG_OUTPUT = "img_output"
    BUTTON = "button"


class Action(str, Enum):
    SUBMIT = "submit"
    CLICK = "click"
    CHANGE = "change"
