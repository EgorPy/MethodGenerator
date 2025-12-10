from RectPacker.layout.rect_packer import Rect


class UIElementRect(Rect):
    def __init__(self, *, type_, props=None, parent=None, **kwargs):
        # props может содержать center_x/center_y
        props = props or {}
        is_container = (type_ == "container")
        center_x = bool(props.get("center_x", False))
        center_y = bool(props.get("center_y", False))

        super().__init__(parent=parent, is_container=is_container, center_x=center_x, center_y=center_y, **kwargs)
        self.type_ = type_
        self.props = props

        if parent is not None:
            parent.children.append(self)
