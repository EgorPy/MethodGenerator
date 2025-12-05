from RectPacker.layout.rect_packer import Rect


class UIElementRect(Rect):
    def __init__(self, *, type_, props=None, **kwargs):
        super().__init__(**kwargs)
        self.type_ = type_
        self.props = props or {}
