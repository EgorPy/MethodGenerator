class UINode:
    def __init__(
            self,
            type_: str,
            *,
            props: dict | None = None,
            children: list["UINode"] | None = None,

            layout: str | None = None,  # vertical | horizontal | center | grid | None
            gap: int | None = None,

            align: str | None = None,  # start | center | end | stretch
            justify: str | None = None,  # start | center | end | space-between

            grow: int | None = None,  # flex-grow
            shrink: int | None = None,  # flex-shrink
            basis: str | None = None,  # flex-basis

            max_width: int | None = None,
            min_width: int | None = None,

            bind: str | None = None,  # input/output binding
            action: str | None = None,  # submit / click / etc.
            endpoint: str | None = None,  # API endpoint
    ):
        self.type_ = type_
        self.props = props or {}
        self.children = children or []

        self.layout = layout
        self.gap = gap

        self.align = align
        self.justify = justify

        self.grow = grow
        self.shrink = shrink
        self.basis = basis

        self.max_width = max_width
        self.min_width = min_width

        self.bind = bind
        self.action = action
        self.endpoint = endpoint

    def add(self, child: "UINode"):
        self.children.append(child)
        return child
