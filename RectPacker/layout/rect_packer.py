class Rect:
    def __init__(self,
                 width: int,
                 height: int,
                 margin_x: int = 20,
                 margin_y: int = 10,
                 min_width: int = 10,
                 min_height: int = 10,
                 max_width: int = 10000,
                 max_height: int = 10000,
                 pwidth: int = None,
                 pheight: int = None,
                 parent: "Rect" = None,
                 children: list["Rect"] = None,
                 element: str = None):

        self.parent = parent
        self.element = element

        if children is None:
            self.children = []
        else:
            self.children = children

        self.x = 0
        self.y = 0

        self.min_width = min_width
        self.min_height = min_height
        self.max_width = max_width
        self.max_height = max_height

        if pwidth is not None and not (0 <= pwidth <= 100):
            raise ValueError("pwidth must be from 0 to 100")
        if pheight is not None and not (0 <= pheight <= 100):
            raise ValueError("pheight must be from 0 to 100")

        self.pwidth = pwidth
        self.pheight = pheight

        self._width = int(width)
        self._height = int(height)

        self.margin_x = int(margin_x)
        self.margin_y = int(margin_y)

    @property
    def width(self):
        width = self._width
        if self.pwidth is not None and self.parent is not None:
            width = self.parent.width * self.pwidth / 100
        return int(self.width_to_range(width))

    @property
    def height(self):
        height = self._height
        if self.pheight is not None and self.parent is not None:
            height = self.parent.height * self.pheight / 100
        return int(self.height_to_range(height))

    def width_to_range(self, width):
        width = int(width)
        if width < self.min_width:
            return self.min_width
        if width > self.max_width:
            return self.max_width
        return width

    def height_to_range(self, height):
        height = int(height)
        if height < self.min_height:
            return self.min_height
        if height > self.max_height:
            return self.max_height
        return height

    @property
    def size_x(self):
        return self.width + self.margin_x * 2

    @property
    def size_y(self):
        return self.height + self.margin_y * 2

    @property
    def size(self):
        return self.size_x, self.size_y

    @staticmethod
    def pack_rects(parent_rect: "Rect", rects: list["Rect"], centered: bool = False):
        """ Defines rect.x and rect.y taking into account margin and size_x/size_y """

        x_cursor = 0
        y_cursor = 0
        row_height = 0

        for rect in rects:
            if x_cursor + rect.size_x > parent_rect.width:
                x_cursor = 0
                y_cursor += row_height
                row_height = 0

            rect.x = x_cursor + rect.margin_x
            rect.y = y_cursor + rect.margin_y

            x_cursor += rect.size_x
            row_height = max(row_height, rect.size_y)

            if y_cursor + rect.size_y > parent_rect.height:
                raise ValueError("Cannot pack rects in the specified parent")

        if centered:
            y_cursor = 0
            for rect in rects:
                rect.x = (parent_rect.width - rect.width) // 2
                y_cursor += row_height
                rect.y = y_cursor + rect.margin_y
