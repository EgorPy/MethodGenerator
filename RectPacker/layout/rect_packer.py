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
                 element: str = None):

        self.parent = parent
        self.element = element

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

        if not self.check_width_range(width):
            raise ValueError(f"width must be from {min_width} to {max_width}")
        if not self.check_height_range(height):
            raise ValueError(f"height must be from {min_height} to {max_height}")

        self._width = int(width)
        self._height = int(height)

        self.margin_x = int(margin_x)
        self.margin_y = int(margin_y)

    def __str__(self):
        return f"Rect({self.width}x{self.height}, pos=({self.x}, {self.y}))"

    def check_width_range(self, width: int):
        return self.min_width <= width <= self.max_width

    def check_height_range(self, height: int):
        return self.min_height <= height <= self.max_height

    def width_to_range(self, width: int):
        width = int(width)
        if width < self.min_width:
            return self.min_width
        if width > self.max_width:
            return self.max_width
        return width

    def height_to_range(self, height: int):
        height = int(height)
        if height < self.min_height:
            return self.min_height
        if height > self.max_height:
            return self.max_height
        return height

    @property
    def width(self):
        width = self._width
        if self.pwidth is not None and self.parent is not None:
            width = self.parent.width * (self.pwidth / 100)
        return int(self.width_to_range(width))

    @property
    def height(self):
        height = self._height
        if self.pheight is not None and self.parent is not None:
            height = self.parent.height * (self.pheight / 100)
        return int(self.height_to_range(height))

    @property
    def size(self):
        return int(self.size_x), int(self.size_y)

    @property
    def size_x(self):
        return int(self.width + self.margin_x * 2)

    @property
    def size_y(self):
        return int(self.height + self.margin_y * 2)

    @staticmethod
    def pack_rects(parent_rect: "Rect", rects: list["Rect"]):
        rects = list(rects)

        min_size_y = min(rect.size_y for rect in rects)
        min_size_x = min(rect.size_x for rect in rects)

        parent_w = int(parent_rect.width)
        parent_h = int(parent_rect.height)

        idx = 0
        for y in range(0, parent_h - min_size_y + 1, min_size_y):
            for x in range(0, parent_w - min_size_x + 1, min_size_x):
                rect = rects[idx]
                rect.x = int(x)
                rect.y = int(y)
                idx += 1

                if idx == len(rects):
                    return

        raise ValueError("Cannot pack rects in the specified parent")
