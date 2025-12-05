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
                 parent: "Rect" = None):
        """
        Create instance of the rect element for rect packing algorithm

        :param width: width property
        :param height: height property
        :param margin_x: margin x
        :param margin_y: margin y
        :param min_width: minimum width
        :param min_height: minimum height
        :param max_width: maximum width
        :param max_height: maximum height
        :param pwidth: width of the element defined as percentage of the width of the parent element
        :param pheight:
        """

        self.parent = parent

        self.min_width = min_width
        self.min_height = min_height

        self.max_width = max_width
        self.max_height = max_height

        max_pwidth = 100
        if pwidth is not None and (0 > pwidth or max_pwidth < pwidth):
            raise ValueError(f"pwidth must be from 0 to {max_pwidth}")
        if pheight is not None and (0 > pheight or max_pwidth < pheight):
            raise ValueError(f"pheight must be from 0 to {max_pwidth}")
        self.pwidth = pwidth
        self.pheight = pheight

        if not self.check_width_range(width):
            raise ValueError(f"width must be from {min_width} to {max_width}")
        if not self.check_height_range(height):
            raise ValueError(f"height must be from {min_height} to {max_height}")
        self._width = width
        self._height = height

        self.margin_x = margin_x
        self.margin_y = margin_y

    def __str__(self):
        return f"Rect with size: {self.width + self.margin_x * 2}, {self.height + self.margin_y * 2}"

    def check_width_range(self, width: int):
        if width < self.min_width or width > self.max_width:
            return False
        return True

    def check_height_range(self, height: int):
        if height < self.min_height or height > self.max_height:
            return False
        return True

    def width_to_range(self, width: int):
        if width < self.min_width + self.margin_x * 2:
            return self.min_width + self.margin_x * 2
        if width > self.max_width - self.margin_x * 2:
            return self.max_width - self.margin_x * 2
        return width

    def height_to_range(self, height: int):
        if height < self.min_height + self.margin_y * 2:
            return self.min_height + self.margin_y * 2
        if height > self.max_height - self.margin_y * 2:
            return self.max_height - self.margin_y * 2
        return height

    @property
    def width(self):
        width = self._width
        if self.pwidth is not None and self.parent is not None:
            width = self.parent.width * (self.pwidth / 100)
        return self.width_to_range(width)

    @property
    def height(self):
        height = self._height
        if self.pheight is not None and self.parent is not None:
            height = self.parent.height * (self.pheight / 100)
        return self.height_to_range(height)

    @property
    def size(self):
        """
        Size differs from width and height.
        Size takes into account the margin of the rect.
        """

        return self.width + self.margin_x * 2, self.height + self.margin_y * 2

    @property
    def size_x(self):
        return self.width + self.margin_x * 2

    @property
    def size_y(self):
        return self.height + self.margin_y * 2

    @staticmethod
    def pack_rects(parent_rect: "Rect", rects: list["Rect"]):
        rect_positions = []

        min_rect_size_y = min([rect.size_y for rect in rects])
        min_rect_size_x = min([rect.size_x for rect in rects])
        for y in range(0, int(parent_rect.height - min_rect_size_y), int(min_rect_size_y)):
            for x in range(0, int(parent_rect.width - min_rect_size_x), int(min_rect_size_x)):
                rect_positions.append((x, y))
                if len(rect_positions) == len(rects):
                    return rect_positions
        if len(rect_positions) != len(rects):
            raise ValueError("Cannot pack rects in the specified parent")

    # @staticmethod
    # def pack_rects(parent_rect: "Rect", rects: list["Rect"]):
    #     x, y, max_row_height = 0, 0, 0
    #
    #     for rect in rects:
    #         if x + rect.size_x > parent_rect.width:
    #             x, max_row_height = 0, 0
    #             y += max_row_height
    #
    #         rect.x = x
    #         rect.y = y
    #
    #         x += rect.size_x
    #         max_row_height = max(max_row_height, rect.size_y)
    #
    #     return rects
