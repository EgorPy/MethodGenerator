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
                 element: str = None,
                 is_container: bool = False,
                 children: list["Rect"] = None,
                 center_x: bool = False,
                 center_y: bool = False):

        self.parent = parent
        self.element = element
        self.is_container = is_container

        self.center_x = center_x
        self.center_y = center_y

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
        """
        Defines rect.x and rect.y taking into account margin and size_x/size_y.
        Child positions are relative to parent_rect (0..parent.width).
        If parent_rect.is_container is True, children positions ignore parent's margin influence
        (we place children starting at 0,0 inside the parent content box).
        Each rect can request center_x/center_y and then is centered relative to parent_rect.
        """

        x_cursor = 0
        y_cursor = 0
        row_height = 0

        for rect in rects:
            # compute advance width we will need for this rect (how much x_cursor will move)
            advance = rect.size_x

            # wrap to next line if it doesn't fit
            if x_cursor + advance > parent_rect.width:
                x_cursor = 0
                y_cursor += row_height
                row_height = 0

            # position relative to parent: if parent is a container, start at 0+margin ignored for parent,
            # otherwise we keep the child's margin as offset
            if parent_rect.is_container:
                rect.x = x_cursor
                rect.y = y_cursor
            else:
                rect.x = x_cursor + rect.margin_x
                rect.y = y_cursor + rect.margin_y

            x_cursor += advance
            row_height = max(row_height, rect.size_y)

            if y_cursor + rect.size_y > parent_rect.height:
                raise ValueError("Cannot pack rects in the specified parent")

        if parent_rect.center_x:
            # 1. Разбить элементы на строки
            rows = []
            current_row = []
            current_width = 0

            for rect in rects:
                if current_width + rect.size_x > parent_rect.width:
                    rows.append(current_row)
                    current_row = []
                    current_width = 0

                current_row.append(rect)
                current_width += rect.size_x

            if current_row:
                rows.append(current_row)

            # 2. Центрировать каждую строку
            y = parent_rect.y

            for row in rows:
                total = sum(r.size_x for r in row)
                start_x = parent_rect.x + (parent_rect.width - total) // 2

                x = start_x
                max_h = 0

                for r in row:
                    r.x = x
                    r.y = y
                    x += r.size_x
                    max_h = max(max_h, r.size_y)

                y += max_h

        # for rect in rects:
        #     if rect.center_x and parent_rect is not None:
        #         rect.x = (parent_rect.width - rect.width) // 2
        #     if rect.center_y and parent_rect is not None:
        #         rect.y = (parent_rect.height - rect.height) // 2

        # legacy: optional global centered arg - center all children horizontally
        # if centered:
        #     for rect in rects:
        #         rect.x = (parent_rect.width - rect.width) // 2
