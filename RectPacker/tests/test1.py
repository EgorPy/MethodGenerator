from RectPacker.layout.RectPacker import Rect

parent_rect = Rect(1536, 864, margin_x=0, margin_y=0)
email_input = Rect(10, 40, pwidth=50, max_width=1000, parent=parent_rect)

print(email_input)
print(Rect.pack_rects(parent_rect, [email_input]))
