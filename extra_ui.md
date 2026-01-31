# UI Zones Around Form

This documentation describes the system for adding additional UI elements around a form generated from the API using YAML
elements.

## 1. Core Concept

* The **form** is always located at the center of the page. This is the main YAML generated from `actions.js`.
* **Additional elements** are added via the `@ui(...)` decorator and are separate YAML elements.
* **Elements can be placed around the form** or relative to the page without breaking the form structure.
* Each zone can contain **multiple elements** and have a **layout**: `horizontal` or `vertical`.

## 1.1 `@ui(...)` Decorator

The `@ui(...)` decorator allows you to attach **additional YAML UI elements** to an API endpoint without changing the API logic.
These elements can be positioned **around the central form** or **relative to the page**, using predefined zones.

### Usage

```python
from fastapi import APIRouter

from core.ui_decorator import ui

router = APIRouter()


@ui("/path/to/header.yaml", "/path/to/footer.yaml")
@router.post("/login/")
async def login():
    ...
```

* Each argument to `@ui(...)` is a **path or URL** to a YAML element.
* You can attach **multiple elements at once** by listing them in the decorator.
* YAML elements can contain any supported UI components: text, buttons, images, notifications, custom elements, etc.

### Features

1. **Zone-based placement**
   Each YAML element must specify a `props.position` field to indicate where it should appear relative to the form or page.
   Supported positions include:

    * `form-top`, `form-bottom`, `form-left`, `form-right`
    * `page-top-left`, `page-top-right`, `page-bottom-left`, `page-bottom-right`
    * (Future zones can be added, e.g., `page-left` / `page-right`)

2. **Layout**

    * Use `props.layout` to define how child elements are arranged inside the container.
    * Options: `vertical` or `horizontal`.

3. **Multiple elements per zone**

    * Each YAML element can contain multiple children.
    * Multiple elements can also be attached in a single `@ui(...)` decorator or via multiple decorators; they will render in the
      order defined.

4. **Central form safety**

    * The form generated from the API YAML always stays in the center; adding UI elements via `@ui(...)` **does not break the form
      ** or API logic.

5. **Styling**

    * Any element can define inline styles via `props.style` or custom CSS classes via `props.class`.
    * CSS files with the same name as the element type are automatically included during page generation.

### Example: Adding a header and a sidebar

```python
from fastapi import APIRouter

from core.ui_decorator import ui

router = APIRouter()


@ui("/static/yaml/header.yaml", "/static/yaml/sidebar.yaml")
@router.post("/login/")
async def login():
   ...
```

* `header.yaml` might define a title and instructions (`position: form-top`).
* `sidebar.yaml` could define a vertical menu or tips (`position: form-left`).

> This approach allows you to **decorate any endpoint** with rich UI content without changing the underlying API behavior.

---

## 2. Zones Around the Form

| Zone                | Placement         | Default Layout        | Example Elements               |
|---------------------|-------------------|-----------------------|--------------------------------|
| `form-top`          | Above the form    | vertical              | Headings, instructions, tips   |
| `form-bottom`       | Below the form    | horizontal            | Buttons, links, tips           |
| `form-left`         | Left of the form  | vertical              | Text tips, icons               |
| `form-right`        | Right of the form | vertical              | Text tips, icons               |
| `page-top-left`     | Page corner       | horizontal / vertical | Toasts, notifications, banners |
| `page-top-right`    | Page corner       | horizontal / vertical | Toasts, notifications, banners |
| `page-bottom-left`  | Page corner       | horizontal / vertical | Toasts, notifications, banners |
| `page-bottom-right` | Page corner       | horizontal / vertical | Toasts, notifications, banners |

> **The form always stays in the center**; elements in zones should not modify the order of form fields.

---

## 3. YAML Structure

Example of adding text above the form:

```yaml
type: container
props:
  position: form-top
  layout: vertical
children:
  - type: h1
    props:
      text: "Login"
  - type: h3
    props:
      text: "Please enter your credentials"
```

Example of multiple elements around the form:

```yaml
type: container
props:
  position: form-right
  layout: vertical
children:
  - type: h3
    props:
      text: "Tip 1"
  - type: h3
    props:
      text: "Tip 2"

type: container
props:
  position: form-left
  layout: vertical
children:
  - type: h3
    props:
      text: "Instruction A"
  - type: h3
    props:
      text: "Instruction B"
```

---

## 4. Page Generation Algorithm

1. Load the **main YAML form** → center of the page.
2. Load **additional YAML elements** via `@ui(...)`.
3. Group elements by `props.position`.
4. Render each zone:

* `form-top` / `form-bottom` → vertical or horizontal flex above/below the form.
* `form-left` / `form-right` → vertical flex left/right of the form.
* Corner zones → absolute / fixed containers, layout is specified in props.
* The center form remains unchanged.

---

## 5. Advantages

* Supports text **above and around the form**.
* Allows adding **any number of elements** to any zone.
* The form center is always safe; API logic is not broken.
* Easy to extend with new YAML elements.
* Supports horizontal and vertical layout for each zone.

---

## 6. Examples of Elements Around the Form and Page Corners

### 6.1 Elements Around the Form

**Above the form (`form-top`)**

```yaml
type: container
props:
  position: form-top
  layout: vertical
children:
  - type: h1
    props:
      text: "Login"
  - type: h3
    props:
      text: "Please enter your credentials"
  - type: p
    props:
      text: "You can use your email or username"
```

**Below the form (`form-bottom`)**

```yaml
type: container
props:
  position: form-bottom
  layout: horizontal
children:
  - type: button
    props:
      text: "Submit"
      class: "primary-button"
  - type: button
    props:
      text: "Cancel"
      class: "secondary-button"
  - type: a
    props:
      text: "Forgot password?"
      href: "#"
```

**Left of the form (`form-left`)**

```yaml
type: container
props:
  position: form-left
  layout: vertical
children:
  - type: h3
    props:
      text: "Tip 1: Use a strong password"
  - type: h3
    props:
      text: "Tip 2: Keep your email safe"
  - type: img
    props:
      src: "/static/icons/security.png"
      alt: "Security icon"
```

**Right of the form (`form-right`)**

```yaml
type: container
props:
  position: form-right
  layout: vertical
children:
  - type: h3
    props:
      text: "Need help?"
  - type: button
    props:
      text: "Support"
      class: "support-button"
  - type: ui-error-toast
```

---

### 6.2 Elements in Page Corners

**Top-left corner (`page-top-left`)**

```yaml
type: container
props:
  position: page-top-left
  layout: vertical
children:
  - type: ui-notification
    props:
      text: "New update available!"
  - type: h3
    props:
      text: "Welcome back!"
```

**Top-right corner (`page-top-right`)**

```yaml
type: container
props:
  position: page-top-right
  layout: horizontal
children:
  - type: ui-error-toast
  - type: button
    props:
      text: "Retry"
      class: "retry-button"
```

**Bottom-left corner (`page-bottom-left`)**

```yaml
type: container
props:
  position: page-bottom-left
  layout: vertical
children:
  - type: h3
    props:
      text: "Tip of the day:"
  - type: p
    props:
      text: "Check your profile settings regularly."
```

**Bottom-right corner (`page-bottom-right`)**

```yaml
type: container
props:
  position: page-bottom-right
  layout: horizontal
children:
  - type: ui-notification
    props:
      text: "You have 3 unread messages"
  - type: button
    props:
      text: "Open Inbox"
      class: "inbox-button"
```

## 7. Sidebars and Distance from the Form

### 7.1 `form-left` / `form-right`

* These zones are **directly next to the form** (left or right).
* **Default behavior:** the container hugs the form with minimal spacing.
* **Use case:** tips, small menus, icons, or helper elements that should stay close to the form.
* **Width and spacing:** controlled via `props.style` (e.g., `width`, `margin`).

Example: small tips container left of the form:

```yaml
type: container
props:
  position: form-left
  layout: vertical
  style:
    width: 150px
    padding: 8px
children:
  - type: h3
    props:
      text: "Tip 1"
  - type: h3
    props:
      text: "Tip 2"
```

> The container stays immediately next to the form; it does **not** extend to the edge of the screen.

---

### 7.2 Full Sidebars to the Edge of the Screen

* If you want a **sidebar stretching from top to bottom or from the edge of the page**, it’s recommended to use a **page-level
  zone**:

    * `page-top-left`, `page-bottom-left`, or a new `page-left` zone.
* **Positioning:** absolute or fixed via `props.style` to extend to the page edge without affecting the central form.
* **Use case:** full navigation menus, notification panels, or large vertical content.

Example: full left sidebar:

```yaml
type: container
props:
  position: page-left   # page-level zone
  layout: vertical
  style:
    width: 250px
    height: 100vh
    position: fixed
    top: 0
    left: 0
    background-color: "#f5f5f5"
    padding: 12px
children:
  - type: h3
    props:
      text: "Dashboard"
  - type: button
    props:
      text: "Profile"
  - type: button
    props:
      text: "Settings"
  - type: button
    props:
      text: "Logout"
```

> This sidebar **does not interfere with the central form** and spans the entire left edge of the screen.

---

### 7.3 Recommendation

| Zone type                  | Use case                                 | Positioning                                                     |
|----------------------------|------------------------------------------|-----------------------------------------------------------------|
| `form-left` / `form-right` | Small tips, helper elements, minor menus | Hug the form; flexible width via style                          |
| `page-left` / `page-right` | Full sidebar, navigation, notifications  | Absolute or fixed to the page edge; does not affect form center |

> Central form always remains in the middle of the page; adding sidebars or elements around it **never breaks API-generated layout
**.
