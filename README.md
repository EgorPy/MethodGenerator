# MethodGenerator

MethodGenerator is a **full-stack UI generation framework** that automates the creation of web pages from backend API endpoints.
It provides a flexible system to attach additional UI elements via YAML while preserving the core form generated from the API.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Core Concept](#core-concept)
3. [Workflow](#workflow)
4. [UI Decorator (`@ui`) and YAML Elements](#ui-decorator-ui-and-yaml-elements)
5. [Process Overview](#process-overview--generation-flow)
6. [Zones and Layouts](#zones-and-layouts)
7. [CSS and Styling](#css-and-styling)
8. [Runtime Behavior](#runtime-behavior-runtimejs)
9. [Extending MethodGenerator](#extending-methodgenerator)
10. [Testing](#testing)

---

## Project Structure

```
backend/
  backend_main.py
  __init__.py
  services/
    auth/
      service.py
      __init__.py
      api/
        auth.py
      logic/
        auth_logic.py
        security.py

core/
  build_site.py
  config.py
  core_main.py
  generate_config_js.py
  logger.py
  main.py
  method_generator.py
  redirects.py
  registry.py
  service_loader.py
  task.py
  ui_decorator.py
  actions_generation/
    actions_parser.py
    action_model.py
    api_inspector.py
    generate_actions_js.py
  html_generation/
    element_types.yaml
    generate_node_html.py
    login_example.yaml
    node_registry.py
    ui_enums.py
    ui_node.py
    yaml_parser.py

frontend/
  frontend_main.py
  ui_yaml/
    auth_login.yaml
    auth_register.yaml
    ...
  web/
    pages/
      auth_login.html
      profile.html
      ...
    static/
      actions.js
      runtime.js
      ui_error_toast.css
      ...
```

* **backend/**: main backend logic, services, and API endpoints. Every service should provide service.py with FastAPI router
* **core/**: the engine of MethodGenerator (site building, HTML, YAML generation, actions parsing, UI decorator, runtime config).
* **frontend/**: generated UI YAML, pages, static assets.
* **RectPacker/**: legacy HTML layout utilities.
* **tests/**: test scripts for backend, frontend, and runtime.

---

## Core Concept

1. The **central form** is generated automatically from backend API endpoints.
   Each endpoint’s payload fields are converted into form inputs.
2. Additional UI elements (header, footer, banners, sidebars, text, etc.) can be attached via `@ui(...)` using separate YAML
   files.
3. **The form always stays in the center** of the page; additional elements are rendered around it without breaking its layout.
4. Runtime logic handles fetching, submission, error handling, and dynamic updates for a fully interactive page.

---

## Workflow

1. **API Definition**: Define endpoints in FastAPI.
   Include payload fields and optional redirect behavior.
   Every API endpoint provides a redirect, by default redirect is set to "self" which is the same page
2. **Actions JS Generation**: Use `generate_actions_js.py` to convert backend endpoints into `actions.js`.
3. **YAML Generation**: `actions_parser.py` converts `actions.js` into base YAML forms for the frontend.
4. **UI Decoration**: Use `@ui(...)` to attach additional YAML elements.
5. **Page Generation**: `generate_page_from_ui_tree()` renders HTML pages automatically including CSS files, JS scripts, and UI
   elements.
6. **Runtime Handling**: `runtime.js` handles:

    * Collecting input values
    * Fetching backend endpoints
    * Displaying errors via toast or inline elements
    * Success redirects and dynamic rendering

---

## UI Decorator (`@ui`) and YAML Elements

The `@ui(...)` decorator attaches additional YAML UI elements to endpoints.

```python
from fastapi import APIRouter

from core.ui_decorator import ui

router = APIRouter()


@ui("/static/yaml/header.yaml", "/static/yaml/sidebar.yaml")
@router.post("/login/")
async def login():
    ...
```

* Multiple YAML elements can be attached at once.
* Each YAML element defines `props.position` and `props.layout` to determine its placement and orientation.
* The decorator does **not change API logic**, only extends UI around the central form.

**Example YAML element:**

```yaml
type: container
props:
  position: form-left
  layout: vertical
  style:
    width: 200px
    background-color: "#f5f5f5"
children:
  - type: h3
    props:
      text: "Menu"
  - type: button
    props:
      text: "Dashboard"
```

## Process Overview / Generation Flow

MethodGenerator is designed to automate the creation of a full-stack web application UI and backend integration from API
definitions.
The system handles backend inspection, action extraction, YAML generation, and HTML page creation.
Here is the flow:

### 1. Core System Startup

- `core/main.py` is the main entry point for the system.
- On launch, it starts the three main subsystems:
    - **Backend** (`backend/backend_main.py`) – FastAPI server exposing APIs.
    - **Frontend** (`frontend/frontend_main.py`) – Serves generated HTML pages and static files.
    - **Core** (`core/core_main.py`) – Coordinates inspection, YAML generation, and HTML rendering.

The launcher can start services either in separate consoles (Windows) or in the background with `nohup` (Linux).

---

### 2. API Inspection & Actions Collection

- Core inspects backend services in `backend/services/` to discover API endpoints.
- Each endpoint is analyzed to collect:
    - HTTP method (GET, POST, etc.)
    - URL path
    - Payload fields (from parameters or request body)
- Extracted actions are used to generate a single `actions.js` file, containing all discovered endpoints with metadata.
- Example of collected action:

```json
{
  "auth.login": {
    "method": "POST",
    "url": "/login/",
    "payload": [
      "email",
      "password"
    ]
  }
}
````

---

### 3. YAML Generation

* For each action in `actions.js`, a corresponding YAML file is generated in `frontend/ui_yaml/`.
* The YAML defines the UI form layout for that action (input fields, submit button, etc.).
* This serves as the main “form” structure for the page.
* Additional user interface elements (headers, banners, sidebars) can be attached via the `@ui(...)` decorator.

---

### 4. HTML Page Generation

* The core renders HTML pages from YAML nodes using `generate_node_html.py`.
* Each element automatically includes its corresponding CSS file if available.
* The generated page always keeps the main form at the center.
* Additional elements can be placed relative to the form or the page without breaking the layout.
* Output HTML pages are saved to `frontend/web/pages/`.

---

### 5. Frontend Routing

* HTML pages are automatically registered as routes by the frontend system.
* Each page can be accessed via its route, e.g., `/auth_login` serves `auth_login.html`.
* Static assets (CSS, JS) are served from `frontend/web/static/`.

---

### 6. Running the Full System

* Run `core/main.py` to start backend, frontend, and core simultaneously.
* On Windows, separate consoles open for each subsystem with live output.
* On Linux, `nohup` launches background processes with `.out` log files.

---

## Zones and Layouts

* **Form-relative zones**: `form-top`, `form-bottom`, `form-left`, `form-right`
* **Page-relative zones**: `page-top-left`, `page-top-right`, `page-bottom-left`, `page-bottom-right`
* Each zone supports multiple elements, horizontally or vertically stacked (`props.layout`).
* Allows flexible “wrapping” of the central form with instructions, tips, sidebars, or banners.

---

## CSS and Styling

* Each element type can have a corresponding CSS file (`button.css`, `ui_error_toast.css`, etc.).
* CSS files are automatically included if present in `frontend/web/static`.
* Inline styles (`props.style`) and classes (`props.class`) can be used for additional customization.

---

## Runtime Behavior (`runtime.js`)

* Collects values from `data-bind` inputs.
* Calls backend endpoints defined in `actions.js`.
* Displays **error toasts** or messages based on status codes.
* Handles **redirects** on success.
* Updates bound elements dynamically without a page reload.

---

## Extending MethodGenerator

1. **Add new YAML elements**: Create a YAML file with `type`, `props`, and `children`.
2. **Attach with `@ui(...)`**: Any endpoint can include multiple elements.
3. **Add CSS**: Name your CSS file after the element type and place in `frontend/web/static`.

---

## Testing

* **Legacy tests**: Located in `tests/` and `RectPacker/tests/`.
* Test API YAML generation, runtime behavior, UI rendering, and HTML output.
* Example: `ui_runtime_test.py` manually generates actions.js by inspecting FastAPI router.
