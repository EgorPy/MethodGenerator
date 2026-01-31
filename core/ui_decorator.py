from fastapi.responses import JSONResponse

from functools import wraps
from typing import Callable
import yaml
import os

EXTRA_UI_PATH = os.path.join("frontend", "ui_yaml", "extra_ui")

UI_REGISTRY: dict[Callable, list[str]] = {}


def ui(*yaml_names: str):
    """
    Decorator to attach additional YAML UI elements to an API endpoint.

    Usage:
        @ui("login_decoration.yaml", "sidebar.yaml")
        @router.post("/login/")
        async def login(...):
            ...

    The decorator automatically loads the YAML files from the central
    extra_ui folder and makes them available in the response under 'extra_ui'.
    """

    def decorator(func: Callable):
        if func not in UI_REGISTRY:
            UI_REGISTRY[func] = []

        # Store only filenames, full path will be resolved at runtime
        UI_REGISTRY[func].extend(yaml_names)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Call original endpoint
            print(123)
            response = await func(*args, **kwargs)

            # Build extra UI only if endpoint has YAML attached
            if func in UI_REGISTRY:
                extra_ui = build_extra_ui(func)

                # Merge extra_ui into response
                if hasattr(response, "body"):
                    import json
                    try:
                        body_data = json.loads(response.body)
                    except Exception:
                        body_data = {}
                    body_data["extra_ui"] = extra_ui
                    return JSONResponse(content=body_data)

                elif isinstance(response, dict):
                    response["extra_ui"] = extra_ui
                    return response

            return response

        # Attach property for runtime access if needed
        wrapper._ui_yaml_paths = UI_REGISTRY[func]

        return wrapper

    return decorator


def get_ui_elements(endpoint_func: Callable) -> list[str]:
    """ Returns a list of YAML filenames attached to the endpoint function. """

    if hasattr(endpoint_func, "_ui_yaml_paths"):
        return getattr(endpoint_func, "_ui_yaml_paths")
    return []


def build_extra_ui(func: Callable) -> dict:
    """
    Load all attached YAML files for the endpoint and combine them by position.
    Returns dict like:
        {
            "form-top": [...],
            "form-left": [...],
            "page-top-right": [...],
            ...
        }
    """
    ui_elements: dict[str, list[dict]] = {}

    yaml_files = get_ui_elements(func)

    for yaml_name in yaml_files:
        full_path = os.path.join(EXTRA_UI_PATH, yaml_name)

        if not os.path.exists(full_path):
            continue

        with open(full_path, "r", encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
            except Exception:
                continue

        if not data:
            continue

        # If a single element, wrap in a list for uniform processing
        elements = data if isinstance(data, list) else [data]

        for elem in elements:
            pos = elem.get("props", {}).get("position", "form-top")
            if pos not in ui_elements:
                ui_elements[pos] = []
            ui_elements[pos].append(elem)

    return ui_elements
