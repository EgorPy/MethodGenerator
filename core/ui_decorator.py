from functools import wraps
from typing import Callable

UI_REGISTRY: dict[Callable, list[str]] = {}


def ui(*yaml_paths: str):
    """
    Decorator to attach additional YAML UI elements to an API endpoint.
    Each path is a path or URL to a YAML element.

    Example:
        @ui("/static/yaml/header.yaml", "/static/yaml/sidebar.yaml")
        @router.post("/login/")
        async def login(...):
            ...
    """

    def decorator(func: Callable):
        if func not in UI_REGISTRY:
            UI_REGISTRY[func] = []

        UI_REGISTRY[func].extend(yaml_paths)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        # Attach property to function so runtime can fetch attached YAMLs
        wrapper._ui_yaml_paths = UI_REGISTRY[func]

        return wrapper

    return decorator


def get_ui_elements(endpoint_func: Callable) -> list[str]:
    """ Returns a list of YAML paths attached to the given endpoint function. """

    if hasattr(endpoint_func, "_ui_yaml_paths"):
        return getattr(endpoint_func, "_ui_yaml_paths")
    return []
