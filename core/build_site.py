from core.actions_generation.generate_actions_js import generate_actions_js
from core.yaml_generation.generate_yaml import generate_yaml_from_actions
from core.html_generation.generate_html import generate_html_from_yaml
from core.registry import get_registered_services
from core.logger import logger

import os

FRONTEND_STATIC = os.path.abspath("frontend/web/static")
UI_YAML_DIR = os.path.abspath("frontend/ui_yaml")
EXTRA_UI_DIR = os.path.join(UI_YAML_DIR, "extra_ui")
FRONTEND_PAGES = os.path.abspath("frontend/web/pages")


def build_site():
    logger.info("Starting site build...")

    # 1. Collect all actions from services
    all_actions = []
    services = get_registered_services()
    for service_id, service in services.items():
        try:
            logger.info(f"Inspecting service '{service_id}'")
            from core.actions_generation.api_inspector import inspect_app
            service_actions = inspect_app(service.app, service_id)
            all_actions.extend(service_actions)
        except Exception as e:
            logger.error(f"Failed to inspect service '{service_id}': {e}")

    logger.info(f"Total actions collected: {len(all_actions)}")

    # 2. Generate actions.js
    actions_js_path = os.path.join(FRONTEND_STATIC, "actions.js")
    generate_actions_js(all_actions, actions_js_path)
    logger.info(f"actions.js generated: {actions_js_path}")

    # 3. Generate YAML from actions.js
    if not os.path.exists(UI_YAML_DIR):
        os.makedirs(UI_YAML_DIR, exist_ok=True)
    generate_yaml_from_actions(actions_js_path)
    logger.info(f"YAML files generated in {UI_YAML_DIR}")

    # 4. Generate HTML from YAML
    if not os.path.exists(FRONTEND_PAGES):
        os.makedirs(FRONTEND_PAGES, exist_ok=True)

    for yaml_file in os.listdir(UI_YAML_DIR):
        if not yaml_file.endswith(".yaml"):
            continue

        yaml_path = os.path.join(UI_YAML_DIR, yaml_file)
        html_name = yaml_file.replace(".yaml", ".html")
        html_path = os.path.join(FRONTEND_PAGES, html_name)

        # Searching extra ui: form auth_login.yaml → extra ui auth_login_decoration.yaml
        base_name = yaml_file.replace(".yaml", "")
        decoration_name = f"{base_name}_decoration.yaml"
        decoration_yaml_path = os.path.join(EXTRA_UI_DIR, decoration_name)
        if not os.path.exists(decoration_yaml_path):
            decoration_yaml_path = None

        try:
            generate_html_from_yaml(yaml_path, html_path, decoration_yaml=decoration_yaml_path)
            logger.info(f"HTML generated: {html_path}")
        except Exception as e:
            logger.error(f"Failed to generate HTML from {yaml_path}: {e}")

    logger.info("Site build completed.")


if __name__ == "__main__":
    build_site()
