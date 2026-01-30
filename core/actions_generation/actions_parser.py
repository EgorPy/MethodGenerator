""" actions.js to YAML parser """

from typing import Dict
import json
import os

BASE_YAML_DIR = os.path.abspath("frontend/ui_yaml")

DEFAULT_LAYOUT = {
    "type": "container",
    "layout": "vertical",
    "children": []
}


def action_to_yaml(action_id: str, action: Dict) -> Dict:
    children = []

    for field in action.get("payload", []):
        children.append({
            "type": "text_input",
            "bind": field,
            "props": {"placeholder": field}
        })

    children.append({
        "type": "button",
        "action": action_id,
        "props": {"text": "Submit"}
    })

    node = {
        "type": "container",
        "layout": "vertical",
        "children": children
    }

    return node


def generate_yaml_from_actions(actions_js_path: str):
    with open(actions_js_path, "r", encoding="utf-8") as f:
        text = f.read()

    start = text.find("{")
    end = text.rfind("}") + 1
    actions_dict = json.loads(text[start:end])

    os.makedirs(BASE_YAML_DIR, exist_ok=True)

    for action_id, action in actions_dict.items():
        yaml_dict = action_to_yaml(action_id, action)
        yaml_file = os.path.join(BASE_YAML_DIR, f"{action_id.replace('.', '_')}.yaml")

        import yaml
        with open(yaml_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(yaml_dict, f, sort_keys=False, allow_unicode=True)

        print(f"Generated YAML: {yaml_file}")


if __name__ == "__main__":
    generate_yaml_from_actions("../../frontend/web/static/actions.js")
