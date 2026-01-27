from core.logger import logger
from pathlib import Path
import json


def generate_actions_js(actions: list, output_path: str):
    """
    Generates JS file with an object window.ACTIONS
    """
    actions_dict = {a.id: {
        "method": a.method,
        "url": a.url,
        "service_id": a.service_id,
        "payload": a.payload,
        "encoding": a.encoding
    } for a in actions}

    js_content = f"window.ACTIONS = {json.dumps(actions_dict, indent=4)};"

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(js_content, encoding="utf-8")

    logger.info(f"actions.js generated for {len(actions)} actions")
    logger.info(f"File path: {path}")
