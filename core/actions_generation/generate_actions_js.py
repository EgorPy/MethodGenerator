from core.logger import logger

from pathlib import Path
import json


def generate_actions_js(actions: list, output_path: str):
    """
    Generates JS file with an object window.ACTIONS
    """

    actions_dict = {}

    for a in actions:
        redirect = getattr(a, "redirect_on_success", "self") or "self"

        item = {
            "method": a.method,
            "url": a.url,
            "serviceId": a.service_id,
            "payload": a.payload,
            "encoding": a.encoding,
            "redirectOnSuccess": redirect
        }

        actions_dict[a.id] = item

    js_content = f"window.ACTIONS = {json.dumps(actions_dict, indent=4)};"

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(js_content, encoding="utf-8")

    logger.info(f"actions.js generated for {len(actions)} actions")
    logger.info(f"file path: {path}")
