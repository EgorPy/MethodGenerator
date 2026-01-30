from .action_model import ActionModel
from fastapi.routing import APIRoute
from fastapi import FastAPI, params
from core.logger import logger


def is_form_param(p):
    # field_info - это объект fastapi.params.Form / Body / etc
    return isinstance(p.field_info, params.Form)


def inspect_app(app: FastAPI, service_id: str) -> list[ActionModel]:
    """
    Inspection of FastAPI app.
    Returns a list of ActionModel for every POST/GET route with Form/Body parameters.
    """

    actions = []

    logger.info(f"Starting FastAPI app inspection for service '{service_id}'")

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue

        method = list(route.methods)[0] if route.methods else "GET"
        url = route.path
        func_name = route.endpoint.__name__
        action_id = f"{service_id}.{func_name}"

        # Collect payload from body parameters
        payload = [p.name for p in route.dependant.body_params]

        # Determine encoding: 'form' if any default comes from Form(), else 'json'
        encoding = "form" if any(is_form_param(p) for p in route.dependant.body_params) else "json"

        action = ActionModel(
            id_=action_id,
            service_id=service_id,
            url=url,
            method=method,
            payload=payload,
            encoding=encoding
        )
        actions.append(action)
        logger.debug(f"Added Action: {action_id}: {method} {url} with payload {payload}")

    logger.info(f"Inspection complete: found {len(actions)} actions for service '{service_id}'")
    return actions
