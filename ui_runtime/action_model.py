from typing import List


class ActionModel:
    """
    Action model used by runtime.
    Connects UI bind/action with specific service and FastAPI route.
    """

    def __init__(
            self,
            id_: str,  # unique identifier, example "auth.login"
            service_id: str,  # which service serves for action, example "auth"
            url: str,  # FastAPI path, example "/login/"
            method: str,  # HTTP method, example "POST"
            payload: List[str],  # list of fields, which needs to be put together from UI bind
            encoding: str = "form"  # form or json
    ):
        self.id = id_
        self.service_id = service_id
        self.url = url
        self.method = method
        self.payload = payload
        self.encoding = encoding

    def to_dict(self):
        return {
            "id": self.id,
            "service_id": self.service_id,
            "url": self.url,
            "method": self.method,
            "payload": self.payload,
            "encoding": self.encoding
        }
