""" Client to send requests to service API """

import requests


def send_request(request: str):
    """ Sends requests to service API """

    headers = {}
    data = {}

    requests.post("https://super-api.ai/api", headers=headers, json=data)
