""" Service to check and send requests """

from service_client import send_request

while True:
    request = db.get_first_pending_request()
    if request:
        response = send_request(request)
    db.save_response(response)
