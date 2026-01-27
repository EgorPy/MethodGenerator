window.ACTIONS = {
    "auth.login": {
        "method": "POST",
        "url": "/auth/login/",
        "service_id": "auth",
        "payload": [
            "email",
            "password"
        ],
        "encoding": "json"
    },
    "auth.register": {
        "method": "POST",
        "url": "/auth/register/",
        "service_id": "auth",
        "payload": [
            "first_name",
            "last_name",
            "phone",
            "email",
            "password"
        ],
        "encoding": "json"
    },
    "auth.logout": {
        "method": "GET",
        "url": "/auth/logout/",
        "service_id": "auth",
        "payload": [],
        "encoding": "json"
    },
    "auth.get_me": {
        "method": "GET",
        "url": "/auth/me",
        "service_id": "auth",
        "payload": [],
        "encoding": "json"
    }
};