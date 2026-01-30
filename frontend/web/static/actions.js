window.ACTIONS = {
    "auth.login": {
        "method": "POST",
        "url": "/auth/login/",
        "serviceId": "auth",
        "payload": [
            "email",
            "password"
        ],
        "encoding": "form",
        "redirectOnSuccess": "self"
    },
    "auth.register": {
        "method": "POST",
        "url": "/auth/register/",
        "serviceId": "auth",
        "payload": [
            "first_name",
            "last_name",
            "phone",
            "email",
            "password"
        ],
        "encoding": "form",
        "redirectOnSuccess": "self"
    },
    "auth.logout": {
        "method": "GET",
        "url": "/auth/logout/",
        "serviceId": "auth",
        "payload": [],
        "encoding": "json",
        "redirectOnSuccess": "self"
    },
    "auth.get_me": {
        "method": "GET",
        "url": "/auth/me",
        "serviceId": "auth",
        "payload": [],
        "encoding": "json",
        "redirectOnSuccess": "self"
    }
};