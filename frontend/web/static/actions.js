window.ACTIONS = {
    "auth.login": {
        "method": "POST",
        "url": "/login/",
        "serviceId": "auth",
        "payload": [
            "email",
            "password"
        ],
        "encoding": "form",
        "redirectOnSuccess": "/profile"
    },
    "auth.register": {
        "method": "POST",
        "url": "/register/",
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
        "url": "/logout/",
        "serviceId": "auth",
        "payload": [],
        "encoding": "json",
        "redirectOnSuccess": "self"
    },
    "auth.get_me": {
        "method": "GET",
        "url": "/me",
        "serviceId": "auth",
        "payload": [],
        "encoding": "json",
        "redirectOnSuccess": "self"
    }
};