window.ACTIONS = {
    "auth.login": {
        "method": "POST",
        "url": "/login/",
        "serviceId": "auth",
        "payload": [],
        "encoding": "form",
        "redirectOnSuccess": "/profile"
    },
    "auth.register": {
        "method": "POST",
        "url": "/register/",
        "serviceId": "auth",
        "payload": [],
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
    },
    "chats.list_chats": {
        "method": "GET",
        "url": "/chats/list",
        "serviceId": "chats",
        "payload": [],
        "encoding": "json",
        "redirectOnSuccess": "self"
    },
    "chats.create_chat": {
        "method": "POST",
        "url": "/chats/create",
        "serviceId": "chats",
        "payload": [
            "title"
        ],
        "encoding": "json",
        "redirectOnSuccess": "self"
    },
    "chats.delete_chat": {
        "method": "POST",
        "url": "/chats/delete",
        "serviceId": "chats",
        "payload": [
            "chat_id"
        ],
        "encoding": "json",
        "redirectOnSuccess": "self"
    }
};