"""
Service object for Core API inspection.
Contains router from auth.py for Core to scan actions.
"""

from types import SimpleNamespace
from backend.services.auth.api.auth import router

SERVICE = SimpleNamespace(
    name="auth",
    app=router  # router is used by API inspector from core/actions_generation/api_inspector.py
)
