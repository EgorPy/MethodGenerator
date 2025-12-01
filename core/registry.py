""" Service registry """

import importlib
import pkgutil
import os

TASKS = {}

SERVICES_PATH = os.path.join(os.path.dirname(__file__), "..", "services")
SERVICES_PATH = os.path.abspath(SERVICES_PATH)

# auto import all packages in services/
for module_info in pkgutil.iter_modules([SERVICES_PATH]):
    module_name = module_info.name
    full_path = f"services.{module_name}.service"
    module = importlib.import_module(full_path)

    # every service must provide SERVICE object
    service_obj = module.SERVICE
    TASKS[service_obj.name] = service_obj
