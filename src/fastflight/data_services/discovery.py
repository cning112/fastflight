import importlib
import inspect
import pkgutil
from typing import Type

from fastflight.data_services import BaseParams


def discover_param_classes(module_path: str) -> dict[str, Type[BaseParams]]:
    """
    Recursively scans the specified module path and returns a dictionary mapping
    fully-qualified class names to classes that are subclasses of BaseParams.
    """
    registry = {}
    module = importlib.import_module(module_path)
    for importer, modname, ispkg in pkgutil.walk_packages(module.__path__, module.__name__ + "."):
        try:
            mod = importlib.import_module(modname)
        except Exception:
            continue
        for name, obj in inspect.getmembers(mod, inspect.isclass):
            if issubclass(obj, BaseParams) and obj is not BaseParams:
                fqcn = f"{obj.__module__}.{obj.__name__}"
                registry[fqcn] = obj
    return registry
