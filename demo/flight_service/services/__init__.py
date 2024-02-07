import importlib
import pkgutil

# Automatically import all submodules and populate __all__
__all__ = []

for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    __all__.append(module_name)
    importlib.import_module(f"{__name__}.{module_name}")
