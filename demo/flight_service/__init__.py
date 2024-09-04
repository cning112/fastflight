# mypackage/__init__.py

import importlib
import pkgutil

# Automatically import all modules in the package
for module_info in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{__name__}.{module_info.name}")

    # Explicitly store the module in globals() to avoid import optimization removal
    globals()[module_info.name] = module


def load_all(): ...
