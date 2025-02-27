import importlib
import pkgutil

# Automatically import all submodules and populate __all__
__all__ = []

for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    __all__.append(module_name)
    importlib.import_module(f"{__name__}.{module_name}")


def load_all():
    """
    Load all FastFlight data services.

    This function will automatically discover all installed data services by
    searching for submodules in the `fastflight.data_services` package. All
    discovered modules will be imported and registered.

    This function is intended to be used in the user's startup code, e.g. in a
    main() function or in a __init__.py file.

    Example:

    >>> from fastflight.data_services import load_all
    >>> load_all()

    :return: None
    """
    pass
