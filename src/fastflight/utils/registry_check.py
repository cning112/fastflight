import importlib
import inspect
import pkgutil

from fastflight.core.base import BaseDataService, BaseParams


def validate_param_service_binding(param_classes: list[type[BaseParams]]) -> None:
    """
    Ensure all given BaseParams classes are registered to a BaseDataService.

    Args:
        param_classes: list of BaseParams subclasses to check.

    Raises:
        RuntimeError: if any param class is not bound to a service.

    Example:
        >>> class MyParams(BaseParams):
        ...     pass
        >>> validate_param_service_binding([MyParams])  # raises if not bound
    """
    missing = []
    for cls in param_classes:
        fqn = cls.fqn()
        if fqn not in BaseDataService._registry:
            missing.append(fqn)

    if missing:
        raise RuntimeError(f"The following BaseParams are NOT bound to any BaseDataService: {missing}")


def import_all_modules_in_package(package_name: str) -> None:
    """
    Recursively import all modules under the given package name.

    This is useful for ensuring that all parameter/service subclasses are registered,
    since registration often happens at import time.

    Args:
        package_name: The name of the package (e.g., "fastflight.params").

    Example:
        >>> import_all_modules_in_package("fastflight.params")
        >>> # Now all params/services under fastflight.params are imported and registered.
    """
    package = importlib.import_module(package_name)
    for finder, name, ispkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        importlib.import_module(name)


def get_param_service_bindings_from_package(package_name: str) -> dict[str, str]:
    """
    Given a package path as a string, import all modules within that package,
    identify all subclasses of BaseParams defined therein, verify that each
    is properly registered to a BaseDataService, and return a mapping from
    each param class's fully qualified name (FQN) to its bound service class's FQN.

    This function ensures that all parameter classes declared in the specified
    package are correctly bound to services, raising an error if any are unbound.

    Args:
        package_name: The string path of the package to inspect (e.g., "fastflight.params").

    Returns:
        A dictionary mapping param class FQNs to their associated BaseDataService class FQNs.

    Raises:
        RuntimeError: if any discovered param classes are not bound to a service.

    Example:
        >>> bindings = get_param_service_bindings_from_package("fastflight.params")
        >>> print(bindings)
        {'fastflight.params.MyParams': 'fastflight.services.MyService'}
    """
    package = importlib.import_module(package_name)
    param_classes = []
    for _, obj in inspect.getmembers(package):
        if inspect.isclass(obj) and issubclass(obj, BaseParams) and obj is not BaseParams:
            param_classes.append(obj)

    validate_param_service_binding(param_classes)

    bindings = {}
    for cls in param_classes:
        param_fqn = cls.fqn()
        service_cls = BaseDataService.lookup(param_fqn)
        if service_cls:
            bindings[param_fqn] = service_cls.fqn()
    return bindings
