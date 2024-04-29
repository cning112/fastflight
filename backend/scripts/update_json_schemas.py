import importlib
import inspect
import pkgutil
import site
import sys
from pathlib import Path
from types import ModuleType
from typing import Type

from pydantic import BaseModel

site.addsitedir(str((Path(__file__).parent.parent / "src").resolve()))


def import_submodules(package_name) -> dict[str, ModuleType]:
    package = importlib.import_module(package_name)
    submodules = {}
    # Find all submodules and dynamically import them
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        # Import the submodule
        module = importlib.import_module(name)
        submodules[name] = module
    return submodules


def find_models(module: ModuleType) -> dict[str, Type[BaseModel]]:
    schemas = {}
    classes = inspect.getmembers(module, inspect.isclass)
    for cls_name, cls in classes:
        if issubclass(cls, BaseModel) and cls is not BaseModel:
            schemas[cls_name] = cls
    return schemas


if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "schemas"
    modules = import_submodules("my_fastapi.internal.schemas")
    for mod_name, mod in modules.items():
        for model_name, model in find_models(mod).items():
            try:
                json_str = model.schema_json(indent=2)
                (output_dir / f"{model_name}.json").write_text(json_str)
            except Exception as e:
                print(str(e), file=sys.stderr)
