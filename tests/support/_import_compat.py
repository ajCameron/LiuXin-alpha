from __future__ import annotations

import importlib.util
import inspect
import sys

from functools import lru_cache
from pathlib import Path
from types import ModuleType


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_module_from_repo(module_name: str, relative_path: str) -> ModuleType:
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing

    module_path = _repo_root() / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module {module_name!r} from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def ensure_interfaces_field_metadata_alias() -> ModuleType:
    try:
        import LiuXin_alpha.interfaces.field_metadata as module  # type: ignore
    except ModuleNotFoundError as exc:
        if exc.name != "LiuXin_alpha.interfaces.field_metadata":
            raise
        surfaces_module = _load_module_from_repo(
            "tests._surfaces_field_metadata",
            "src/LiuXin_alpha/surfaces/field_metadata.py",
        )
        alias = ModuleType("LiuXin_alpha.interfaces.field_metadata")
        alias.FieldMetadata = surfaces_module.FieldMetadata
        alias.CalibreFieldMetadata = getattr(surfaces_module, "CalibreFieldMetadata", None)
        alias.calibre_name_to_liuxin_name = surfaces_module.calibre_name_to_liuxin_name
        alias.__file__ = str(_repo_root() / "src/LiuXin_alpha/surfaces/field_metadata.py")
        sys.modules[alias.__name__] = alias
        return alias
    else:
        return module


@lru_cache(maxsize=1)
def load_surfaces_categories_module() -> ModuleType:
    return _load_module_from_repo(
        "tests._surfaces_categories",
        "src/LiuXin_alpha/surfaces/categories.py",
    )


@lru_cache(maxsize=1)
def driver_wrapper_abstract_state() -> tuple[bool, tuple[str, ...]]:
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    abstract_methods = tuple(sorted(getattr(DriverWrapper, "__abstractmethods__", set())))
    return inspect.isabstract(DriverWrapper), abstract_methods
