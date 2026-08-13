"""Reusable storage operations kept outside the contract definitions.

The submodules are grouped by the layer they operate on:

``store``
    Convenience operations over configured ``StoreAPI`` objects.
``driver``
    Policy-free transfer and materialisation operations over raw drivers.
``workflow``
    Helpers shared by storage workflow models and implementations.

Imports are resolved lazily so API models can depend on a narrow utility
module without importing every Store and driver contract in return.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DEFAULT_COPY_CHUNK_SIZE": (
        "LiuXin_alpha.storage.utils.store",
        "DEFAULT_COPY_CHUNK_SIZE",
    ),
    "compute_digest": ("LiuXin_alpha.storage.utils.store", "compute_digest"),
    "copy": ("LiuXin_alpha.storage.utils.store", "copy"),
    "exists": ("LiuXin_alpha.storage.utils.store", "exists"),
    "get": ("LiuXin_alpha.storage.utils.store", "get"),
    "iter_infos": ("LiuXin_alpha.storage.utils.store", "iter_infos"),
    "move": ("LiuXin_alpha.storage.utils.store", "move"),
    "put": ("LiuXin_alpha.storage.utils.store", "put"),
    "read_bytes": ("LiuXin_alpha.storage.utils.store", "read_bytes"),
    "try_stat": ("LiuXin_alpha.storage.utils.store", "try_stat"),
    "write_bytes": ("LiuXin_alpha.storage.utils.store", "write_bytes"),
    "iter_object_addresses": (
        "LiuXin_alpha.storage.utils.driver",
        "iter_object_addresses",
    ),
    "materialize_object": (
        "LiuXin_alpha.storage.utils.driver",
        "materialize_object",
    ),
    "move_between_drivers": (
        "LiuXin_alpha.storage.utils.driver",
        "move_between_drivers",
    ),
    "put_object": ("LiuXin_alpha.storage.utils.driver", "put_object"),
    "transfer_between_drivers": (
        "LiuXin_alpha.storage.utils.driver",
        "transfer_between_drivers",
    ),
    "write_all": ("LiuXin_alpha.storage.utils.driver", "write_all"),
    "write_object_bytes": (
        "LiuXin_alpha.storage.utils.driver",
        "write_object_bytes",
    ),
    "normalize_archive_path": (
        "LiuXin_alpha.storage.utils.workflow",
        "normalize_archive_path",
    ),
}


__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    """Load one utility only when it is requested.

    Example:
        >>> normalize = __getattr__("normalize_archive_path")
        >>> normalize("/books//novel.epub")
        'books/novel.epub'
    """
    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from exc
    value: Any = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return eager and lazy utility names for interactive discovery.

    Example:
        >>> "materialize_object" in __dir__()
        True
    """
    return sorted({*globals(), *__all__})
