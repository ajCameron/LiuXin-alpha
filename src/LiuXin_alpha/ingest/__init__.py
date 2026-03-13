"""Top-level ingest package.

Ingest owns acquisition/discovery pipelines that feed the library and storage
layers, but are not themselves storage backends.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "register_wget_html_readonly_store_files": "LiuXin_alpha.ingest.remote_html",
    "register_wget_html_readonly_with_database_path": "LiuXin_alpha.ingest.remote_html",
    "register_native_html_readonly_store_files": "LiuXin_alpha.ingest.remote_html",
    "register_native_html_readonly_with_database_path": "LiuXin_alpha.ingest.remote_html",
    "RemoteHtmlRegistrationReport": "LiuXin_alpha.ingest.models",
}

__all__ = list(_EXPORT_MODULES.keys())


def __getattr__(name: str) -> Any:
    if name not in _EXPORT_MODULES:
        raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))
    module = import_module(_EXPORT_MODULES[name])
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
