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
    "StoreIngestCheckpointedError": "LiuXin_alpha.ingest.models",
    "StoreIngestFailure": "LiuXin_alpha.ingest.models",
    "StoreIngestItem": "LiuXin_alpha.ingest.models",
    "StoreIngestMode": "LiuXin_alpha.ingest.models",
    "StoreIngestObjectCheckpoint": "LiuXin_alpha.ingest.models",
    "StoreIngestReport": "LiuXin_alpha.ingest.models",
    "StoreIngestInfo": "LiuXin_alpha.ingest.stores",
    "StoreIngestSource": "LiuXin_alpha.ingest.stores",
    "StoreMetadataInput": "LiuXin_alpha.ingest.stores",
    "StorePlacementInput": "LiuXin_alpha.ingest.stores",
    "adopt_store": "LiuXin_alpha.ingest.stores",
    "ingest_store": "LiuXin_alpha.ingest.stores",
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
