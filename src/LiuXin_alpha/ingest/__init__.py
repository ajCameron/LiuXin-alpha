"""
Expose acquisition reports and ingest entry points through lazy compatibility exports.

Ingest owns discovery and acquisition pipelines, not storage backends. Public
names import their owning models, Store ingest, or remote-HTML module on first
access and are cached here. Importing this package alone does not construct
services or start discovery; wildcard imports resolve all declared exports.
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
    """
    Resolve a declared export and cache the owner's exact object in this module.

    Normal attribute access bypasses this hook once cached. A direct call still
    resolves the owner again; import/getattr errors propagate without caching.

    Example:
        >>> __getattr__('StoreIngestMode').COPY.value
        'copy'


    :param name: Exact public name in the lazy-export table.
    :return: Original owner attribute, also stored in the package globals.
    :raises AttributeError: The name is not exported or absent from its owner.
    """
    if name not in _EXPORT_MODULES:
        raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))
    module = import_module(_EXPORT_MODULES[name])
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """
    List current globals and declared exports without resolving lazy imports.

    Names already cached in globals appear twice because the concatenation is
    sorted, not deduplicated. Private names are included.

    Example:
        >>> 'ingest_store' in __dir__()
        True


    :return: Sorted name list, potentially containing duplicates.
    """
    return sorted(list(globals().keys()) + __all__)
