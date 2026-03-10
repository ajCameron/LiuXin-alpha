"""Core proxy entrypoints.

Local proxies call the in-process runtime directly.
Remote proxies are placeholders for the daemon transport phase.
"""

from __future__ import annotations

from .local import LocalDatabaseProxy, LocalLibraryProxy, LocalStorageProxy
from .remote import RemoteDatabaseProxy, RemoteLibraryProxy, RemoteProxyError, RemoteStorageProxy

__all__ = [
    "LocalLibraryProxy",
    "LocalDatabaseProxy",
    "LocalStorageProxy",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
    "RemoteProxyError",
]
