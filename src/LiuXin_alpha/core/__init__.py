"""Core runtime, envelopes, events, and proxy entrypoints."""

from __future__ import annotations

from .commands import CoreCommand, CoreCommandResult
from .events import CoreEvent, make_core_event
from .proxies import (
    LocalDatabaseProxy,
    LocalLibraryProxy,
    LocalStorageProxy,
    RemoteDatabaseProxy,
    RemoteLibraryProxy,
    RemoteProxyError,
    RemoteStorageProxy,
)
from .queries import CoreQuery, CoreQueryResult
from .runtime import CoreRuntime
from .transport import CoreHttpDaemon

__all__ = [
    "CoreRuntime",
    "CoreCommand",
    "CoreCommandResult",
    "CoreQuery",
    "CoreQueryResult",
    "CoreEvent",
    "make_core_event",
    "LocalLibraryProxy",
    "LocalDatabaseProxy",
    "LocalStorageProxy",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
    "RemoteProxyError",
    "CoreHttpDaemon",
]
