"""
Expose envelope clients, target-specific compatibility proxies, and explicit job-operation contracts.

Local proxies call the in-process runtime directly.
Remote proxies call the daemon's HTTP command/query and event transports.
Constructing a remote proxy configures its endpoint but does not start a server.
CoreProxy names remain aliases of their corresponding envelope-level clients.
"""

from __future__ import annotations

from .jobs import JobStatesArg, JobsProxyABC, JobsProxyAPI
from .local import (
    LocalCoreClient,
    LocalCoreProxy,
    LocalDatabaseProxy,
    LocalJobsProxy,
    LocalLibraryProxy,
    LocalStorageProxy,
)
from .remote import (
    RemoteCoreClient,
    RemoteCoreProxy,
    RemoteDatabaseProxy,
    RemoteJobsProxy,
    RemoteLibraryProxy,
    RemoteProxyError,
    RemoteStorageProxy,
)

__all__ = [
    "JobStatesArg",
    "JobsProxyAPI",
    "JobsProxyABC",
    "LocalCoreClient",
    "LocalCoreProxy",
    "LocalLibraryProxy",
    "LocalDatabaseProxy",
    "LocalStorageProxy",
    "LocalJobsProxy",
    "RemoteCoreClient",
    "RemoteCoreProxy",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
    "RemoteJobsProxy",
    "RemoteProxyError",
]
