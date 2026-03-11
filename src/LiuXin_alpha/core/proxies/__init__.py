"""Core proxy entrypoints.

Local proxies call the in-process runtime directly.
Remote proxies are placeholders for the daemon transport phase.
"""

from __future__ import annotations

from .jobs import JobStatesArg, JobsProxyABC, JobsProxyAPI
from .local import LocalDatabaseProxy, LocalJobsProxy, LocalLibraryProxy, LocalStorageProxy
from .remote import RemoteDatabaseProxy, RemoteJobsProxy, RemoteLibraryProxy, RemoteProxyError, RemoteStorageProxy

__all__ = [
    "JobStatesArg",
    "JobsProxyAPI",
    "JobsProxyABC",
    "LocalLibraryProxy",
    "LocalDatabaseProxy",
    "LocalStorageProxy",
    "LocalJobsProxy",
    "RemoteLibraryProxy",
    "RemoteDatabaseProxy",
    "RemoteStorageProxy",
    "RemoteJobsProxy",
    "RemoteProxyError",
]
