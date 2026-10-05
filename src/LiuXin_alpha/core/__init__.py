"""
Expose the public Core runtime, direct/RPC clients, envelopes, event records, and transport helpers.

Use ``create_core`` to compose a runtime and ``core_client`` to select direct or
remote access through the shared client contract. Imports make those entry points
available; they do not create a library/runtime or start an HTTP daemon. Legacy
target-specific proxy names remain exported for compatibility.
"""

from __future__ import annotations

from .api import CoreAPI, CoreClientAPI
from .application_api import CoreApplicationAPI
from .commands import CoreCommand, CoreCommandResult
from .description import (
    CoreEndpointDescription,
    CoreMethodDescription,
    CoreParameterDescription,
    CorePayloadFieldDescription,
    CoreTargetDescription,
)
from .events import CoreEvent, make_core_event
from .factory import core_client, create_core
from .proxies import (
    JobStatesArg,
    JobsProxyABC,
    JobsProxyAPI,
    LocalCoreClient,
    LocalCoreProxy,
    LocalDatabaseProxy,
    LocalJobsProxy,
    LocalLibraryProxy,
    LocalStorageProxy,
    RemoteDatabaseProxy,
    RemoteCoreClient,
    RemoteCoreProxy,
    RemoteJobsProxy,
    RemoteLibraryProxy,
    RemoteProxyError,
    RemoteStorageProxy,
)
from .queries import CoreQuery, CoreQueryResult
from .runtime import CoreRuntime
from .services import CoreServiceReconciliationError, CoreServices
from .transport import CoreHttpDaemon
from .wire import CoreWireError, to_wire

__all__ = [
    "CoreRuntime",
    "CoreAPI",
    "CoreClientAPI",
    "CoreApplicationAPI",
    "CoreServices",
    "CoreServiceReconciliationError",
    "CoreWireError",
    "to_wire",
    "create_core",
    "core_client",
    "CoreCommand",
    "CoreCommandResult",
    "CoreEndpointDescription",
    "CoreMethodDescription",
    "CoreParameterDescription",
    "CorePayloadFieldDescription",
    "CoreQuery",
    "CoreQueryResult",
    "CoreEvent",
    "CoreTargetDescription",
    "make_core_event",
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
    "CoreHttpDaemon",
]
