"""
Expose transaction-bound maintenance and the transient callback adapter.

Importing the namespace loads event, plugin and service definitions but
starts no worker. MaintenanceBot is an alias of MaintenanceEngine; constructing
Maintainer starts its engine.
MaintenanceRuntime supplies transaction-bound prompt effects and a durable lazy
journal. MaintenanceEngine retains only the historical transient callback route.
"""

from __future__ import annotations

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.builtin_plugins import (
    get_builtin_maintenance_plugins,
)
from LiuXin_alpha.databases.maintenance.engine import (
    MaintenanceCallbackSink,
    MaintenanceEngine,
)
from LiuXin_alpha.databases.maintenance.errors import MaintenanceFailure
from LiuXin_alpha.databases.maintenance.events import (
    DirtyInterlinkEvent,
    DirtyRowEvent,
    MaintenanceEvent,
    RenameRequestEvent,
    ShutdownEvent,
    TickEvent,
)
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginContext,
    MaintenancePluginResult,
)
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime
from LiuXin_alpha.databases.maintenance.service import Maintainer

# Backwards-compatible alias while older code still expects a thread-shaped name.
MaintenanceBot = MaintenanceEngine

__all__ = [
    "DirtyInterlinkEvent",
    "DirtyRowEvent",
    "MaintenanceBot",
    "MaintenanceCallbackSink",
    "MaintenanceEngine",
    "MaintenanceEvent",
    "MaintenanceMode",
    "MaintenanceRuntime",
    "MaintenanceFailure",
    "MaintenancePluginBase",
    "MaintenancePluginContext",
    "MaintenancePluginResult",
    "Maintainer",
    "RenameRequestEvent",
    "ShutdownEvent",
    "TickEvent",
    "get_builtin_maintenance_plugins",
]
