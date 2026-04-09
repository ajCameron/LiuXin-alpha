"""Database maintenance subsystem.

This is the new top-level home for maintenance work inside ``databases``.
It separates:
- the background engine
- the plugin primitives
- builtin maintenance plugins
- legacy helper functions that still need disentangling

``maintenance_bot.py`` remains as a compatibility wrapper so existing imports do
not all have to move in one refactor.
"""

from __future__ import annotations

from LiuXin_alpha.databases.maintenance.builtin_plugins import get_builtin_maintenance_plugins
from LiuXin_alpha.databases.maintenance.engine import MaintenanceCallbackSink, MaintenanceEngine
from LiuXin_alpha.databases.maintenance.events import (
    DirtyInterlinkEvent,
    DirtyRowEvent,
    MaintenanceEvent,
    RenameRequestEvent,
    ShutdownEvent,
    TickEvent,
)
from LiuXin_alpha.databases.maintenance.legacy import (
    clean,
    create_creator_insert_update_trigger,
    direct_create_meta_2_view,
    direct_create_tag_browser_news,
    direct_ensure_creators_sort,
    direct_merge,
    direct_set_original_one_row_creator_sort,
    do_pre_view_startup_tasks,
    do_view_startup_tasks,
    ensure_creators_sort,
    find_duplicates,
    fix_duplicates,
    repoint_intralink_row,
    run_ta_updates,
    ta_trigger,
)
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginContext,
    MaintenancePluginResult,
)
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
    "MaintenancePluginBase",
    "MaintenancePluginContext",
    "MaintenancePluginResult",
    "Maintainer",
    "RenameRequestEvent",
    "ShutdownEvent",
    "TickEvent",
    "clean",
    "create_creator_insert_update_trigger",
    "direct_create_meta_2_view",
    "direct_create_tag_browser_news",
    "direct_ensure_creators_sort",
    "direct_merge",
    "direct_set_original_one_row_creator_sort",
    "do_pre_view_startup_tasks",
    "do_view_startup_tasks",
    "ensure_creators_sort",
    "find_duplicates",
    "fix_duplicates",
    "get_builtin_maintenance_plugins",
    "repoint_intralink_row",
    "run_ta_updates",
    "ta_trigger",
]
