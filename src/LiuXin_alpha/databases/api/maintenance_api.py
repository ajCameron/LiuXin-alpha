"""Maintenance-related API contracts."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class DatabaseMaintainerAPI(abc.ABC):
    """Compatibility façade between the database and maintenance services."""

    def __init__(self, db: DatabaseAPI) -> None:
        self.db = db

    @abc.abstractmethod
    def _do_merge_one_table(self, src_table: str, dst_table: str, link_table: str, item_1_id: int, item_2_id: int) -> None:
        ...

    @abc.abstractmethod
    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        ...

    @abc.abstractmethod
    def dirty_interlink_record(self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int) -> None:
        ...

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        ...

    @abc.abstractmethod
    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        ...

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        ...


class MaintenanceCallbackSinkAPI(abc.ABC):
    """Minimal callback surface used by driver UDF hooks."""

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        ...

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        ...

    @abc.abstractmethod
    def dirty_interlink_record(self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int) -> None:
        ...


class MaintenancePluginAPI(abc.ABC):
    """Protocol-ish abstract base for internal maintenance plugins."""

    name: str = "maintenance-plugin"
    priority: int = 0
    enabled_by_default: bool = True

    def startup(self, context: Any) -> None:
        return None

    def shutdown(self, context: Any) -> None:
        return None

    def wants_event(self, event: Any) -> bool:
        return True

    def coalesce_key(self, event: Any) -> object | None:
        return None

    @abc.abstractmethod
    def handle_events(self, context: Any, events: Iterable[Any]) -> Any:
        raise NotImplementedError


class MaintenanceServiceAPI(abc.ABC):
    """Service-shaped maintenance API.

    This is the shape new code should care about. The older ``MaintenanceBotAPI``
    remains below for compatibility with thread-shaped code.
    """

    @abc.abstractmethod
    def register_plugin(self, plugin: MaintenancePluginAPI) -> None:
        ...

    @abc.abstractmethod
    def iter_plugins(self):
        ...

    @abc.abstractmethod
    def run_once(self, *, max_events: int = 128):
        ...

    @abc.abstractmethod
    def stop(self) -> None:
        ...

    @abc.abstractmethod
    def rename_item(self, item_id: int, table: str, value: str, now: bool = True, db: Optional[DatabaseAPI] = None) -> None:
        ...


class MaintenanceBotAPI(MaintenanceServiceAPI):
    """Compatibility contract for the background maintenance thread/service."""

    @abc.abstractmethod
    def __init__(self, db: DatabaseAPI, dirtied_main_queue=None, dirtied_interlink_queue=None, interval: int = 2, scheduling_interval: float = 0.5) -> None:
        ...

    @abc.abstractmethod
    def run(self):
        ...
