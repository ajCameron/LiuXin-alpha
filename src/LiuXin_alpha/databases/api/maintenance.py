"""Maintenance/cache related API contracts."""

from __future__ import annotations

import abc

from typing import Iterable, Optional

try:
    from LiuXin_alpha.utils.text.icu import lower as icu_lower
except Exception:
    icu_lower = str.lower

from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class DatabaseMaintainerAPI(abc.ABC):
    """
    Maintenance interface between database and maintenance thread.
    """

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


class MaintenanceBotAPI(abc.ABC):
    """API contract for the background maintenance bot thread."""

    @abc.abstractmethod
    def __init__(self, db: DatabaseAPI, dirtied_main_queue, dirtied_interlink_queue, interval: int=2, scheduling_interval: float=0.5) -> None:
        ...

    @abc.abstractmethod
    def rename_item(self, item_id: int, table: str, value: bool, now: bool=True, db: Optional[DatabaseAPI]=None) -> None:
        ...

    @abc.abstractmethod
    def run(self):
        ...

    @abc.abstractmethod
    def stop(self) -> None:
        ...
