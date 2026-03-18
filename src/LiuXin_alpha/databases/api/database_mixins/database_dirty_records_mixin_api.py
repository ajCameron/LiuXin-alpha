from __future__ import annotations

import abc
from typing import Optional, Any


class DatabaseDirtiedRecordsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseDirtiedRecordsMixin``.

    This defines the interface for dealing with the dirtied records on the database.
    """

    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        """
        Return the name for the metadata dirty table.

        :return:
        """

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        """
        Get the dirtied record count for the database.

        :param include_persisted:
        :return:
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        """
        Note that a record in a given table has been dirtied.

        :param table:
        :param row_id:
        :param reason:
        :return:
        """

    # Todo: Counts per table would be good/interesting?

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        """
        Get the record count for the records marked persistently dirtied.

        :return:
        """

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        """Drain dirtied-record events from the in-memory queue into ``metadata_dirtied_table``.

        This is intended to be called from a single controlling thread (e.g. a maintenance loop) to avoid
        cross-thread SQLite connection use. Returns the number of persisted events.

        :param limit:
        :return:
        """

    @abc.abstractmethod
    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8) -> dict[str, Any]:
        """
        Return a lightweight live snapshot of observed database write activity.

        :param recent_limit:
        :return:
        """
