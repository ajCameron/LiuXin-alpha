"""
Dirty-event queue, persistence and telemetry operations.

Abstract declarations describe the concrete facade conventions; their bodies do not execute database operations. Counts describe events rather than unique changed rows; persistence and queue operations have separate failure boundaries.
"""

from __future__ import annotations

import abc
from typing import Optional, Any


class DatabaseDirtiedRecordsMixinAPI(abc.ABC):
    """
    Declare dirty-event queue, persistence and telemetry operations.

    Implement all abstract members before instantiation. Counts describe events rather than unique changed rows; persistence and queue operations have separate failure boundaries.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseDirtiedRecordsMixinAPI)
        True
    """

    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        """
        Return the configured persistence-table name or its historical default.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The fallback applies only when the attribute is absent; an explicitly assigned None is returned unchanged.

        Example:
            A concrete Database uses metadata_dirtied_books unless its persistence-table name is overridden.


        :return: Configured _metadata_dirtied_table value, default metadata_dirtied_books.
        """

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        """
        Count current memory events and optionally add the persisted row count.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A None queue contributes zero. Queue and SQL counts are sampled separately; concurrent changes can make the combined result inconsistent.

        Example:
            For an open db, db.get_dirtied_count(include_persisted=True) includes both observed queue sizes.


        :param include_persisted: Include the persistence helper count when True.
        :return: Approximate event count, without deduplication.
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        """
        Queue a table/row/reason event only for a recognized dirtiable table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: If dirtiable_tables is None, attempt metadata refresh and suppress its ordinary errors before checking again. This method does not persist events or directly enqueue maintenance callbacks.

        Example:
            For a known dirtiable works table, db.dirty_record("works", 12, reason="metadata") puts that triple on the shared dirty queue.


        :param table: Table name checked against the cached dirtiable set.
        :param row_id: Row ID queued unchanged.
        :param reason: Reason string queued unchanged.
        :return: None; unknown tables log a warning and are ignored.
        """

    # Todo: Counts per table would be good/interesting?

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        """
        Best-effort count of rows in the configured persistence helper table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A zero result does not distinguish an empty table from a database error.

        Example:
            For an open db, db.get_persisted_dirtied_count() counts stored events without draining the memory queue.


        :return: Row count, or zero when the table/cache is unavailable or querying fails.
        """

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        """
        Drain queued triples into the available legacy persistence columns.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Discover known ID/table/row/reason column aliases before draining and generate UUID primary keys. No explicit commit or task_done calls occur here. On executemany failure, best-effort requeue is possible only when table and row columns are available; partial SQL writes may coexist with requeued events. Malformed tuples stop draining after consumption, and row-ID conversion failures can escape after earlier events were removed. This is not a lossless transactional queue.

        Example:
            On the database-owning thread, written = db.persist_dirtied_records(limit=100) submits up to 100 queued events; backend transaction policy still determines durability.


        :param limit: Maximum number of events to drain, or None for all currently available; nonpositive values drain none.
        :return: Number of rows submitted successfully, or zero for unavailable schema, no events or insertion failure.
        """

    @abc.abstractmethod
    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8) -> dict[str, Any]:
        """
        Combine recorder observations with current memory and persisted queue counts.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Persisted counting is best effort and may report zero on a query failure. Counts are collected outside the recorder lock.

        Example:
            For an open db, snapshot = db.get_write_telemetry_snapshot(recent_limit=5) provides counters and up to five retained events.


        :param recent_limit: Tail size passed to the recorder when one is present.
        :return: Telemetry dictionary, with zero event counters and an empty history if no recorder is attached.
        """
