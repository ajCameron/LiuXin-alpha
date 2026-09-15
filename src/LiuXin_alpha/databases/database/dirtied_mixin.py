
"""
Track dirty-record notifications in memory, telemetry and an optional persistence table.

Telemetry observes attempted queue/trigger activity, not a durable write ledger. Queue insertion, telemetry and SQL persistence have separate failure boundaries. Persistence belongs on the thread that owns the database connection.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from numbers import Number

from collections import Counter, deque
from typing import Any, Optional, TYPE_CHECKING

from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class DatabaseWriteTelemetry:
    """
    Maintain locked event counters and a bounded recent-event history.

    Counters cover all observed events while the deque retains only recent ones. Row IDs are retained by reference; this recorder neither reads the database nor establishes transaction success.

    Example:
        >>> telemetry = DatabaseWriteTelemetry()
        >>> telemetry.record_event(source="import", table="works", row_id=1)
        >>> telemetry.snapshot()["observed_total"]
        1
    """

    def __init__(self, *, max_recent_events: int = 200) -> None:
        """
        Initialize empty counters and a bounded event deque.

        Example:
            >>> DatabaseWriteTelemetry(max_recent_events=20).snapshot()["observed_total"]
            0


        :param max_recent_events: Requested history capacity, converted to int and clamped to at least 20.
        :return: None.
        :raises ValueError: The capacity cannot be converted to int.
        """
        self._lock = threading.Lock()
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=max(20, int(max_recent_events)))
        self._source_counts: Counter[str] = Counter()
        self._table_counts: Counter[str] = Counter()
        self._observed_total = 0

    def record_event(
        self,
        *,
        source: str,
        table: str,
        row_id: object,
        reason: str = "",
    ) -> None:
        """
        Normalize event labels, then increment counters and append its timestamped record.

        Label conversion and timestamp creation precede the lock. Counter/history mutation is protected together; recording does not verify a database write occurred.

        Example:
            >>> telemetry = DatabaseWriteTelemetry()
            >>> telemetry.record_event(source=" ", table=" works ", row_id=7)
            >>> telemetry.snapshot()["source_counts"]
            {'unknown': 1}


        :param source: Source label; blank or false values become unknown.
        :param table: Table label; blank names are omitted from per-table counts.
        :param row_id: Row identifier retained without conversion or copying.
        :param reason: Optional reason label, stripped after string conversion.
        :return: None.
        """
        event = {
            "timestamp": float(time.time()),
            "source": str(source or "").strip() or "unknown",
            "table": str(table or "").strip(),
            "row_id": row_id,
            "reason": str(reason or "").strip(),
        }
        with self._lock:
            self._observed_total += 1
            self._source_counts[event["source"]] += 1
            if event["table"]:
                self._table_counts[event["table"]] += 1
            self._recent_events.append(event)

    def snapshot(
        self,
        *,
        queue_size: int = 0,
        persisted_queue_size: int = 0,
        recent_limit: int = 8,
    ) -> dict[str, Any]:
        """
        Copy telemetry counters and recent events while holding the recorder lock.

        Events are ordered oldest to newest within the chosen tail. Copies are shallow; mutable row_id objects remain shared. Supplied queue counts are not sampled atomically with counters.

        Example:
            >>> telemetry = DatabaseWriteTelemetry()
            >>> telemetry.snapshot(queue_size=3)["queue_size"]
            3


        :param queue_size: Caller-supplied in-memory queue count, converted to int.
        :param persisted_queue_size: Caller-supplied persisted queue count, converted to int.
        :param recent_limit: Number of most recent retained events, clamped to at least one.
        :return: Dictionary containing counters, supplied queue counts and copied event dictionaries.
        """
        with self._lock:
            recent = list(self._recent_events)[-max(1, int(recent_limit)) :]
            return {
                "observed_total": int(self._observed_total),
                "queue_size": int(queue_size),
                "persisted_queue_size": int(persisted_queue_size),
                "source_counts": dict(self._source_counts),
                "table_counts": dict(self._table_counts),
                "recent_events": [dict(event) for event in recent],
            }


class ObservedDirtyRecordsQueue(queue.Queue):
    """
    Observe successful Queue insertion without changing the queued item.

    Only tuple items are decoded as table, row ID and optional reason for telemetry. Recording occurs after insertion, so telemetry failure can propagate even though the item is already queued.

    Example:
        >>> telemetry = DatabaseWriteTelemetry()
        >>> events = ObservedDirtyRecordsQueue(telemetry=telemetry)
        >>> events.put(("works", 4, "edit"))
        >>> events.get_nowait()
        ('works', 4, 'edit')
    """

    def __init__(self, *args, telemetry: Optional[DatabaseWriteTelemetry] = None, **kwargs) -> None:
        """
        Initialize the standard queue and retain an optional event recorder.

        Example:
            >>> events = ObservedDirtyRecordsQueue(maxsize=1)
            >>> events.maxsize
            1


        :param args: Positional arguments forwarded to queue.Queue.
        :param telemetry: Recorder used after each successful insertion, or None.
        :param kwargs: Keyword arguments forwarded to queue.Queue.
        :return: None.
        """
        super().__init__(*args, **kwargs)
        self._telemetry = telemetry

    def put(
            self,
            item: Any,
            block: bool = True,
            timeout: Optional["Number"] = None) -> None:  # noqa: ANN001 - queue API compatibility
        """
        Insert an item using Queue blocking rules, then record dirty_queue telemetry.

        A tuple supplies table, row ID and optional reason; other objects produce blank telemetry fields. Tuple conversion and recorder exceptions occur after insertion, with no removal on failure.

        Example:
            >>> events = ObservedDirtyRecordsQueue()
            >>> events.put(("works", 1), block=False)
            >>> events.get_nowait()
            ('works', 1)


        :param item: Object retained unchanged in the queue.
        :param block: Wait for capacity when True.
        :param timeout: Optional Queue timeout in seconds.
        :return: None.
        :raises queue.Full: Capacity is unavailable under the requested blocking/timeout policy.
        """
        super().put(item, block=block, timeout=timeout)
        telemetry = self._telemetry
        if telemetry is None:
            return
        table = ""
        row_id = None
        reason = ""
        if isinstance(item, tuple):
            if len(item) >= 1:
                table = str(item[0] or "")
            if len(item) >= 2:
                row_id = item[1]
            if len(item) >= 3:
                reason = str(item[2] or "")

        telemetry.record_event(
            source="dirty_queue",
            table=table,
            row_id=row_id,
            reason=reason,
        )


class TelemetryMaintainerProxy:
    """
    Record maintenance callbacks before forwarding them to the target.

    Target arguments and return values are preserved. Telemetry failure prevents delegation, while target failure leaves the already recorded observation intact. Other attributes are read directly from the target.

    Example:
        With an initialized maintainer and recorder, proxy = TelemetryMaintainerProxy(maintainer, telemetry) observes callback calls without owning a worker thread.
    """

    def __init__(self, target, telemetry: Optional[DatabaseWriteTelemetry]) -> None:
        """
        Retain the maintenance target and optional recorder by reference.

        Example:
            During facade assembly, TelemetryMaintainerProxy(maintainer, telemetry) wraps the newly created maintenance service.


        :param target: Object implementing maintenance callbacks.
        :param telemetry: Event recorder, or None to delegate without observations.
        :return: None.
        """
        self._target = target
        self._telemetry = telemetry

    def dirty_record(self, table: str, row_id: int) -> None:  # noqa: ANN001 - callback compatibility
        """
        Observe a DIRTY_RECORD callback, then forward its original arguments.

        Example:
            With a configured proxy, proxy.dirty_record("works", 7) records trigger_dirty_record telemetry before invoking the maintainer.


        :param table: Affected table passed unchanged to the target.
        :param row_id: Affected row ID passed unchanged to the target.
        :return: Target callback result, normally None.
        """
        telemetry = self._telemetry
        if telemetry is not None:
            telemetry.record_event(
                source="trigger_dirty_record",
                table=str(table or ""),
                row_id=row_id,
                reason="DIRTY_RECORD",
            )
        return self._target.dirty_record(table, row_id)

    def new_dirty_record(self, table: str, row_id: int) -> None:  # noqa: ANN001 - callback compatibility
        """
        Observe a NEW_DIRTY_RECORD callback, then forward its original arguments.

        Example:
            With a configured proxy, proxy.new_dirty_record("works", 7) records trigger_new_dirty_record telemetry before delegation.


        :param table: Affected table.
        :param row_id: Affected row ID.
        :return: Target callback result, normally None.
        """
        telemetry = self._telemetry
        if telemetry is not None:
            telemetry.record_event(
                source="trigger_new_dirty_record",
                table=str(table or ""),
                row_id=row_id,
                reason="NEW_DIRTY_RECORD",
            )
        return self._target.new_dirty_record(table, row_id)

    def dirty_interlink_record(
            self,
            update_type: str,
            table1: str,
            table2: str,
            table1_id: int,
            table2_id: int) -> None:  # noqa: ANN001
        """
        Observe a relationship callback and forward its five original arguments.

        Example:
            A configured proxy accepts proxy.dirty_interlink_record("add", "agents", "works", 1, 2) and delegates the same endpoint values.


        :param update_type: Update classification included in the telemetry reason.
        :param table1: First endpoint table, also used as the event table.
        :param table2: Second endpoint table included in the reason.
        :param table1_id: First endpoint ID, also used as the event row ID.
        :param table2_id: Second endpoint ID included in the reason.
        :return: Target callback result, normally None.
        """
        telemetry = self._telemetry
        if telemetry is not None:
            telemetry.record_event(
                source="trigger_interlink",
                table=str(table1 or ""),
                row_id=table1_id,
                reason="{} {}:{} -> {}:{}".format(
                    str(update_type or "").strip() or "interlink",
                    str(table1 or "").strip() or "<unknown>",
                    table1_id,
                    str(table2 or "").strip() or "<unknown>",
                    table2_id,
                ),
            )
        return self._target.dirty_interlink_record(update_type, table1, table2, table1_id, table2_id)

    def __getattr__(self, name: str):
        """
        Resolve an attribute absent on the proxy from its maintenance target.

        Example:
            >>> from types import SimpleNamespace
            >>> proxy = TelemetryMaintainerProxy(SimpleNamespace(label="worker"), None)
            >>> proxy.label
            'worker'


        :param name: Attribute name to read.
        :return: The target attribute, including bound methods.
        :raises AttributeError: The target also lacks the requested attribute.
        """

        return getattr(self._target, name)


class DatabaseDirtiedRecordsMixin:
    """
    Expose dirty-event queue counts, observations and best-effort persistence.

    Requires a facade queue, schema categories and wrapper. Dirty events describe table/row/reason triples rather than deduplicated rows. Memory and persistence operations are separate; callers must coordinate the persistence thread.

    Example:
        For an open db, db.dirty_record("works", row_id, reason="edit") queues an event for later db.persist_dirtied_records().
    """
    # ------------------------------------------------------------------------------------------------------------------
    # Dirtied-record tracking (queue + optional persistence)
    # ------------------------------------------------------------------------------------------------------------------
    @property
    def metadata_dirtied_table(self) -> str:
        """
        Return the configured persistence-table name or its historical default.

        The fallback applies only when the attribute is absent; an explicitly assigned None is returned unchanged.

        Example:
            >>> DatabaseDirtiedRecordsMixin().metadata_dirtied_table
            'metadata_dirtied_books'


        :return: Configured _metadata_dirtied_table value, default metadata_dirtied_books.
        """
        return getattr(self, "_metadata_dirtied_table", "metadata_dirtied_books")

    def get_dirtied_count(self: "DatabaseAPI", *, include_persisted: bool = False) -> int:
        """
        Count current memory events and optionally add the persisted row count.

        A None queue contributes zero. Queue and SQL counts are sampled separately; concurrent changes can make the combined result inconsistent.

        Example:
            For an open db, db.get_dirtied_count(include_persisted=True) includes both observed queue sizes.


        :param include_persisted: Include the persistence helper count when True.
        :return: Approximate event count, without deduplication.
        """
        q = self.dirty_records_queue.qsize() if self.dirty_records_queue is not None else 0
        if include_persisted:
            q += self.get_persisted_dirtied_count()
        return q

    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8) -> dict[str, Any]:
        """
        Combine recorder observations with current memory and persisted queue counts.

        Persisted counting is best effort and may report zero on a query failure. Counts are collected outside the recorder lock.

        Example:
            For an open db, snapshot = db.get_write_telemetry_snapshot(recent_limit=5) provides counters and up to five retained events.


        :param recent_limit: Tail size passed to the recorder when one is present.
        :return: Telemetry dictionary, with zero event counters and an empty history if no recorder is attached.
        """
        telemetry = getattr(self, "write_telemetry", None)
        if telemetry is None:
            return {
                "observed_total": 0,
                "queue_size": self.get_dirtied_count(include_persisted=False),
                "persisted_queue_size": self.get_persisted_dirtied_count(),
                "source_counts": {},
                "table_counts": {},
                "recent_events": [],
            }
        return telemetry.snapshot(
            queue_size=self.get_dirtied_count(include_persisted=False),
            persisted_queue_size=self.get_persisted_dirtied_count(),
            recent_limit=recent_limit,
        )

    def dirty_record(self: "DatabaseAPI", table: str, row_id: int, reason: str = "") -> None:
        """
        Queue a table/row/reason event only for a recognized dirtiable table.

        If dirtiable_tables is None, attempt metadata refresh and suppress its ordinary errors before checking again. This method does not persist events or directly enqueue maintenance callbacks.

        Example:
            For a known dirtiable works table, db.dirty_record("works", 12, reason="metadata") puts that triple on the shared dirty queue.


        :param table: Table name checked against the cached dirtiable set.
        :param row_id: Row ID queued unchanged.
        :param reason: Reason string queued unchanged.
        :return: None; unknown tables log a warning and are ignored.
        """
        if self.dirtiable_tables is None:
            # Defensive: refresh metadata if this is called very early in init.
            try:
                self.refresh_db_metadata()
            except Exception:
                pass

        if self.dirtiable_tables is None or table not in self.dirtiable_tables:
            wrn_str = "Unable to dirty record - table not found.\n"
            default_log.log_variables(
                wrn_str,
                "WARNING",
                ("table", table),
                ("row_id", row_id),
                ("reason", reason),
            )
            return

        self.dirty_records_queue.put((table, row_id, reason))

    def get_persisted_dirtied_count(self: "DatabaseAPI") -> int:
        """
        Best-effort count of rows in the configured persistence helper table.

        A zero result does not distinguish an empty table from a database error.

        Example:
            For an open db, db.get_persisted_dirtied_count() counts stored events without draining the memory queue.


        :return: Row count, or zero when the table/cache is unavailable or querying fails.
        """
        table = self.metadata_dirtied_table
        if getattr(self, "all_tables", None) is None or table not in self.all_tables:
            return 0
        try:
            cur = self.driver_wrapper.execute(f"SELECT COUNT(*) FROM `{table}`")
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def persist_dirtied_records(self: "DatabaseAPI", *, limit: Optional[int] = None) -> int:
        """
        Drain queued triples into the available legacy persistence columns.

        Discover known ID/table/row/reason column aliases before draining and generate UUID primary keys. No explicit commit or task_done calls occur here. On executemany failure, best-effort requeue is possible only when table and row columns are available; partial SQL writes may coexist with requeued events. Malformed tuples stop draining after consumption, and row-ID conversion failures can escape after earlier events were removed. This is not a lossless transactional queue.

        Example:
            On the database-owning thread, written = db.persist_dirtied_records(limit=100) submits up to 100 queued events; backend transaction policy still determines durability.


        :param limit: Maximum number of events to drain, or None for all currently available; nonpositive values drain none.
        :return: Number of rows submitted successfully, or zero for unavailable schema, no events or insertion failure.
        """
        table = self.metadata_dirtied_table
        if getattr(self, "all_tables", None) is None or table not in self.all_tables:
            return 0

        # Discover which columns exist (legacy schemas vary).
        try:
            headings = set(self.driver_wrapper.get_column_headings(table))
        except Exception:
            return 0

        id_col = None
        for cand in ("metadata_dirtied_id", "metadata_dirtied_book_id", "metadata_dirtied_record_id"):
            if cand in headings:
                id_col = cand
                break
        if id_col is None:
            # Can't safely persist without a primary key column.
            return 0

        table_col = None
        for cand in ("metadata_dirtied_table", "metadata_dirtied_table_name"):
            if cand in headings:
                table_col = cand
                break

        row_id_col = None
        for cand in ("metadata_dirtied_table_id", "metadata_dirtied_book", "metadata_dirtied_row_id"):
            if cand in headings:
                row_id_col = cand
                break

        reason_col = None
        for cand in ("metadata_drtied_reason", "metadata_dirtied_reason"):
            if cand in headings:
                reason_col = cand
                break

        cols = [id_col]
        if table_col:
            cols.append(table_col)
        if row_id_col:
            cols.append(row_id_col)
        if reason_col:
            cols.append(reason_col)

        col_sql = ", ".join(f"`{c}`" for c in cols)
        ph_sql = ", ".join(["?"] * len(cols))
        stmt = f"INSERT INTO `{table}` ({col_sql}) VALUES ({ph_sql})"

        values = []
        persisted = 0
        while True:
            if limit is not None and persisted >= int(limit):
                break
            try:
                tname, rid, rsn = self.dirty_records_queue.get_nowait()
            except Exception:
                break

            row = [uuid.uuid4().hex]
            if table_col:
                row.append(tname)
            if row_id_col:
                row.append(int(rid))
            if reason_col:
                row.append(str(rsn))
            values.append(tuple(row))
            persisted += 1

        if not values:
            return 0

        try:
            self.driver_wrapper.executemany(stmt, values)
        except Exception:
            # Best-effort: if persistence fails, re-queue the drained items to avoid silent loss.
            try:
                for v in values:
                    # v layout: (id, table?, row_id?, reason?)
                    vi = 1
                    tname = v[vi] if table_col else None
                    if table_col:
                        vi += 1
                    rid = v[vi] if row_id_col else None
                    if row_id_col:
                        vi += 1
                    rsn = v[vi] if reason_col else ""
                    if tname is not None and rid is not None:
                        self.dirty_records_queue.put((tname, int(rid), str(rsn)))
            except Exception:
                pass
            return 0

        return persisted

    #
    # ----------------------------------------------------------------------------------------------------------------------
