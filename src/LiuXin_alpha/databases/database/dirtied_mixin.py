

import uuid

from typing import Optional

from LiuXin_alpha.utils.logging import default_log


class DatabaseDirtiedRecordsMixin:
    """
    Mixin containing methods dealing with the dirtied records queues.
    """
    # ------------------------------------------------------------------------------------------------------------------
    # Dirtied-record tracking (queue + optional persistence)
    # ------------------------------------------------------------------------------------------------------------------
    @property
    def metadata_dirtied_table(self) -> str:
        """Name of the persistent dirtied-records helper table.

        The name is historic ("..._books") but the contents are generic: it records (table, row_id, reason)
        so a sidecar writer can resume across process restarts.
        """
        return getattr(self, "_metadata_dirtied_table", "metadata_dirtied_books")

    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        """Return the number of queued dirtied-record events.

        By default this reflects the in-memory queue size (fast, thread-safe-ish). If include_persisted is True,
        we add the number of rows already persisted to ``metadata_dirtied_table``.
        """
        q = self.dirty_records_queue.qsize() if self.dirty_records_queue is not None else 0
        if include_persisted:
            q += self.get_persisted_dirtied_count()
        return q

    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        """Enqueue a dirtied-record event for later processing.

        This method is intentionally lightweight: it only enqueues into ``dirty_records_queue`` so callers can
        safely call it from many contexts (including triggers that bounce into Python). Persisting to the database
        is performed separately via :meth:`persist_dirtied_records`.
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

    def get_persisted_dirtied_count(self) -> int:
        """Count rows currently stored in the persistent dirtied table (if present)."""
        table = self.metadata_dirtied_table
        if getattr(self, "all_tables", None) is None or table not in self.all_tables:
            return 0
        try:
            cur = self.driver_wrapper.execute(f"SELECT COUNT(*) FROM `{table}`")
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        """Drain dirtied-record events from the in-memory queue into ``metadata_dirtied_table``.

        This is intended to be called from a single controlling thread (e.g. a maintenance loop) to avoid
        cross-thread SQLite connection use. Returns the number of persisted events.
        """
        table = self.metadata_dirtied_table
        if getattr(self, "all_tables", None) is None or table not in self.all_tables:
            return 0

        # Discover which columns exist (legacy schemas vary).
        try:
            headings = set(self.driver_wrapper.direct_get_column_headings(table))
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
