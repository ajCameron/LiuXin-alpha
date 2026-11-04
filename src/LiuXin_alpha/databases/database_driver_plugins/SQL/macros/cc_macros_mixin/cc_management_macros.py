"""
Manage legacy custom-column definitions, physical tables and deferred deletion.

The mixin updates custom_columns metadata, builds SQLite table/index/trigger/view
scripts and removes mapped objects during deferred cleanup. Datatype/collation
fragments are trusted SQL inputs. Creation, metadata edits, deletion marking and
cleanup preserve borrowed transactions through savepoints and commit only units
they own. Compatibility table creation remains SQLite-specific.
"""

from __future__ import annotations

# Todo: Swap out to our hardened parser
from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    MacroHostState,
    macro_connection,
    macro_operations,
)
import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection, QueryConnection
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin._reads import (
    read_rows,
)
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.databases.normalized_identities import normalize_identity_value
from LiuXin_alpha.databases.transactions import transaction_active, transaction_scope
from LiuXin_alpha.metadata.value_policies import ColumnNormalizationProfile
from LiuXin_alpha.utils.language_tools import plural_singular_mapper


class CustomColumnsManagementMacrosMixin(MacroHostState):
    """
    Supply custom-column definition operations to a database-backed macro host.

    The host provides db.driver, db.driver_wrapper and _macro_connection.
    Construction does not attach a database or load metadata.
    Marking only changes a flag; a later loader must invoke physical cleanup. Table
    creation builds schema but does not create the custom_columns definition row,
    refresh host maps or use the ordered flag.

    For example, mark_custom_column_for_delete(7) sets the definition's flag;
    delete_marked_custom_columns() later removes its physical storage and row.
    """

    def mark_cc_for_delete(self, cc_column_id: int) -> None:
        """
        Set the deferred-deletion flag in an owned unit or borrowed savepoint.

        Bind the definition ID and set custom_column_mark_for_delete to 1. Missing IDs
        are silently ignored by the UPDATE; no row count is checked. The method neither
        drops physical objects nor schedules a restart. Commit, rollback and cache
        behavior preserve the active macro or primary connection's transaction.

        :param cc_column_id: Custom-column definition ID to flag for later cleanup.
        :return: None; the execution result is discarded.
        """
        with self._cc_transaction() as active:
            active.execute(
                "UPDATE custom_columns SET custom_column_mark_for_delete = 1 "
                "WHERE custom_column_id=?",
                (cc_column_id,),
            ).close()

    def set_custom_column_metadata(
        self,
        num: int,
        name: str | None = None,
        label: str | None = None,
        is_editable: bool | None = None,
        display: Mapping[str, Any] | None = None,
        in_table: str | None = None,
        conn: SQLConnection | None = None,
    ) -> bool:
        """Update definition fields in one owned transaction or borrowed savepoint.

        Omitted fields retain their values. The attachment table may be checked
        but cannot be changed without migrating physical storage. Prepare JSON
        and normalized names before issuing one UPDATE. A failure preserves the
        previous definition and any caller-owned writes.
        """
        headings = set(
            self._macro_database.driver_wrapper.get_column_headings("custom_columns")
        )
        values: dict[str, Any] = {}
        for suffix, value in (("name", name), ("label", label)):
            if value is not None:
                field = "custom_column_" + suffix
                values[field] = value
                if field + "_norm" in headings:
                    values[field + "_norm"] = normalize_identity_value(
                        value, ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
                    )
        if is_editable is not None:
            values["custom_column_editable"] = int(is_editable)
        if display is not None:
            values["custom_column_display"] = json.dumps(display)
        with self._cc_transaction(conn) as active:
            if in_table is not None:
                cursor = active.execute(
                    "SELECT custom_column_in_table FROM custom_columns WHERE custom_column_id=?",
                    (num,),
                )
                try:
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                if row is not None and row[0] != in_table:
                    raise ValueError(
                        "Changing a custom column's attachment requires a storage migration"
                    )
            if not values:
                return False
            assignments = ", ".join(f'"{field}"=?' for field in values)
            active.execute(
                f"UPDATE custom_columns SET {assignments} WHERE custom_column_id=?",
                (*values.values(), num),
            ).close()
        return True

    @contextmanager
    def _cc_transaction(
        self, conn: SQLConnection | None = None
    ) -> Iterator[SQLConnection]:
        """Use the active macro connection, preserving any enclosing transaction."""
        connection = macro_connection(self) if conn is None else conn
        with transaction_scope(
            connection, owns_transaction=not transaction_active(connection)
        ) as active:
            yield active

    def delete_marked_custom_columns(self, conn: SQLConnection | None = None) -> None:
        """Select and remove marked definitions/storage in one SQLite transaction.

        Use the macro transaction connection when present, otherwise the primary
        connection. A borrowed transaction remains open and can undo the cleanup.
        """
        with self._cc_transaction(conn) as active:
            cursor = active.execute(
                "SELECT custom_column_id, custom_column_in_table FROM custom_columns "
                "WHERE custom_column_mark_for_delete=1"
            )
            try:
                rows = cursor.fetchall()
            finally:
                cursor.close()
            objects = {
                int(num): self._macro_database.driver_wrapper.custom_table_names(
                    int(num), in_table=in_table
                )
                for num, in_table in rows
            }
            self.preform_cc_column_delete_from_map(objects, conn=active)

    def create_cc_table(
        self,
        normalized: bool,
        # Todo: We can type this.
        datatype: str,
        dt: str,
        table: str,
        link_table: str,
        collate: str,
        in_table: str = "books",
        ordered: bool = False,
        conn: SQLConnection | None = None,
    ) -> None:
        """Create SQLite custom-column tables and helper objects atomically.

        A supplied connection participates through a savepoint when already in a
        transaction, otherwise this call owns its transaction. Without one, use
        the macro transaction owner. Execute each complete generated statement
        individually so SQLite cannot implicitly commit an enclosing transaction.

        The caller registers the custom_columns definition in the same unit.
        Normalized types have value/link tables and tag-browser views; other
        types have one value table. Optional rating views retain the legacy
        schema checks. The ordered argument does not add ordering columns.

        Names and datatype fragments are trusted internal SQL inputs. These
        compatibility DDL statements use SQLite syntax. This method does not
        publish facade maps, commit a borrowed unit or replace connections.

        :param conn: Optional connection shared with definition registration.
        :return: None; failure rolls back this call's DDL statements.
        """
        supplied_conn = conn
        conn = conn if conn is not None else self._macro_database.driver.conn

        in_table_id_col = self._macro_database.driver_wrapper.get_id_column(in_table)

        cc_table = table
        cc_table_col = plural_singular_mapper(cc_table)

        if normalized:
            lt_col = plural_singular_mapper(link_table)

            if datatype == "series":
                s_index = "{lt_col}_extra REAL,".format(lt_col=lt_col)
            else:
                s_index = ""

            # Todo: If multiple nulls do not count towards uniqueness in an index - why does it call a problem when
            #       trying to get a blank copy of a custom row?
            lines = [
                # Create the table to hold the values
                """
                CREATE TABLE {cc_table}(
                    {cc_table_col}_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {cc_table_col}_value {dt} NOT NULL {collate},
                    UNIQUE({cc_table_col}_value));
                """.format(
                    cc_table=cc_table, dt=dt, collate=collate, cc_table_col=cc_table_col
                ),
                "CREATE INDEX {cc_table}_idx ON {cc_table} ({cc_table_col}_value {collate});".format(
                    cc_table=cc_table, collate=collate, cc_table_col=cc_table_col
                ),
                # Create a link table for the value and titles
                """
                CREATE TABLE {lt}(
                    {lt_col}_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {lt_col}_book INTEGER NOT NULL,
                    {lt_col}_value INTEGER NOT NULL,
                    {s_index}
                    UNIQUE({lt_col}_book, {lt_col}_value)
                    );""".format(lt=link_table, s_index=s_index, lt_col=lt_col),
                "CREATE INDEX {lt}_aidx ON {lt} ({lt_col}_value);".format(
                    lt=link_table, lt_col=lt_col
                ),
                "CREATE INDEX {lt}_bidx ON {lt} ({lt_col}_book);".format(
                    lt=link_table, lt_col=lt_col
                ),
                # Todo: Need tests for these triggers
                # Update trigger on left link - the link to the table the cc is in - check the value it's in actually
                # exists
                """
                CREATE TRIGGER fkc_update_{lt}_a
                        BEFORE UPDATE OF {lt_col}_book ON {lt}
                        BEGIN
                            SELECT CASE
                                WHEN (SELECT {in_table_id_col} from {in_table} WHERE {in_table_id_col}=NEW.{lt_col}_book) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
                            END;
                        END;
                """.format(
                    lt=link_table,
                    lt_col=lt_col,
                    in_table=in_table,
                    in_table_id_col=in_table_id_col,
                ),
                # Todo: This seems to be an error in the calibre code - tell the guy - was originally an update of author
                # update triggers for the right link - to the custom column value the table is actually referencing
                #        checks that the
                """
                CREATE TRIGGER fkc_update_{lt}_b
                        BEFORE UPDATE OF value ON {lt}
                        BEGIN
                            SELECT CASE
                                WHEN (SELECT {cc_table_col}_id from {cc_table} WHERE {cc_table_col}_id=NEW.{lt_col}_value) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: value not in {cc_table}')
                            END;
                        END;
                """.format(
                    lt=link_table,
                    lt_col=lt_col,
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                ),
                """
                CREATE TRIGGER fkc_insert_{lt}
                        BEFORE INSERT ON {lt}
                        BEGIN
                            SELECT CASE
                                WHEN (SELECT {in_table_id_col} from {in_table} WHERE {in_table_id_col}=NEW.{lt_col}_book) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
                                WHEN (SELECT {cc_table_col}_id from {cc_table} WHERE {cc_table_col}_id=NEW.{lt_col}_value) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: value not in {cc_table}')
                            END;
                        END;
                """.format(
                    lt=link_table,
                    lt_col=lt_col,
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                    in_table=in_table,
                    in_table_id_col=in_table_id_col,
                ),
                # Todo: Also need triggers to tidy up when books or the linked items are deleted
                # Todo: Not sure why this couldn't just be rolled into the table definitions
                #       Perhaps it's intended to allow you to disable foreign key checking for reloading the database
                """
                CREATE TRIGGER fkc_delete_{lt}
                        AFTER DELETE ON {cc_table}
                        BEGIN
                            DELETE FROM {lt} WHERE {lt_col}_value=OLD.{cc_table_col}_id;
                        END;
                """.format(
                    lt=link_table,
                    lt_col=lt_col,
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                ),
            ]

            # Tag browser helper views (Calibre-style).
            #
            # These views are used by Calibre-style UIs to show counts and (optionally)
            # average ratings for a given normalized custom column table.
            #
            # In FRBR-first databases we may *not* have the Calibre ratings tables yet.
            # If we create views that reference missing tables, later introspection
            # (e.g. PRAGMA table_info(view_name)) will error and break generic tooling
            # like get_blank_row(). Build a safe fallback variant in that case.
            def _has_table(_name: str) -> bool:
                """
                Check whether the enclosing probe connection exposes a named main-schema
                physical table.

                Query sqlite_master with a bound name and LIMIT 1. Views and temporary
                tables do not qualify. Any Exception from execution or fetch returns
                False, causing the caller to use its fallback view definition. The
                helper closes over the connection chosen by create_cc_table and neither
                modifies schema nor closes that connection.

                Example:
                    During normalized-table creation, an absent ratings table makes
                    ``_has_table("ratings")`` return False and selects NULL
                    average-rating views.


                :param _name: Exact table name bound to the sqlite_master query.
                :return: True for a fetched physical-table row; False for no row or a
                    caught exception.
                """
                if conn is None:
                    return False
                try:
                    row = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
                        (_name,),
                    ).fetchone()
                    return row is not None
                except Exception:
                    return False

            supports_ratings = _has_table("book_rating_links") and _has_table("ratings")

            if supports_ratings:
                lines.extend(
                    [
                        """
                        CREATE VIEW tag_browser_{cc_table} AS
                            SELECT
                                {cc_table}.{cc_table_col}_id AS id,
                                {cc_table}.{cc_table_col}_value AS value,
                                (
                                    SELECT COUNT({lt}.{lt_col}_id)
                                    FROM {lt}
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                ) AS count,
                                (
                                    SELECT AVG(r.rating)
                                    FROM {lt}
                                    JOIN book_rating_links AS bl
                                        ON bl.book_rating_link_book_id = {lt}.{lt_col}_book
                                    JOIN ratings AS r
                                        ON r.rating_id = bl.book_rating_link_rating_id
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                      AND r.rating <> 0
                                ) AS avg_rating,
                                {cc_table}.{cc_table_col}_value AS sort
                            FROM {cc_table};
                        """.format(
                            lt=link_table,
                            lt_col=lt_col,
                            cc_table=cc_table,
                            cc_table_col=cc_table_col,
                        ),
                        """
                        CREATE VIEW tag_browser_filtered_{cc_table} AS
                            SELECT
                                {cc_table}.{cc_table_col}_id AS id,
                                {cc_table}.{cc_table_col}_value AS value,
                                (
                                    SELECT COUNT({lt}.{lt_col}_id)
                                    FROM {lt}
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                      AND books_list_filter({lt}.{lt_col}_book)
                                ) AS count,
                                (
                                    SELECT AVG(r.rating)
                                    FROM {lt}
                                    JOIN book_rating_links AS bl
                                        ON bl.book_rating_link_book_id = {lt}.{lt_col}_book
                                    JOIN ratings AS r
                                        ON r.rating_id = bl.book_rating_link_rating_id
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                      AND r.rating <> 0
                                      AND books_list_filter(bl.book_rating_link_book_id)
                                ) AS avg_rating,
                                {cc_table}.{cc_table_col}_value AS sort
                            FROM {cc_table};
                        """.format(
                            lt=link_table,
                            lt_col=lt_col,
                            cc_table=cc_table,
                            cc_table_col=cc_table_col,
                        ),
                    ]
                )
            else:
                # Ratings tables not present: create views with the same shape but
                # with avg_rating set to NULL.
                lines.extend(
                    [
                        """
                        CREATE VIEW tag_browser_{cc_table} AS
                            SELECT
                                {cc_table}.{cc_table_col}_id AS id,
                                {cc_table}.{cc_table_col}_value AS value,
                                (
                                    SELECT COUNT({lt}.{lt_col}_id)
                                    FROM {lt}
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                ) AS count,
                                NULL AS avg_rating,
                                {cc_table}.{cc_table_col}_value AS sort
                            FROM {cc_table};
                        """.format(
                            lt=link_table,
                            lt_col=lt_col,
                            cc_table=cc_table,
                            cc_table_col=cc_table_col,
                        ),
                        """
                        CREATE VIEW tag_browser_filtered_{cc_table} AS
                            SELECT
                                {cc_table}.{cc_table_col}_id AS id,
                                {cc_table}.{cc_table_col}_value AS value,
                                (
                                    SELECT COUNT({lt}.{lt_col}_id)
                                    FROM {lt}
                                    WHERE {lt}.{lt_col}_value = {cc_table}.{cc_table_col}_id
                                      AND books_list_filter({lt}.{lt_col}_book)
                                ) AS count,
                                NULL AS avg_rating,
                                {cc_table}.{cc_table_col}_value AS sort
                            FROM {cc_table};
                        """.format(
                            lt=link_table,
                            lt_col=lt_col,
                            cc_table=cc_table,
                            cc_table_col=cc_table_col,
                        ),
                    ]
                )
        else:
            lines = [
                """
                CREATE TABLE {cc_table}(
                    {cc_table_col}_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    {cc_table_col}_book  INTEGER,
                    {cc_table_col}_value {dt} NOT NULL {collate},
                    UNIQUE({cc_table_col}_book));
                """.format(
                    cc_table=cc_table, cc_table_col=cc_table_col, dt=dt, collate=collate
                ),
                "CREATE INDEX {cc_table}_idx ON {cc_table} ({cc_table_col}_book);".format(
                    cc_table=cc_table, cc_table_col=cc_table_col
                ),
                """
                CREATE TRIGGER fkc_insert_{cc_table}
                        BEFORE INSERT ON {cc_table}
                        BEGIN
                            SELECT CASE
                                WHEN (SELECT {in_table_id_col} from {in_table} WHERE {in_table_id_col}=NEW.{cc_table_col}_book) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
                            END;
                        END;
                """.format(
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                    in_table=in_table,
                    in_table_id_col=in_table_id_col,
                ),
                """
                CREATE TRIGGER fkc_update_{cc_table}
                        BEFORE UPDATE OF {cc_table_col}_book ON {cc_table}
                        BEGIN
                            SELECT CASE
                                WHEN (SELECT {in_table_id_col} from {in_table} WHERE {in_table_id_col}=NEW.{cc_table_col}_book) IS NULL
                                THEN RAISE(ABORT, 'Foreign key violation: book not in books')
                            END;
                        END;
                """.format(
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                    in_table=in_table,
                    in_table_id_col=in_table_id_col,
                ),
            ]

        # Each generated entry is one complete statement, including trigger bodies.
        # executescript would implicitly commit SQLite's enclosing transaction.
        scope = (
            macro_operations(self).transaction()
            if supplied_conn is None
            else transaction_scope(
                supplied_conn, owns_transaction=not transaction_active(supplied_conn)
            )
        )
        with scope as active:
            for statement in lines:
                active.execute(statement).close()

    # Todo: Is num the same as the id? If not, why. If so, why not called? It doesn't seem to be.
    def do_custom_column_delete_by_num(self, num: int) -> None:
        """
        Delete one custom_columns definition row using a one-item binding tuple.

        Forward to the driver wrapper and discard its execution result. This removes
        metadata only; physical value/link tables and helper objects are not dropped by
        this method. Missing IDs are tolerated, and transaction/cache behavior belongs
        to wrapper.execute.

        Example:
            >>> from types import SimpleNamespace
            >>> bindings = []
            >>> macros = CustomColumnsManagementMacrosMixin()
            >>> macros.db = SimpleNamespace(
            ...     driver_wrapper=SimpleNamespace(
            ...         execute=lambda sql, values: bindings.append(values)
            ...     )
            ... )
            >>> macros.do_custom_column_delete_by_num(7)
            >>> bindings
            [(7,)]


        :param num: Custom-column definition ID bound to custom_column_id.
        :return: None; the wrapper result is discarded.
        """
        self._macro_database.driver_wrapper.execute(
            "DELETE FROM custom_columns WHERE custom_column_id=?", (num,)
        )

    def do_custom_column_delete_by_id(self, cc_id: int) -> None:
        """
        Delete one definition row while forwarding the ID as a scalar binding argument.

        This variant passes cc_id directly to wrapper.execute instead of wrapping it in
        a tuple. The shared SQLite direct_execute path adapts bare integers to a one-
        item text tuple; another wrapper may reject scalar bindings. No physical objects
        are dropped, and result/transaction handling is delegated to the wrapper.

        Example:
            >>> from types import SimpleNamespace
            >>> bindings = []
            >>> macros = CustomColumnsManagementMacrosMixin()
            >>> macros.db = SimpleNamespace(
            ...     driver_wrapper=SimpleNamespace(
            ...         execute=lambda sql, values: bindings.append(values)
            ...     )
            ... )
            >>> macros.do_custom_column_delete_by_id(7)
            >>> bindings
            [7]


        :param cc_id: Definition ID passed unchanged as the wrapper's binding argument.
        :return: None; no affected-row count is checked.
        """
        del_stmt = "DELETE FROM custom_columns WHERE custom_column_id=?;"
        self._macro_database.driver_wrapper.execute(del_stmt, cc_id)

    def mark_custom_column_for_delete(self, num: int) -> None:
        """
        Flag one definition for deferred deletion on the active connection.

        Set custom_column_mark_for_delete to 1 using a bound ID. Marking does not
        immediately remove metadata or schema objects; the caller or a later loader must
        run cleanup. Missing IDs are silently ignored. A borrowed transaction is
        preserved; otherwise this operation owns and commits its transaction.

        Example:
            ``macros.mark_custom_column_for_delete(7)`` flags definition 7
            without dropping its value or link table.


        :param num: Custom-column definition ID bound to custom_column_id.
        :return: None.
        """
        self.mark_cc_for_delete(num)

    def get_all_cc_ids_marked_for_delete(
        self, conn: QueryConnection | None = None
    ) -> list[int]:
        """
        Collect IDs whose deferred-deletion flag equals 1 on the selected connection.

        Use the project connection.get extension in its default row mode and take the
        first cell of each row. No ordering, deduplication or integer conversion is
        applied. Other flag values and SQL nulls are excluded. The query does not clear
        the flags or delete any objects.

        Example:
            >>> from types import SimpleNamespace
            >>> conn = SimpleNamespace(get=lambda sql: [(7,), (9,)])
            >>> CustomColumnsManagementMacrosMixin().get_all_cc_ids_marked_for_delete(
            ...     conn=conn
            ... )
            [7, 9]


        :param conn: Optional compatible connection; None selects db.driver.conn. Its
            role and transaction effects are described above.
        :return: A list of returned ID cells, possibly empty, despite the int-only
            annotation.
        """
        conn = conn if conn is not None else self._macro_database.driver.conn

        ids_list = []
        if conn is None:
            raise DatabaseIntegrityError(
                "Custom-column reads have no active connection"
            )
        for record in read_rows(
            conn,
            "SELECT custom_column_id "
            "FROM custom_columns "
            "WHERE custom_column_mark_for_delete=1;",
        ):
            ids_list.append(record[0])
        return ids_list

    def preform_cc_column_delete_from_map(
        self,
        num_table_lt_map: dict[int, tuple[str, str]],
        conn: SQLConnection | None = None,
    ) -> None:
        """Atomically drop mapped SQLite objects and their marked definitions.

        IDs restrict the definition DELETE. Empty mappings do nothing; unmarked
        definitions are rejected before any DDL. Complete statements execute
        individually, so no executescript can commit a borrowed transaction.
        """
        if not num_table_lt_map:
            return
        with self._cc_transaction(conn) as active:
            for num in num_table_lt_map:
                cursor = active.execute(
                    "SELECT custom_column_mark_for_delete FROM custom_columns WHERE custom_column_id=?",
                    (num,),
                )
                try:
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                if row is None or not row[0]:
                    raise ValueError(f"Custom column {num} is not marked for deletion")
            for num, (table, lt) in num_table_lt_map.items():
                objects = (
                    ("INDEX", table + "_idx"),
                    ("INDEX", lt + "_aidx"),
                    ("INDEX", lt + "_bidx"),
                    ("TRIGGER", "fkc_update_" + lt + "_a"),
                    ("TRIGGER", "fkc_update_" + lt + "_b"),
                    ("TRIGGER", "fkc_insert_" + lt),
                    ("TRIGGER", "fkc_delete_" + lt),
                    ("TRIGGER", "fkc_insert_" + table),
                    ("TRIGGER", "fkc_delete_" + table),
                    ("VIEW", "tag_browser_" + table),
                    ("VIEW", "tag_browser_filtered_" + table),
                    ("TABLE", table),
                    ("TABLE", lt),
                )
                for kind, name in objects:
                    quoted = '"' + name.replace('"', '""') + '"'
                    active.execute(f"DROP {kind} IF EXISTS {quoted}").close()
                active.execute(
                    "DELETE FROM custom_columns WHERE custom_column_id=? AND custom_column_mark_for_delete=1",
                    (num,),
                ).close()
