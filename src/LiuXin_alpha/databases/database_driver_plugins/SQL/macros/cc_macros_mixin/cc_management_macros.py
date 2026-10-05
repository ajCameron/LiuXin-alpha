
"""
Manage legacy custom-column definitions, physical tables and deferred deletion.

The mixin updates custom_columns metadata, builds SQLite table/index/trigger/view
scripts and removes mapped objects during deferred cleanup. Schema names and
datatype/collation fragments are trusted SQL inputs. Execution is split between host
execute, wrapper methods, explicit connections and driver script execution; these
helpers do not establish a shared atomic boundary. Some methods explicitly commit even a
caller-supplied connection.
"""

from __future__ import annotations

# Todo: Swap out to our hardened parser
import json

from typing import Optional, TYPE_CHECKING

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.utils.language_tools import plural_singular_mapper
from LiuXin_alpha.databases.column_metadata import ColumnNormalizationProfile
from LiuXin_alpha.databases.normalized_identities import normalize_identity_value

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class CustomColumnsManagementMacrosMixin:
    """
    Supply custom-column definition operations to a database-backed macro host.

    The host provides db.driver, db.driver_wrapper and an execute callable for
    mark_cc_for_delete. Construction does not attach a database or load metadata.
    Marking only changes a flag; a later loader must invoke physical cleanup. Table
    creation builds schema but does not create the custom_columns definition row,
    refresh host maps or use the ordered flag.

    Example:
        >>> from types import SimpleNamespace
        >>> recorded = []
        >>> macros = CustomColumnsManagementMacrosMixin()
        >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(execute=lambda sql, values: recorded.append(values)))
        >>> macros.mark_custom_column_for_delete(7)
        >>> recorded
        [(7,)]
    """

    db: "DatabaseAPI"

    def mark_cc_for_delete(self, cc_column_id: int) -> None:
        """
        Set the deferred-deletion flag for one definition through the host execute
        operation.

        Bind the definition ID and set custom_column_mark_for_delete to 1. Missing IDs
        are silently ignored by the UPDATE; no row count is checked. The method neither
        drops physical objects nor schedules a restart. Commit, rollback and cache
        behavior belong to self.execute.

        Example:
            >>> recorded = []
            >>> macros = CustomColumnsManagementMacrosMixin()
            >>> macros.execute = lambda sql, values: recorded.append(values)
            >>> macros.mark_cc_for_delete(7)
            >>> recorded
            [(7,)]


        :param cc_column_id: Custom-column definition ID to flag for later cleanup.
        :return: None; the execution result is discarded.
        """
        self.execute(
            "UPDATE custom_columns " "SET custom_column_mark_for_delete = 1 " "WHERE custom_column_id=?",
            (cc_column_id,),
        )

    def set_custom_column_metadata(
        self,
        num: int,
        name: Optional[str] = None,
        label: Optional[str] = None,
        is_editable: Optional[bool] = None,
        display: Optional[str] = None,
        in_table: Optional[str] = None,
        # Todo: Need the conn protocol
        conn = None,
    ) -> bool:
        """
        Write supplied definition fields and commit when at least one field is
        requested.

        Resolve the connection and always inspect custom_columns headings through the
        host wrapper, even for an explicit connection or a call with no updates. Process
        non-None fields in name, label, editable, display and attachment-table order. If
        the corresponding name_norm or label_norm column exists, update it using
        NFC/trim/casefold while retaining the original display text. Convert editable
        with bool and encode display with json.dumps, including when it is already a
        string.

        None leaves each field unchanged and cannot clear it to SQL NULL. The changed
        result records requested updates, not changed values or affected rows; it is
        True even for a missing ID. Successful requested updates commit the chosen
        connection, including other pending work. No savepoint or rollback is added, so
        a later failure can follow earlier writes. Changing in_table updates metadata
        only and does not move or rebuild physical tables.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.executescript("CREATE TABLE custom_columns (custom_column_id INTEGER PRIMARY KEY, custom_column_name TEXT, custom_column_name_norm TEXT, custom_column_display TEXT); INSERT INTO custom_columns VALUES (1, 'Old', 'old', NULL);")
            >>> macros = CustomColumnsManagementMacrosMixin()
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(get_column_headings=lambda table: ("custom_column_name_norm",)))
            >>> macros.set_custom_column_metadata(1, name="  Straße  ", display={"heading": "A"}, conn=conn)
            True
            >>> conn.execute("SELECT custom_column_name, custom_column_name_norm FROM custom_columns").fetchone()
            ('  Straße  ', 'strasse')
            >>> json.loads(conn.execute("SELECT custom_column_display FROM custom_columns").fetchone()[0])
            {'heading': 'A'}
            >>> macros.set_custom_column_metadata(999, name="Missing", conn=conn)
            True
            >>> macros.set_custom_column_metadata(1, conn=conn)
            False
            >>> conn.in_transaction
            False
            >>> conn.close()


        :param num: Custom-column definition ID bound to custom_column_id.
        :param name: Replacement display name; None leaves it unchanged. A present
            normalization column receives its derived key.
        :param label: Replacement label; None leaves it unchanged. A present
            normalization column receives its derived key.
        :param is_editable: Value converted with bool when non-None; no stricter boolean
            validation occurs.
        :param display: JSON-serializable object despite the str annotation; serialized
            with json.dumps when non-None.
        :param in_table: Replacement attachment-table metadata, or None to leave it
            unchanged; no table validation or schema migration occurs.
        :param conn: Optional compatible connection; None selects db.driver.conn. Its
            role and transaction effects are described above.
        :return: True if any non-None update was executed, otherwise False; this does
            not confirm a row changed.
        """
        conn = conn if conn is not None else self.db.driver.conn

        changed = False
        custom_columns_headings = set(
            self.db.driver_wrapper.get_column_headings("custom_columns")
        )
        if name is not None:
            if "custom_column_name_norm" in custom_columns_headings:
                conn.execute(
                    "UPDATE custom_columns "
                    "SET custom_column_name=?, custom_column_name_norm=? "
                    "WHERE custom_column_id=?",
                    (
                        name,
                        normalize_identity_value(
                            name,
                            ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
                        ),
                        num,
                    ),
                )
            else:
                conn.execute(
                    "UPDATE custom_columns SET custom_column_name=? WHERE custom_column_id=?",
                    (name, num),
                )
            changed = True

        if label is not None:
            if "custom_column_label_norm" in custom_columns_headings:
                conn.execute(
                    "UPDATE custom_columns "
                    "SET custom_column_label=?, custom_column_label_norm=? "
                    "WHERE custom_column_id=?",
                    (
                        label,
                        normalize_identity_value(
                            label,
                            ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
                        ),
                        num,
                    ),
                )
            else:
                conn.execute(
                    "UPDATE custom_columns SET custom_column_label=? WHERE custom_column_id=?",
                    (label, num),
                )
            changed = True

        if is_editable is not None:
            conn.execute(
                "UPDATE custom_columns SET custom_column_editable=? WHERE custom_column_id=?",
                (bool(is_editable), num),
            )
            changed = True

        if display is not None:
            conn.execute(
                "UPDATE custom_columns SET custom_column_display=? WHERE custom_column_id=?",
                (json.dumps(display), num),
            )
            changed = True

        if in_table is not None:
            conn.execute(
                "UPDATE custom_columns SET custom_column_in_table=? WHERE custom_column_id=?",
                (in_table, num),
            )
            changed = True

        if changed:
            conn.commit()

        return changed

    def create_cc_table(
        self,
        normalized: bool,
        # Todo: We can type this.
        datatype: str,
        dt,
        table: str,
        link_table,
        collate,
        in_table="books",
        ordered = False,
        conn=None,
    ):
        """
        Build legacy custom-column schema and submit its complete SQLite DDL script to
        the driver.

        Resolve the owner ID column through the wrapper and derive storage-column
        prefixes with the shared inflector. A truthy normalized flag creates a unique
        non-null value table plus an owner/value link table, pair uniqueness, lookup
        indexes and validation/deletion triggers. Only datatype="series" adds a nullable
        REAL _extra column. A falsey flag creates one value table with a unique owner
        column and owner-validation triggers. No ordered/priority column is generated.

        Normalized columns also get tag-browser views exposing id, value, count,
        avg_rating and sort. Probe the selected connection for physical
        book_rating_links and ratings tables; both present selects average-rating SQL,
        otherwise avg_rating is NULL. Probes do not validate their column layouts.
        Filtered views call books_list_filter, which must exist when queried.

        The generated value-update trigger watches the literal column value rather than
        the prefixed link value column, so it does not protect normal updates to that
        prefixed column. Insert triggers validate both endpoints; deleting a custom
        value clears its links. This method creates no owner-deletion cleanup trigger or
        general foreign-key declarations.

        The conn argument supplies only rating-table probes; DDL always goes to
        db.driver.direct_execute_sql_script. The shared SQLite driver opens a separate
        connection for that script. Names, dt and collate are interpolated without
        validation or IF NOT EXISTS, so collisions or invalid fragments can leave a
        partially created schema according to driver script semantics. Definition-row
        registration, host cache refresh and an atomic transaction are left to callers.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> target = sqlite3.connect(":memory:")
            >>> probe = sqlite3.connect(":memory:")
            >>> _ = target.executescript("CREATE TABLE books (book_id INTEGER PRIMARY KEY); INSERT INTO books VALUES (1);")
            >>> driver = SimpleNamespace(direct_execute_sql_script=target.executescript)
            >>> wrapper = SimpleNamespace(get_id_column=lambda table: "book_id")
            >>> macros = CustomColumnsManagementMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=driver, driver_wrapper=wrapper)
            >>> macros.create_cc_table(True, "series", "TEXT", "custom_column_1", "books_custom_column_1_link", "", conn=probe)
            >>> probe.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0]
            0
            >>> _ = target.execute("INSERT INTO custom_column_1(custom_column_1_value) VALUES (?)", ("Saga",))
            >>> _ = target.execute("INSERT INTO books_custom_column_1_link(books_custom_column_1_link_book, books_custom_column_1_link_value) VALUES (1, 1)")
            >>> target.execute("SELECT value, count, avg_rating FROM tag_browser_custom_column_1").fetchall()
            [('Saga', 1, None)]
            >>> _ = target.execute("UPDATE books_custom_column_1_link SET books_custom_column_1_link_value=999")
            >>> target.execute("SELECT books_custom_column_1_link_value FROM books_custom_column_1_link").fetchone()[0]
            999
            >>> target.close()
            >>> probe.close()


        :param normalized: Truthy to separate unique values from owner/value links;
            falsey for a single owner/value table.
        :param datatype: Logical type string; only the exact series value changes this
            method's DDL.
        :param dt: Trusted SQL datatype fragment inserted into the value-column
            declaration.
        :param table: Trusted value-table name used directly in DDL and to derive column
            prefixes.
        :param link_table: Trusted link-table name used only in normalized mode.
        :param collate: Trusted collation fragment, such as COLLATE NOCASE, or an empty
            string.
        :param in_table: Existing owner table; its ID column is supplied by the driver
            wrapper, defaulting to books.
        :param ordered: Accepted but unused; it does not alter any generated table or
            trigger.
        :param conn: Optional connection for normalized-mode rating-table probes only;
            DDL execution always uses the driver. None resolves db.driver.conn even in
            unnormalized mode.
        :return: None; the driver script result is discarded.
        """
        conn = conn if conn is not None else self.db.driver.conn

        in_table_id_col = self.db.driver_wrapper.get_id_column(in_table)

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
                    );""".format(
                    lt=link_table, s_index=s_index, lt_col=lt_col
                ),
                "CREATE INDEX {lt}_aidx ON {lt} ({lt_col}_value);".format(lt=link_table, lt_col=lt_col),
                "CREATE INDEX {lt}_bidx ON {lt} ({lt_col}_book);".format(lt=link_table, lt_col=lt_col),
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
                    table=cc_table,
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
                    ``_has_table("ratings")`` return False and selects NULL average-rating views.


                :param _name: Exact table name bound to the sqlite_master query.
                :return: True for a fetched physical-table row; False for no row or a
                    caught exception.
                """
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

        script = " \n".join(lines)
        self.db.driver.direct_execute_sql_script(script)

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
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(execute=lambda sql, values: bindings.append(values)))
            >>> macros.do_custom_column_delete_by_num(7)
            >>> bindings
            [(7,)]


        :param num: Custom-column definition ID bound to custom_column_id.
        :return: None; the wrapper result is discarded.
        """
        self.db.driver_wrapper.execute("DELETE FROM custom_columns WHERE custom_column_id=?", (num,))

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
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(execute=lambda sql, values: bindings.append(values)))
            >>> macros.do_custom_column_delete_by_id(7)
            >>> bindings
            [7]


        :param cc_id: Definition ID passed unchanged as the wrapper's binding argument.
        :return: None; no affected-row count is checked.
        """
        del_stmt = "DELETE FROM custom_columns WHERE custom_column_id=?;"
        self.db.driver_wrapper.execute(del_stmt, cc_id)

    def mark_custom_column_for_delete(self, num: int) -> None:
        """
        Flag one definition for deferred deletion through the driver wrapper.

        Set custom_column_mark_for_delete to 1 using a bound ID. Marking does not
        immediately remove metadata or schema objects; the caller or a later loader must
        run cleanup. Missing IDs are silently ignored and wrapper.execute determines
        transaction and cache effects.

        Example:
            ``macros.mark_custom_column_for_delete(7)`` flags definition 7
            without dropping its value or link table.


        :param num: Custom-column definition ID bound to custom_column_id.
        :return: None; the wrapper execution result is discarded.
        """
        self.db.driver_wrapper.execute(
            "UPDATE custom_columns SET custom_column_mark_for_delete=1 " "WHERE custom_column_id=?",
            (num,),
        )

    def get_all_cc_ids_marked_for_delete(self, conn = None) -> list[int]:
        """
        Collect IDs whose deferred-deletion flag equals 1 on the selected connection.

        Use the project connection.get extension in its default row mode and take the
        first cell of each row. No ordering, deduplication or integer conversion is
        applied. Other flag values and SQL nulls are excluded. The query does not clear
        the flags or delete any objects.

        Example:
            >>> from types import SimpleNamespace
            >>> conn = SimpleNamespace(get=lambda sql: [(7,), (9,)])
            >>> CustomColumnsManagementMacrosMixin().get_all_cc_ids_marked_for_delete(conn=conn)
            [7, 9]


        :param conn: Optional compatible connection; None selects db.driver.conn. Its
            role and transaction effects are described above.
        :return: A list of returned ID cells, possibly empty, despite the int-only
            annotation.
        """
        conn = conn if conn is not None else self.db.driver.conn

        ids_list = []
        for record in conn.get(
            "SELECT custom_column_id " "FROM custom_columns " "WHERE custom_column_mark_for_delete=1;"
        ):
            ids_list.append(record[0])
        return ids_list

    def preform_cc_column_delete_from_map(self, num_table_lt_map: dict[int, tuple[str, str]], conn=None) -> None:
        """
        Drop mapped custom-column objects, then delete every marked definition row and
        commit.

        Iterate mapping values in mapping order; the numeric keys are unused. For each
        trusted (value_table, link_table) pair, execute a script dropping the named
        indexes, triggers, browser views and both tables with IF EXISTS. Objects are
        dropped regardless of whether their definition is marked. SQLite also removes
        table-owned indexes/triggers when their table is dropped.

        After all scripts, delete every custom_columns row with mark_for_delete=1,
        including IDs absent from the map, and commit the selected connection. An empty
        map still performs that broad DELETE and commit. Conversely, dropping an
        unmarked mapped table leaves its definition row. No transaction or rollback is
        established; script effects and earlier drops can survive a later failure. Even
        an explicit connection is committed, including other pending work.

        Example:
            >>> import sqlite3
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.executescript("CREATE TABLE custom_columns (custom_column_id INTEGER, custom_column_mark_for_delete INTEGER); INSERT INTO custom_columns VALUES (1, 1), (2, 0);")
            >>> CustomColumnsManagementMacrosMixin().preform_cc_column_delete_from_map({}, conn=conn)
            >>> conn.execute("SELECT custom_column_id FROM custom_columns").fetchall()
            [(2,)]
            >>> conn.in_transaction
            False
            >>> conn.close()


        :param num_table_lt_map: Mapping to trusted (value_table, link_table) names;
            keys do not restrict object drops or the final metadata DELETE.
        :param conn: Optional compatible connection; None selects db.driver.conn. Its
            role and transaction effects are described above.
        :return: None; mapped objects and all marked metadata rows are removed on
            success.
        """
        conn = conn if conn is not None else self.db.driver.conn

        for num, table_lt_pair in iteritems(num_table_lt_map):

            table, lt = table_lt_pair

            conn.executescript(
                """\
                                DROP INDEX   IF EXISTS {table}_idx;
                                DROP INDEX   IF EXISTS {lt}_aidx;
                                DROP INDEX   IF EXISTS {lt}_bidx;
                                DROP TRIGGER IF EXISTS fkc_update_{lt}_a;
                                DROP TRIGGER IF EXISTS fkc_update_{lt}_b;
                                DROP TRIGGER IF EXISTS fkc_insert_{lt};
                                DROP TRIGGER IF EXISTS fkc_delete_{lt};
                                DROP TRIGGER IF EXISTS fkc_insert_{table};
                                DROP TRIGGER IF EXISTS fkc_delete_{table};
                                DROP VIEW    IF EXISTS tag_browser_{table};
                                DROP VIEW    IF EXISTS tag_browser_filtered_{table};
                                DROP TABLE   IF EXISTS {table};
                                DROP TABLE   IF EXISTS {lt};
                                """.format(
                    table=table, lt=lt
                )
            )

        conn.execute("DELETE FROM custom_columns WHERE custom_column_mark_for_delete=1;")
        conn.commit()
