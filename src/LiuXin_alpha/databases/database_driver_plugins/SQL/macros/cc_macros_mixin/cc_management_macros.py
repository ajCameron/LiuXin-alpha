
from six import iteritems



class CustomColumnsManagementMacrosMixin:
    """
    Mixin for management (creation, update and deletion) of custom columns themselves.
    """
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CC MANAGEMENT METHODS

    def mark_cc_for_delete(self, cc_column_id):
        """
        Note that a cc should be deleted on the next restart.
        :param cc_column_id: The id of the custom column to delete.
        :return:
        """
        self.execute(
            "UPDATE custom_columns " "SET custom_column_mark_for_delete = 1 " "WHERE custom_column_id=?",
            (cc_column_id,),
        )

    def set_custom_column_metadata(
        self,
        num,
        name=None,
        label=None,
        is_editable=None,
        display=None,
        in_table=None,
        conn=None,
    ):
        """
        Preforms a set of the custom column metadata.

        :param num:
        :param name:
        :param label:
        :param is_editable:
        :param display:
        :param in_table: Which table

        :param conn: An override conn to execute the stmnts on

        :return:
        """
        conn = conn if conn is not None else self.db.driver.conn

        changed = False
        if name is not None:
            conn.execute(
                "UPDATE custom_columns SET custom_column_name=? WHERE custom_column_id=?",
                (name, num),
            )
            changed = True

        if label is not None:
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
        normalized,
        datatype,
        dt,
        table,
        link_table,
        collate,
        in_table="books",
        ordered=False,
        conn=None,
    ):
        """
        Execute the SQL needed to create a custom table.
        :param normalized:
        :param datatype:
        :param dt:
        :param table:
        :param link_table:
        :param collate:
        :param in_table:
        :param conn:
        :return:
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
                # Todo: This is both totally broken and needs to be generalized - probably a bad idea to do it this way
                #       in the database at all
                # Todo: Titles have ratings, not books in the current schema
                """
                CREATE VIEW tag_browser_{cc_table} AS SELECT
                    {cc_table_col}_id,
                    {cc_table_col}_value,
                    (SELECT COUNT(id) FROM {lt} WHERE value={cc_table}.{cc_table_col}_id) count,
                    (SELECT AVG(r.rating)
                     FROM {lt},
                          book_rating_links as bl,
                          ratings as r
                     WHERE {lt}.value={cc_table}.id and bl.book_rating_link_book_id={lt}.book and
                           r.rating_id = bl.book_rating_link_rating_id and r.rating <> 0) avg_rating,
                    value AS sort
                FROM {cc_table};
                """.format(
                    lt=link_table,
                    lt_col=lt_col,
                    cc_table=cc_table,
                    cc_table_col=cc_table_col,
                ),
                """
                CREATE VIEW tag_browser_filtered_{cc_table} AS SELECT
                    id,
                    value,
                    (SELECT COUNT({lt}.id) FROM {lt} WHERE value={cc_table}.id AND
                    books_list_filter(book)) count,
                    (SELECT AVG(r.rating)
                     FROM {lt},
                          book_rating_links as bl,
                          ratings as r
                     WHERE {lt}.value={cc_table}.id AND bl.book_rating_link_book_id={lt}.book AND
                           r.rating_id = bl.book_rating_link_rating_id AND r.rating <> 0 AND
                           books_list_filter(bl.book_rating_link_book_id)) avg_rating,
                    value AS sort
                FROM {cc_table};
                """.format(
                    lt=link_table, cc_table=cc_table
                ),
            ]

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
        self.db.driver_wrapper.executescript(script)

    def do_custom_column_delete_by_num(self, num):
        """
        Actually do the deletion of a custom column.
        :param num:
        :return:
        """
        self.db.driver_wrapper.execute("DELETE FROM custom_columns WHERE custom_column_id=?", (num,))

    def do_custom_column_delete_by_id(self, cc_id):
        """
        Actually do the deletion of a custom column.
        :param cc_id:
        :return:
        """
        del_stmt = "DELETE FROM custom_columns WHERE custom_column_id=?;"
        self.db.driver_wrapper.execute(del_stmt, cc_id)

    def mark_custom_column_for_delete(self, num):
        """
        Set the custom_column_mark_for_delete column value to 1.
        It will be deleted on the next restart.
        :param num:
        :return:
        """
        self.db.driver_wrapper.execute(
            "UPDATE custom_columns SET custom_column_mark_for_delete=1 " "WHERE custom_column_id=?",
            (num,),
        )

    def get_all_cc_ids_marked_for_delete(self, conn=None):
        """
        Get all the custom column ids which are not marked for delete.
        :return:
        """
        conn = conn if conn is not None else self.db.driver.conn

        ids_list = []
        for record in conn.get(
            "SELECT custom_column_id " "FROM custom_columns " "WHERE custom_column_mark_for_delete=1;"
        ):
            ids_list.append(record[0])
        return ids_list

    def preform_cc_column_delete_from_map(self, num_table_lt_map, conn=None):
        """
        Use a num_table_lt map to actually remove entries from the database.
        :param num_table_lt_map:
        :param conn:
        :return:
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

