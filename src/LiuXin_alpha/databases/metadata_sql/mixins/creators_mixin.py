"""
Provide metadata SQL operations for creators.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMCreatorMacrosMixin:
    """
    Implement the creators operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.get_creator_sort(1)  # doctest: +SKIP
    """


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR VALUES METHODS

    def get_creator_sort(self, creator_id):
        """
        Read creator_sort from the first returned row.

        Expects self.get() to return indexable rows; IndexError becomes None, while other
        shape/query errors propagate.

        Example:
            >>> metadata_sql.get_creator_sort(1)  # doctest: +SKIP


        :param creator_id: Creator identifier bound to the query or relationship.
        :return: Stored sort value or None for an empty result.
        """
        db_result = self.get("SELECT creator_sort FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    def get_creator_link(self, creator_id):
        """
        Read creator_link from the first returned row.

        Expects self.get() to return indexable rows; IndexError becomes None, while other
        shape/query errors propagate.

        Example:
            >>> metadata_sql.get_creator_link(1)  # doctest: +SKIP


        :param creator_id: Creator identifier bound to the query or relationship.
        :return: Stored link value or None for an empty result.
        """
        db_result = self.get("SELECT creator_link FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    # Todo: Add the single option - bring into line with the naming scheme
    def update_creator_links(self, values):
        """
        Batch-write creator_link from (new_link, creator_id) bindings.

        Forwards the iterable unchanged; the earlier ID-first description was incorrect.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE creators (creator_id INTEGER PRIMARY KEY, creator_link TEXT, creator_sort TEXT)')
            >>> _ = conn.execute("INSERT INTO creators VALUES (1, 'old', 'Old')")
            >>> conn.commit()
            >>> host = SimpleNamespace(executemany=conn.executemany)
            >>> CMCreatorMacrosMixin.update_creator_links(host, [('https://example.org/jane', 1)])
            >>> conn.execute('SELECT creator_link FROM creators WHERE creator_id=1').fetchone()
            ('https://example.org/jane',)
            >>> conn.close()


        :param values: Iterable of (new_link, creator_id) binding pairs.
        :return: None.
        """
        stmt = "UPDATE creators SET creator_link=? WHERE creator_id=?"
        self.executemany(stmt, values)

    # Todo: Bring into line with the rest by offering a singular and multiple update options
    def update_creator_sorts(self, values):
        """
        Batch-write creator_sort from (new_sort, creator_id) bindings.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE creators (creator_id INTEGER PRIMARY KEY, creator_link TEXT, creator_sort TEXT)')
            >>> _ = conn.execute("INSERT INTO creators VALUES (1, 'old', 'Old')")
            >>> conn.commit()
            >>> host = SimpleNamespace(executemany=conn.executemany)
            >>> CMCreatorMacrosMixin.update_creator_sorts(host, [('Doe, Jane', 1)])
            >>> conn.execute('SELECT creator_sort FROM creators WHERE creator_id=1').fetchone()
            ('Doe, Jane',)
            >>> conn.close()


        :param values: Iterable of (new_sort, creator_id) binding pairs.
        :return: None.
        """
        stmt = "UPDATE creators SET creator_sort=? WHERE creator_id=?"
        self.executemany(stmt, values)

    #
    # ------------------------------------------------------------------------------------------------------------------


    # Todo: Not actually file macros..
    # - FILE MACROS
    def read_creator_with_sort_and_link(self):
        """
        Read every creator ID, name, sort value and link without explicit ordering.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_creator_with_sort_and_link()  # doctest: +SKIP


        :return: Execution cursor/iterable of four-cell creator rows.
        """
        stmt = "SELECT creator_id, creator, creator_sort, creator_link FROM creators;"
        return self.execute(stmt)
