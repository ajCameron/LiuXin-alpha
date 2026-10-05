
"""
Provide metadata SQL operations for deletion.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""


class CMDeletionMacros:
    """
    Implement the deletion operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.delete_item_by_id("files", "file_id", 1)  # doctest: +SKIP
    """

    def delete_item_by_id(self, item_table, item_id_col, item_id):
        """
        Delete rows from a trusted table using a trusted ID column.

        Identifiers are interpolated without validation. Integer IDs use one-cell bindings;
        other inputs are forwarded unchanged as batch bindings.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_item_by_id("files", "file_id", 1)  # doctest: +SKIP


        :param item_table: Trusted table identifier interpolated into SQL.
        :param item_id_col: Trusted identifier-column name interpolated into SQL.
        :param item_id: Integer ID for one execution, or batch bindings passed unchanged to
            executemany.
        :return: None.
        """
        # Todo: Really needs some kind of checking
        del_stmt = "DELETE FROM {} WHERE {}=?".format(item_table, item_id_col)
        if isinstance(item_id, int):
            self.execute(del_stmt, (item_id,))
        else:
            self.executemany(del_stmt, item_id)

    def delete_title(self, title_id):
        """
        Delete the title row first, then its same-ID book row in a separate execution.

        Foreign-key or trigger effects belong to the schema; no combined rollback boundary
        is added.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_title(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        title_del_stmt = "DELETE FROM titles WHERE title_id = ?;"
        self.execute(title_del_stmt, (title_id,))
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (title_id,))

    def delete_book(self, book_id):
        """
        Delete one books row without directly deleting its title or physical files.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_book(1)  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :return: None.
        """
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (book_id,))
