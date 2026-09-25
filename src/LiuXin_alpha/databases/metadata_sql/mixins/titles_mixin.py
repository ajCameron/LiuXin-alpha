"""
Provide metadata SQL operations for titles.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""





class CMTitlesMacrosMixin:
    """
    Implement the titles operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.update_title(1, "Example")  # doctest: +SKIP
    """


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLES VALUES METHODS

    def update_title(self, title_id, title):
        """
        Write a title value, converting every false input to SQL NULL.

        Updates existing rows only; does not create missing titles or alter physical assets.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.update_title(1, "Example")  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param title: Text bound as a feed/title value; false title handling is described
            above.
        :return: None.
        """
        if not title:
            self.execute("UPDATE titles SET title=Null WHERE title_id=?;", (title_id,))
        else:
            self.execute("UPDATE titles SET title=? WHERE title_id=?;", (title, title_id))

    # Todo: Add the setting null option
    def update_title_creator_sort(self, title_id, creator_val):
        """
        Write title_creator_sort through the host execution helper.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.update_title_creator_sort(1, "Doe, Jane")  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param creator_val: Replacement title_creator_sort value.
        :return: None.
        """
        stmt = "UPDATE titles SET title_creator_sort = ? WHERE title_id = ?;"
        self.execute(stmt, (creator_val, title_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
