"""
Provide metadata SQL operations for series.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMSeriesMacrosMixin:
    """
    Implement the series operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.library_unset_series(1, 1)  # doctest: +SKIP
    """



    def library_unset_series(self, title_id, series_id):
        """
        Delete all links matching the supplied title and series IDs.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.library_unset_series(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param series_id: Series row identifier.
        :return: None.
        """
        del_stmt = (
            "DELETE FROM series_title_links "
            "WHERE series_title_link_title_id = ? AND series_title_link_series_id= ? ;"
        )
        self.execute(del_stmt, (title_id, series_id))


    def remove_unused_series(self):
        """
        Delete series with no title-link rows and commit the live connection.

        Reads all series IDs, checks each separately and applies no sentinel or ancestry
        protection. Backend constraints determine whether deletion succeeds.

        Example:
            >>> metadata_sql.remove_unused_series()  # doctest: +SKIP


        :return: None.
        """
        for (series_id,) in self.db.driver.conn.get("SELECT series_id FROM series"):
            if not self.db.driver.conn.get(
                "SELECT series_title_link_id " "FROM series_title_links " "WHERE series_title_link_series_id=?",
                (series_id,),
            ):
                self.db.driver.conn.execute("DELETE FROM series WHERE series_id=?", (series_id,))
        self.db.driver.conn.commit()
