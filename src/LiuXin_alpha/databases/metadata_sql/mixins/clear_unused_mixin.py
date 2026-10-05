
"""
Provide metadata SQL operations for clear unused.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""


class CMClearMixin:
    """
    Implement the clear unused operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.publisher_clear_unused()  # doctest: +SKIP
    """
    def publisher_clear_unused(self):
        """
        Delete publishers absent from publisher_title_links.

        Uses NOT IN; NULL values in the subquery can prevent expected deletions. No special
        sentinel-row exclusion is added.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.publisher_clear_unused()  # doctest: +SKIP


        :return: None.
        """
        del_stmt = (
            "DELETE FROM publishers WHERE publisher_id NOT IN "
            "(SELECT publisher_title_link_publisher_id FROM publisher_title_links);"
        )
        self.execute(del_stmt)

    def creator_clear_unused(self):
        """
        Delete creators absent from creator_title_links.

        Uses NOT IN, including its NULL semantics, and does not protect a sentinel row
        explicitly.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.creator_clear_unused()  # doctest: +SKIP


        :return: None.
        """
        del_stmt = (
            "DELETE FROM creators WHERE creator_id NOT IN "
            "(SELECT creator_title_link_creator_id FROM creator_title_links);"
        )
        self.execute(del_stmt)


    def break_lang_title_primary_link(self, title_id):
        """
        Delete only primary language links for one title or supplied batch bindings.

        Integer input uses execute with a scalar; other inputs go unchanged to executemany.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.break_lang_title_primary_link(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        del_stmt = (
            "DELETE FROM language_title_links "
            "WHERE language_title_link_title_id = ? AND language_title_link_type = 'primary';"
        )
        if isinstance(title_id, int):
            self.execute(del_stmt, title_id)
        else:
            self.executemany(del_stmt, title_id)
