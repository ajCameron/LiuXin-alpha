"""
Provide metadata SQL operations for publisher title links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMPublisherTitleLinkMacros:
    """
    Implement the publisher title links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.clear_publisher_title_links_by_title_id(1)  # doctest: +SKIP
    """


    def clear_publisher_title_links_by_title_id(self, title_id):
        """
        Delete all publisher relationships for the selected title through its wrapper.

        Example:
            >>> metadata_sql.clear_publisher_title_links_by_title_id(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        del_stmt = "DELETE FROM publisher_title_links " "WHERE publisher_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(del_stmt, (title_id,))

    def check_for_title_id_publisher_id_link(self, pub_id, title_id):
        """
        Read the highest-priority publisher/title relationship ID.

        Priority is descending; ties are not otherwise ordered.

        Example:
            >>> metadata_sql.check_for_title_id_publisher_id_link(1, 1)  # doctest: +SKIP


        :param pub_id: Publisher row identifier.
        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: Connection.get(all=False) result, normally a link-ID scalar or None.
        """
        stmt = (
            "SELECT publisher_title_link_id "
            "FROM publisher_title_links "
            "WHERE publisher_title_link_publisher_id = ? AND publisher_title_link_title_id = ? "
            "ORDER BY publisher_title_link_priority DESC;"
        )
        pt_id = self.db.driver.conn.get(stmt, (pub_id, title_id), all=False)
        return pt_id

    def clear_null_publisher_links_from_title(self, title_id):
        """
        Delete only publisher-ID-zero links for the selected title.

        Example:
            >>> metadata_sql.clear_null_publisher_links_from_title(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        del_stmt = (
            "DELETE FROM publisher_title_links "
            "WHERE publisher_title_link_publisher_id = 0 AND publisher_title_link_title_id = ?;"
        )
        self.db.driver_wrapper.execute(del_stmt, (title_id,))
