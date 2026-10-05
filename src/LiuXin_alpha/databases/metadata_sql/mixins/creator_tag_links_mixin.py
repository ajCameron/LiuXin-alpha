"""
Provide metadata SQL operations for creator tag links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""





class CMCreatorTagLinkMacros:
    """
    Implement the creator tag links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.break_creator_tag_link(1, 1)  # doctest: +SKIP
    """

    def break_creator_tag_link(self, tag_id, creator_id):
        """
        Delete links matching both the supplied creator and tag IDs.

        Uses the live connection without an explicit commit.

        Example:
            >>> metadata_sql.break_creator_tag_link(1, 1)  # doctest: +SKIP


        :param tag_id: Tag identifier bound to the relationship predicate.
        :param creator_id: Creator identifier bound to the query or relationship.
        :return: None.
        """
        self.db.driver.conn.execute(
            "DELETE FROM creator_tag_links " "WHERE creator_tag_link_tag_id=? " "AND creator_tag_link_creator_id=?",
            (tag_id, creator_id),
        )

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR_TITLE_MACROS

    def clear_tag_title_links_for_title(self, title_id):
        """
        Delete all tag links for one title on the live connection.

        Does not delete tag rows or explicitly commit.

        Example:
            >>> metadata_sql.clear_tag_title_links_for_title(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        self.db.driver.conn.execute("DELETE FROM tag_title_links WHERE tag_title_link_title_id=?;", (title_id,))

    def check_for_tag_title_link(self, title_id, tag_id):
        """
        Read the matching title ID from tag_title_links without ordering.

        Example:
            >>> metadata_sql.check_for_tag_title_link(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: Connection.get(all=False) result: normally a title-ID scalar or None, not a
            strict bool.
        """
        return self.db.driver.conn.get(
            "SELECT tag_title_link_title_id "
            "FROM tag_title_links "
            "WHERE tag_title_link_title_id=? AND tag_title_link_tag_id=?;",
            (title_id, tag_id),
            all=False,
        )

    def add_tag_title_link(self, title_id, tag_id):
        """
        Insert a title/tag relationship without deduplication or explicit commit.

        Constraint failures propagate; priority/type columns rely on schema defaults.

        Example:
            >>> metadata_sql.add_tag_title_link(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: None.
        """
        self.db.driver.conn.execute(
            "INSERT INTO tag_title_links" "(tag_title_link_title_id, tag_title_link_tag_id) VALUES (?,?)",
            (title_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
