"""
Provide metadata SQL operations for tag title links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMTagTitleLinkMacros:
    """
    Implement the tag title links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.break_tag_title_link(1, 1)  # doctest: +SKIP
    """



    def break_tag_title_link(self, tag_id, title_id):
        """
        Delete links matching the supplied tag/title pair without explicitly committing.

        Example:
            >>> metadata_sql.break_tag_title_link(1, 1)  # doctest: +SKIP


        :param tag_id: Tag identifier bound to the relationship predicate.
        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        self.db.driver.conn.execute(
            "DELETE FROM tag_title_links " "WHERE tag_title_link_tag_id=? " "AND tag_title_link_title_id=?",
            (tag_id, title_id),
        )
