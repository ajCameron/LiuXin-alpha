"""
Provide metadata SQL operations for title comments.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMTitleCommentsMacrosMixin:
    """
    Implement the title comments operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.clear_title_comments_from_title_id(1)  # doctest: +SKIP
    """

    
    def clear_title_comments_from_title_id(self, title_id):
        """
        Delete comment_title_links for one title without deleting comment rows.

        Example:
            >>> metadata_sql.clear_title_comments_from_title_id(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        stmt = "DELETE FROM comment_title_links WHERE comment_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(stmt, (title_id,))
