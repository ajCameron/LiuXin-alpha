"""
Provide metadata SQL operations for publishers.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""



from LiuXin_alpha.errors import DatabaseDriverError


class CMPublisherMacros:
    """
    Implement the publishers operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.link_publisher_to_null_publisher_row(1)  # doctest: +SKIP
    """
    def link_publisher_to_null_publisher_row(self, title_id):
        """
        Insert a publisher-ID-zero link using global maximum priority plus one.

        An empty link table yields NULL priority. Suppresses every DatabaseDriverError as if
        a null link already existed, including unrelated driver failures.

        Example:
            >>> metadata_sql.link_publisher_to_null_publisher_row(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        # Nullify the publisher - by linking it to the null pub row
        stmt = (
            "INSERT INTO publisher_title_links "
            "(publisher_title_link_title_id, publisher_title_link_publisher_id, "
            "publisher_title_link_priority) "
            "SELECT ?, 0, MAX(publisher_title_link_priority) + 1 FROM publisher_title_links;"
        )

        try:
            self.db.driver_wrapper.execute(stmt, (title_id,))
        except DatabaseDriverError:
            # Link has already been set null
            pass
