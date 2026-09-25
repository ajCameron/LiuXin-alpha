"""
Provide metadata SQL operations for series tag links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""





class CMSeriesTagLinksMacros:
    """
    Implement the series tag links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.unapply_series_tags(1, ["news"])  # doctest: +SKIP
    """



    def unapply_series_tags(self, series_id, tags):
        """
        Resolve exact tag values and delete their links to a series.

        Skips false lookup results, including ID zero; no ordering selects among duplicate
        values. Commits the live connection twice after iteration, preserving the existing
        behavior.

        Example:
            >>> metadata_sql.unapply_series_tags(1, ["news"])  # doctest: +SKIP


        :param series_id: Series row identifier.
        :param tags: Iterable of exact tag strings to resolve before deleting their links.
        :return: None.
        """
        for tag in tags:
            tag_id = self.db.driver.conn.get("SELECT tag_id FROM tags WHERE tag=?", (tag,), all=False)
            if tag_id:
                self.db.driver.conn.execute(
                    "DELETE FROM series_tag_links " "WHERE series_tag_link_tag_id=? " "AND series_tag_link_series_id=?",
                    (tag_id, series_id),
                )
        self.db.driver.conn.commit()
        self.db.driver.conn.commit()

# ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES_TAG_MACROS

    def clear_series_tag_links_for_series(self, series_id):
        """
        Delete all tag links for one series without explicitly committing.

        Example:
            >>> metadata_sql.clear_series_tag_links_for_series(1)  # doctest: +SKIP


        :param series_id: Series row identifier.
        :return: None.
        """
        self.db.driver.conn.execute(
            "DELETE FROM series_tag_links WHERE series_tag_link_series_id=?;",
            (series_id,),
        )

    def check_for_series_tag_link(self, series_id, tag_id):
        """
        Read the series ID from a matching series/tag link without ordering.

        Example:
            >>> metadata_sql.check_for_series_tag_link(1, 1)  # doctest: +SKIP


        :param series_id: Series row identifier.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: Connection.get(all=False) result, normally series ID or None.
        """
        return self.db.driver.conn.get(
            "SELECT series_tag_link_series_id "
            "FROM series_tag_links "
            "WHERE series_tag_link_series_id=? AND series_tag_link_tag_id=?;",
            (series_id, tag_id),
            all=False,
        )

    def add_series_tag_link(self, series_id, tag_id):
        """
        Insert a series/tag pair without deduplication or explicit commit.

        Example:
            >>> metadata_sql.add_series_tag_link(1, 1)  # doctest: +SKIP


        :param series_id: Series row identifier.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: None.
        """
        self.db.driver.conn.execute(
            "INSERT INTO series_tag_links" "(series_tag_link_series_id, series_tag_link_tag_id) VALUES (?,?)",
            (series_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
