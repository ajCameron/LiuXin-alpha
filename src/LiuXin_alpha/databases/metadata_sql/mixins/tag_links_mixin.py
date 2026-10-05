"""
Provide metadata SQL operations for tag links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMTagXLinkMacros:
    """
    Implement the tag links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.clear_creator_tag_links_for_creator(1)  # doctest: +SKIP
    """


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR_TAG_MACROS

    def clear_creator_tag_links_for_creator(self, creator_id):
        """
        Delete all tag links for one creator without explicitly committing.

        Example:
            >>> metadata_sql.clear_creator_tag_links_for_creator(1)  # doctest: +SKIP


        :param creator_id: Creator identifier bound to the query or relationship.
        :return: None.
        """
        self.db.driver.conn.execute(
            "DELETE FROM creator_tag_links WHERE creator_tag_link_creator_id=?;",
            (creator_id,),
        )

    def check_for_creator_tag_link(self, creator_id, tag_id):
        """
        Read the creator ID from a matching creator/tag relationship.

        Example:
            >>> metadata_sql.check_for_creator_tag_link(1, 1)  # doctest: +SKIP


        :param creator_id: Creator identifier bound to the query or relationship.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: Connection.get(all=False) result, normally creator ID or None.
        """
        return self.db.driver.conn.get(
            "SELECT creator_tag_link_creator_id "
            "FROM creator_tag_links "
            "WHERE creator_tag_link_creator_id=? AND creator_tag_link_tag_id=?;",
            (creator_id, tag_id),
            all=False,
        )

    def add_creator_tag_link(self, creator_id, tag_id):
        """
        Insert a creator/tag pair without deduplication or explicit commit.

        Example:
            >>> metadata_sql.add_creator_tag_link(1, 1)  # doctest: +SKIP


        :param creator_id: Creator identifier bound to the query or relationship.
        :param tag_id: Tag identifier bound to the relationship predicate.
        :return: None.
        """
        self.db.driver.conn.execute(
            "INSERT INTO creator_tag_links" "(creator_tag_link_creator_id, creator_tag_link_tag_id) VALUES (?,?)",
            (creator_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
