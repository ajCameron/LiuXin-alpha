"""Metadata SQL macros for creator-to-tag relationships."""





class CMCreatorTagLinkMacros:
    """
    Creator tag links.
    """

    def break_creator_tag_link(self, tag_id, creator_id):
        """
        Break a list - if one exists - between the given creator and given tag.
        :param tag_id:
        :param creator_id:
        :return:
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
        Clear the tags linked to a given title.
        :param title_id:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM tag_title_links WHERE tag_title_link_title_id=?;", (title_id,))

    def check_for_tag_title_link(self, title_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param title_id:
        :param tag_id:
        :return:
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
        Add a link betwekn the given tag and title
        :param title_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO tag_title_links" "(tag_title_link_title_id, tag_title_link_tag_id) VALUES (?,?)",
            (title_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
