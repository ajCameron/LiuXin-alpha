


class CMTagXLinkMacros:

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR_TAG_MACROS

    def clear_creator_tag_links_for_creator(self, creator_id):
        """
        Clear the tags linked to a given creator.
        :param creator_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM creator_tag_links WHERE creator_tag_link_creator_id=?;",
            (creator_id,),
        )

    def check_for_creator_tag_link(self, creator_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param creator_id:
        :param tag_id:
        :return:
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
        Add a link between a given creator and tag.
        :param title_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO creator_tag_links" "(creator_tag_link_creator_id, creator_tag_link_tag_id) VALUES (?,?)",
            (creator_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
