"""Metadata SQL macros for tag-to-title relationships."""




class CMTagTitleLinkMacros:
    """Implement tag-to-title relationship macros."""



    def break_tag_title_link(self, tag_id, title_id):
        """
        Break a link, if one exists, between the given title and tag
        :param tag_id:
        :param title_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM tag_title_links " "WHERE tag_title_link_tag_id=? " "AND tag_title_link_title_id=?",
            (tag_id, title_id),
        )
