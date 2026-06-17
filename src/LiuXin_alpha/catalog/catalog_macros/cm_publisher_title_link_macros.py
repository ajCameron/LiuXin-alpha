


class CMPublisherTitleLinkMacros:

    def clear_publisher_title_links_by_title_id(self, title_id):
        """
        Remove all publisher title links with the publisher being linked to the given title_id.
        :param title_id:
        :return:
        """
        del_stmt = "DELETE FROM publisher_title_links " "WHERE publisher_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(del_stmt, (title_id,))

    def check_for_title_id_publisher_id_link(self, pub_id, title_id):
        """
        Check to see if there is an existing link between a given publisher id and a given title id.
        :param pub_id:
        :param title_id:
        :return:
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
        Remove all the links to publisher 0, linked to the specified title_id, are removed.
        :param title_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM publisher_title_links "
            "WHERE publisher_title_link_publisher_id = 0 AND publisher_title_link_title_id = ?;"
        )
        self.db.driver_wrapper.execute(del_stmt, (title_id,))
