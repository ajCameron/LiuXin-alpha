


class TitlesMacroMethodsMixin:
    """
    Mixin to provide methods to manipulate the titles table.
    """
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLE CREATOR METHODS

    def clear_title_creator_links_for_given_type_and_title(self, title_id):
        """
        Clear the links between a certain title and all creators with a certain link type.
        :param title_id: All creator links to this title will be cleared
        :return:
        """
        stmt = (
            "DELETE FROM creator_title_links "
            "WHERE creator_title_link_title_id = ? AND creator_title_link_type='authors';"
        )
        self.db.driver.conn.execute(stmt, (title_id,))
        self.db.driver.conn.commit()

    def check_for_title_author_link(self, title_id, creator_id):
        """
        Check to see that there is an author type link between the title and the creator
        :param title_id:
        :param creator_id:
        :return:
        """
        stmt = (
            "SELECT creator_title_link_id FROM creator_title_links "
            "WHERE creator_title_link_title_id = ? "
            "AND creator_title_link_creator_id = ? "
            "AND creator_title_links.creator_title_link_type='authors';"
        )
        return self.db.driver.conn.get(stmt, (title_id, creator_id), all=False)

    def update_title_author_link_priority(self, title_id, creator_id, new_priority):
        """
        Update the link between the title and the creator - of author type
        :param title_id:
        :param creator_id:
        :param new_priority:
        :return:
        """
        stmt = (
            "UPDATE creator_title_links "
            "SET creator_title_link_priority = ? "
            "WHERE creator_title_link_title_id = ? "
            "AND creator_title_link_creator_id = ? "
            "AND creator_title_links.creator_title_link_type='authors';"
        )
        self.db.driver.conn.execute(stmt, (new_priority, title_id, creator_id))
        self.db.driver.conn.commit()

    #
    # ------------------------------------------------------------------------------------------------------------------
