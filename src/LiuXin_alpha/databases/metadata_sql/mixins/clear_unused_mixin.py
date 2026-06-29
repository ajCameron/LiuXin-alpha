
"""
Macros allow direct changes to the database.
"""


class CMClearMixin:
    """
    Macros for clearing various tables under various circumstances.
    """
    def publisher_clear_unused(self):
        """
        Clear publishers which don't have any active entries in publisher_title_links.
        :return:
        """
        del_stmt = (
            "DELETE FROM publishers WHERE publisher_id NOT IN "
            "(SELECT publisher_title_link_publisher_id FROM publisher_title_links);"
        )
        self.execute(del_stmt)

    def creator_clear_unused(self):
        """
        Clear creators which don't have any active entries in publisher_title_links.
        :return:
        """
        del_stmt = (
            "DELETE FROM creators WHERE creator_id NOT IN "
            "(SELECT creator_title_link_creator_id FROM creator_title_links);"
        )
        self.execute(del_stmt)


    def break_lang_title_primary_link(self, title_id):
        """
        Remove links of primary type between titles and languages.
        :param title_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM language_title_links "
            "WHERE language_title_link_title_id = ? AND language_title_link_type = 'primary';"
        )
        if isinstance(title_id, int):
            self.execute(del_stmt, title_id)
        else:
            self.executemany(del_stmt, title_id)
