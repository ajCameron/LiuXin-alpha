"""Metadata SQL macros for publisher catalogue rows."""



from LiuXin_alpha.errors import DatabaseDriverError


class CMPublisherMacros:
    """
    Macros for interacting with Publishers.
    """
    def link_publisher_to_null_publisher_row(self, title_id):
        """
        Link the null publisher row to a title with maximum priorityt.
        :param title_id:
        :return:
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
