

from LiuXin_alpha.errors import DatabaseDriverError

class SeriesTitleLinkMacros:
    """
    Macros for controlling series title links.
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES_TITLE_LINK MACROS

    def get_series_id_from_value(self, series):
        """
        Returns the series_id from the given series value.
        :param series:
        :return:
        """
        return self.db.driver.conn.get("SELECT series_id FROM series WHERE series=?;", (series,), all=False)

    def check_for_series_title_link(self, series_id, title_id):
        """
        Check to see if there is an existing link between a given series and title.
        :param series_id:
        :param title_id:
        :return:
        """
        stmt = (
            "SELECT series_title_link_id, series_title_link_index "
            "FROM series_title_links "
            "WHERE series_title_link_series_id = ? AND series_title_link_title_id = ?"
            "ORDER BY series_title_link_priority DESC;"
        )
        return self.db.driver.conn.get_row(stmt, (series_id, title_id), all=False)

    def get_primary_series_index(self, title_id):
        """
        Return the index of the primary series for the given title.
        :param title_id:
        :return:
        """
        stmt = (
            "SELECT series_title_link_index "
            "FROM series_title_links "
            "WHERE series_title_link_title_id = ?"
            "ORDER BY series_title_link_priority DESC;"
        )
        return self.db.driver.conn.get(stmt, (title_id,), all=False)

    def break_series_title_link(self, title_id, series_id=0):
        """
        Break a link between the series and a given title.
        :param title_id:
        :param series_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM series_title_links "
            "WHERE series_title_link_series_id = ? AND series_title_link_title_id = ?;"
        )
        self.db.driver_wrapper.execute(
            del_stmt,
            (
                series_id,
                title_id,
            ),
        )

    def link_null_series_to_title(self, title_id, series_index):
        """
        Link the title to the null series - and records the series index for later use.
        :param title_id:
        :param series_index:
        :return:
        """
        stmt = (
            "INSERT INTO series_title_links "
            "(series_title_link_title_id, series_title_link_series_id, "
            "series_title_link_index, series_title_link_priority) "
            "SELECT ?, 0, ?, MAX(series_title_link_priority) + 1 FROM series_title_links;"
        )
        try:
            self.db.driver_wrapper.execute(stmt, (title_id, series_index))
        except DatabaseDriverError:
            # Link has already been set null
            # Todo: Should, if this link exists, update the link with the new index
            pass

    def read_primary_title_series_id_from_meta(self, title_id):
        """
        Read and return the series_id from the meta view.
        :param title_id:
        :return:
        """
        return self.db.driver.conn.get("SELECT series_id FROM meta WHERE id=?;", (title_id,), all=False)

    def update_index_for_series_title_link(self, title_id, series_id, index):
        """
        Update the index for the given series title link.
        :param title_id:
        :param series_id:
        :param index:
        :return:
        """
        stmt = (
            "UPDATE series_title_links "
            "SET series_title_link_index = ? "
            "WHERE series_title_link_series_id = ?"
            "AND series_title_link_title_id = ?;"
        )
        self.db.driver.conn.execute(stmt, (float(index), series_id, title_id))
        self.db.driver.conn.commit()

    #
    # ------------------------------------------------------------------------------------------------------------------
