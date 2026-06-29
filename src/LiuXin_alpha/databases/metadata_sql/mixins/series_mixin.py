


class CMSeriesMacrosMixin:


    def library_unset_series(self, title_id, series_id):
        """
        Used to remove a specific link between a title and a series.
        :param db:
        :param title_id:
        :param series_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM series_title_links "
            "WHERE series_title_link_title_id = ? AND series_title_link_series_id= ? ;"
        )
        self.execute(del_stmt, (title_id, series_id))


    def remove_unused_series(self):
        """
        Remove series which are not currently in use - i.e. linked to the titles table.
        :return:
        """
        for (series_id,) in self.db.driver.conn.get("SELECT series_id FROM series"):
            if not self.db.driver.conn.get(
                "SELECT series_title_link_id " "FROM series_title_links " "WHERE series_title_link_series_id=?",
                (series_id,),
            ):
                self.db.driver.conn.execute("DELETE FROM series WHERE series_id=?", (series_id,))
        self.db.driver.conn.commit()
