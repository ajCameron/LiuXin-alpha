



class CMSeriesTagLinksMacros:


    def unapply_series_tags(self, series_id, tags):
        """
        Remove all the tags in the given itterator from the given series.
        :param series_id:
        :param tags:
        :return:
        """
        for tag in tags:
            tag_id = self.db.driver.conn.get("SELECT tag_id FROM tags WHERE tag=?", (tag,), all=False)
            if tag_id:
                self.db.driver.conn.execute(
                    "DELETE FROM series_tag_links " "WHERE series_tag_link_tag_id=? " "AND series_tag_link_series_id=?",
                    (tag_id, series_id),
                )
        self.db.driver.conn.commit()
        self.db.driver.conn.commit()

# ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES_TAG_MACROS

    def clear_series_tag_links_for_series(self, series_id):
        """
        Clear the tags linked to a given creator.
        :param series_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM series_tag_links WHERE series_tag_link_series_id=?;",
            (series_id,),
        )

    def check_for_series_tag_link(self, series_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param series_id:
        :param tag_id:
        :return:
        """
        return self.db.driver.conn.get(
            "SELECT series_tag_link_series_id "
            "FROM series_tag_links "
            "WHERE series_tag_link_series_id=? AND series_tag_link_tag_id=?;",
            (series_id, tag_id),
            all=False,
        )

    def add_series_tag_link(self, series_id, tag_id):
        """
        Add a link between a given creator and tag.
        :param series_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO series_tag_links" "(series_tag_link_series_id, series_tag_link_tag_id) VALUES (?,?)",
            (series_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
