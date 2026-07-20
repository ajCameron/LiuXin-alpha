


class CMTagsMixin:

    def delete_tag_by_value(self, tag):
        """
        Delete a tag from the tags table using the value of the tag in that table.
        :param tag:
        :return:
        """
        identity = self.db.get_canonical_identity("tags", "tag", tag)
        if identity is None:
            return
        self.db.driver.conn.execute(
            "DELETE FROM tags WHERE tag_id=?;",
            (identity.row_id,),
        )
        self.db.driver.conn.commit()

    def get_tag_id_from_value(self, tag):
        """
        Retrieve the tag id corresponding to the given tag value.
        :param tag:
        :return:
        """
        identity = self.db.get_canonical_identity("tags", "tag", tag)
        return None if identity is None else identity.row_id


    def add_tag(self, tag_value):
        """
        Add a tag to the database and return the lastrowid - hopefully corresponding to that tag.
        :param tag_value:
        :return:
        """
        return self.db.macros.ensure_table_value("tags", "tag", tag_value)
