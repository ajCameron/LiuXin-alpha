


class CMTagsMixin:

    def delete_tag_by_value(self, tag):
        """
        Delete a tag from the tags table using the value of the tag in that table.
        :param tag:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM tags WHERE tag=?;", (tag,))
        self.db.driver.conn.commit()

    def get_tag_id_from_value(self, tag):
        """
        Retrieve the tag id corresponding to the given tag value.
        :param tag:
        :return:
        """
        return self.db.driver.conn.get("SELECT tag_id FROM tags WHERE tag=?", (tag,), all=False)


    def add_tag(self, tag_value):
        """
        Add a tag to the database and return the lastrowid - hopefully corresponding to that tag.
        :param tag_value:
        :return:
        """
        return self.db.driver.conn.execute("INSERT INTO tags(tag) VALUES(?);", (tag_value,)).lastrowid