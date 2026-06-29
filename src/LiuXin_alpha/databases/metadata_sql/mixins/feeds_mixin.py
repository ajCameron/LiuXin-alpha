
"""
Feeds mixin - methods to access, delete and update feeds.
"""

from __future__ import annotations


class FeedsMixin:
    """
    Feeds SQL helpers.
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FEED MANAGEMENT

    def add_feed(self, title: str, script: str) -> None:
        """
        Add a feed to the feeds table.

        :param title:
        :param script:
        :return:
        """
        insert_stmt = "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?);"
        self.execute(insert_stmt, (title, script))

    def delete_feed(self, feed_id):
        """
        Remove a feed from the feeds table.

        :param feed_id:
        :return:
        """
        del_stmt = "DELETE FROM feeds WHERE feed_id=?;"
        if isinstance(feed_id, int):
            self.execute(del_stmt, (feed_id,))
        else:
            self.executemany(del_stmt, feed_id)

    #
    # ------------------------------------------------------------------------------------------------------------------

    # Todo: THe below probably needs to be tested

    def update_feed(self, feed_id, script, title):
        """
        Update a feed stored in the feeds table.
        :param feed_id:
        :param script:
        :param title:
        :return:
        """
        self.db.driver.conn.execute("UPDATE feeds set feed_title=? WHERE feed_id=?", (title, feed_id))
        self.db.driver.conn.execute("UPDATE feeds set feed_script=? WHERE feed_id=?", (script, feed_id))
        self.db.driver.conn.commit()

    def set_feeds(self, feeds):
        """
        Clears an entire feed table and populate the table anew with an iterator.
        :param feeds:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM feeds")
        for title, script in feeds:
            self.db.driver.conn.execute(
                "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?)",
                (title, script),
            )
        self.db.driver.conn.commit()
