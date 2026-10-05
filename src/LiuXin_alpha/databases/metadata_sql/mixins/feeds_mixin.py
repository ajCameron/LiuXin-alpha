
"""
Provide metadata SQL operations for feeds.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""

from __future__ import annotations


class FeedsMixin:
    """
    Implement the feeds operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.add_feed("Example", "recipe")  # doctest: +SKIP
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FEED MANAGEMENT

    def add_feed(self, title: str, script: str) -> None:
        """
        Insert a feed title and script as bound data.

        The script is stored text and is not executed.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE feeds (feed_id INTEGER PRIMARY KEY, feed_title TEXT, feed_script TEXT)')
            >>> host = SimpleNamespace(execute=conn.execute, executemany=conn.executemany, db=SimpleNamespace(driver=SimpleNamespace(conn=conn)))
            >>> FeedsMixin.add_feed(host, "Today's news", 'stored recipe')
            >>> conn.execute('SELECT feed_title, feed_script FROM feeds').fetchone()
            ("Today's news", 'stored recipe')
            >>> conn.close()


        :param title: Text bound as a feed/title value; false title handling is described
            above.
        :param script: Stored feed script text; it is not executed as Python.
        :return: None.
        """
        insert_stmt = "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?);"
        self.execute(insert_stmt, (title, script))

    def delete_feed(self, feed_id):
        """
        Delete an integer feed ID or forward other inputs as batch bindings.

        The bulk path performs no per-item tuple wrapping.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE feeds (feed_id INTEGER PRIMARY KEY, feed_title TEXT, feed_script TEXT)')
            >>> host = SimpleNamespace(execute=conn.execute, executemany=conn.executemany, db=SimpleNamespace(driver=SimpleNamespace(conn=conn)))
            >>> _ = conn.executemany('INSERT INTO feeds VALUES (?, ?, ?)', [(1, 'one', ''), (2, 'two', '')])
            >>> FeedsMixin.delete_feed(host, [(1,), (2,)])
            >>> conn.execute('SELECT count(*) FROM feeds').fetchone()
            (0,)
            >>> conn.close()


        :param feed_id: Integer feed ID or batch bindings, where the deletion method
            supports them.
        :return: None.
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
        Update feed title and script with two live-connection statements, then commit.

        A failure in the second statement can leave the first pending; no rollback is added.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE feeds (feed_id INTEGER PRIMARY KEY, feed_title TEXT, feed_script TEXT)')
            >>> host = SimpleNamespace(execute=conn.execute, executemany=conn.executemany, db=SimpleNamespace(driver=SimpleNamespace(conn=conn)))
            >>> _ = conn.execute("INSERT INTO feeds VALUES (1, 'old', 'old recipe')")
            >>> FeedsMixin.update_feed(host, 1, 'new recipe', 'new title')
            >>> conn.execute('SELECT feed_title, feed_script FROM feeds').fetchone()
            ('new title', 'new recipe')
            >>> conn.in_transaction
            False
            >>> conn.close()


        :param feed_id: Integer feed ID or batch bindings, where the deletion method
            supports them.
        :param script: Stored feed script text; it is not executed as Python.
        :param title: Text bound as a feed/title value; false title handling is described
            above.
        :return: None.
        """
        self.db.driver.conn.execute("UPDATE feeds set feed_title=? WHERE feed_id=?", (title, feed_id))
        self.db.driver.conn.execute("UPDATE feeds set feed_script=? WHERE feed_id=?", (script, feed_id))
        self.db.driver.conn.commit()

    def set_feeds(self, feeds):
        """
        Delete all feeds, insert the supplied title/script pairs and commit.

        Does not preserve IDs or wrap failures in rollback; iteration or insertion errors
        can leave partial pending work.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE feeds (feed_id INTEGER PRIMARY KEY, feed_title TEXT, feed_script TEXT)')
            >>> host = SimpleNamespace(execute=conn.execute, executemany=conn.executemany, db=SimpleNamespace(driver=SimpleNamespace(conn=conn)))
            >>> _ = conn.execute("INSERT INTO feeds VALUES (9, 'old', '')")
            >>> FeedsMixin.set_feeds(host, [('new', 'recipe')])
            >>> conn.execute('SELECT feed_title, feed_script FROM feeds').fetchall()
            [('new', 'recipe')]
            >>> conn.in_transaction
            False
            >>> conn.close()


        :param feeds: Iterable of (title, script) pairs to insert after clearing the table.
        :return: None.
        """
        self.db.driver.conn.execute("DELETE FROM feeds")
        for title, script in feeds:
            self.db.driver.conn.execute(
                "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?)",
                (title, script),
            )
        self.db.driver.conn.commit()
