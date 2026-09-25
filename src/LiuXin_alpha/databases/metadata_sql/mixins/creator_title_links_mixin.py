"""
Provide metadata SQL operations for creator title links.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""



from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import DatabaseDriverError


class CreatorTitleLinkMacros:
    """
    Implement the creator title links operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.break_creator_title_links(1)  # doctest: +SKIP
    """

    def break_creator_title_links(self, title_id, creator_type=("author", "authors")):
        """
        Delete creator links of the requested types for one title or a title batch.

        Interpolates creator_type directly into IN; callers must provide trusted,
        SQL-compatible tuple syntax. Integer titles use one execution; iterables become
        one-cell binding tuples. Batch exceptions are logged and translated to
        DatabaseDriverError.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.break_creator_title_links(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param creator_type: Trusted type tuple interpolated into the IN expression; not
            parameter-bound.
        :return: None.
        """
        del_stmt = (
            "DELETE FROM creator_title_links "
            "WHERE creator_title_link_title_id=? AND creator_title_link_type IN {};".format(creator_type)
        )

        if isinstance(title_id, int):
            self.execute(del_stmt, (title_id,))
        else:
            try:
                self.executemany(del_stmt, ((k,) for k in title_id))
            except Exception as e:
                err_str = "db.executemany failed"
                err_str = default_log.log_exception(err_str, e, "ERROR")
                raise DatabaseDriverError(err_str)

    def make_creator_title_links(self, title_id=None, creator_id=None, id_pairs=None, creator_type="authors"):
        """
        Insert creator/title links with a globally computed minimum priority minus one.

        The SQL always stores authors, ignoring creator_type. id_pairs takes precedence over
        individual IDs. On an empty link table MIN yields NULL; priorities are not scoped to
        the title.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.make_creator_title_links(title_id=1, creator_id=2)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param creator_id: Creator identifier bound to the query or relationship.
        :param id_pairs: Optional iterable of (title_id, creator_id) bindings; takes
            precedence over individual IDs.
        :param creator_type: Ignored compatibility argument; SQL always stores authors.
        :return: None.
        """
        insert_stmt = (
            "INSERT INTO creator_title_links "
            "(creator_title_link_title_id, creator_title_link_creator_id, "
            "creator_title_link_type, creator_title_link_priority) "
            "SELECT ?, ?, 'authors', MIN(creator_title_link_priority) - 1 FROM creator_title_links;"
        )

        if id_pairs is not None:
            self.executemany(insert_stmt, id_pairs)
        else:
            self.execute(insert_stmt, (title_id, creator_id))


    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLE CREATOR METHODS
    # Todo: We need to re-write this entirely
    def clear_title_creator_links_for_given_type_and_title(
            self,
            title_id: str) -> None:
        """
        Delete authors-type creator links for one title and commit.

        The type is fixed to plural authors; other link types remain.

        Example:
            >>> metadata_sql.clear_title_creator_links_for_given_type_and_title(1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :return: None.
        """
        stmt = (
            "DELETE FROM creator_title_links "
            "WHERE creator_title_link_title_id = ? AND creator_title_link_type='authors';"
        )
        self.db.driver.conn.execute(stmt, (title_id,))
        self.db.driver.conn.commit()

    # Todo: Add type filtering
    def check_for_title_author_link(
            self,
            title_id: int,
            creator_id: int) -> bool:
        """
        Read an authors-type link ID for the supplied creator/title pair.

        Example:
            >>> metadata_sql.check_for_title_author_link(1, 1)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param creator_id: Creator identifier bound to the query or relationship.
        :return: Connection.get(all=False) result, normally link ID or None despite the bool
            annotation.
        """
        stmt = (
            "SELECT creator_title_link_id FROM creator_title_links "
            "WHERE creator_title_link_title_id = ? "
            "AND creator_title_link_creator_id = ? "
            "AND creator_title_links.creator_title_link_type='authors';"
        )
        return self.db.driver.conn.get(stmt, (title_id, creator_id), all=False)

    def update_title_author_link_priority(self, title_id: int, creator_id: int, new_priority: int) -> None:
        """
        Update priority on every matching authors-type creator/title link and commit.

        Example:
            >>> metadata_sql.update_title_author_link_priority(1, 1, 3)  # doctest: +SKIP


        :param title_id: Title identifier bound to the operation; batch handling, where
            supported, is described above.
        :param creator_id: Creator identifier bound to the query or relationship.
        :param new_priority: Replacement relationship priority, bound as supplied.
        :return: None.
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
