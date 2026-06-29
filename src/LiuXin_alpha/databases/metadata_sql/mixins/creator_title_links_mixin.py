

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import DatabaseDriverError


class CreatorTitleLinkMacros:
    """

    """

    def break_creator_title_links(self, title_id, creator_type=("author", "authors")):
        """
        Remove links of a certain type between titles and creators

        :param title_id: The title to remove all the creators for
        :param creator_type:
        :return:
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
        Construct a link between a title and a creator.

        :param title_id:
        :param creator_id:
        :param creator_type:
        :return:
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
        Clear the links between a certain title and all creators with a certain link type.

        :param title_id: All creator links to this title will be cleared
        :return:
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
        Check to see that there is an author type link between the title and the creator

        :param title_id:
        :param creator_id:
        :return:
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
        Update the link between the title and the creator - of author type

        :param title_id:
        :param creator_id:
        :param new_priority:
        :return:
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
