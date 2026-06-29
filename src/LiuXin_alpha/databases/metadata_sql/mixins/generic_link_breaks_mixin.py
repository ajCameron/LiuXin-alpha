


class CMLangTitleLinkMixin:

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK BREAKING METHODS

    def break_lang_title_links(self, title_id, link_type=None):
        """
        Break links between the given title and any relevant languages

        :param title_id:
        :param link_type: Defaults to None - which will remove all links between the given title row and any languages
        :return:
        """
        if link_type is not None:
            stmt = (
                "DELETE FROM language_title_links WHERE language_title_link_title_id = ? "
                "AND language_title_link_type = '{}';".format(link_type)
            )
            self.execute(stmt, (title_id,))
        else:
            stmt = "DELETE FROM language_title_links WHERE language_title_link_title_id = ?;".format(link_type)
            self.execute(stmt, (title_id,))

    # Todo: This is a bad name - it breaks generic LINKS
    def break_generic_link(self, link_table, link_col, remove_id, link_type=None):
        """
        Break a generic link - all links matching the given remove_id will be deleted.

        :param link_table:
        :param link_col:
        :param remove_id: If remove_id is an int, only that row will be removed. If it's an iterable then all the ids
                          in that iterable will be removed.
        :param link_type: If provided
        :return:
        """
        if link_type is None:
            stmt = "DELETE FROM {0} WHERE {1} = ?;".format(link_table, link_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id,))
            else:
                self.executemany(stmt, remove_id)
        else:
            link_table_col = self.db.driver_wrapper.get_column_base(link_table)
            link_table_type_col = "{}_type".format(link_table_col)
            stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, link_col, link_table_type_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id, link_type))
            else:
                # self.executemany(stmt, remove_id)
                raise NotImplementedError

    def break_generic_single_link(self, link_table, left_link_col, right_link_col, left_id, right_id):
        """
        Break a specified link between two entities.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, left_link_col, right_link_col)
        self.execute(del_stmt, (left_id, right_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
