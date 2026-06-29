
"""
Delete macros - responsible for customized deletion.
"""


class CMDeletionMacros:
    """
    Custom macros for deleting from various tables.
    """

    def delete_item_by_id(self, item_table, item_id_col, item_id):
        """
        Delete an item from a given table.

        :param item_table:
        :param item_id_col:
        :param item_id:
        :return:
        """
        # Todo: Really needs some kind of checking
        del_stmt = "DELETE FROM {} WHERE {}=?".format(item_table, item_id_col)
        if isinstance(item_id, int):
            self.execute(del_stmt, (item_id,))
        else:
            self.executemany(del_stmt, item_id)

    def delete_title(self, title_id):
        """
        Delete a title - and a book if it exists.
        :param title_id:
        :return:
        """
        title_del_stmt = "DELETE FROM titles WHERE title_id = ?;"
        self.execute(title_del_stmt, (title_id,))
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (title_id,))

    def delete_book(self, book_id):
        """
        Just delete a book from the system python
        :param book_id:
        :return:
        """
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (book_id,))
