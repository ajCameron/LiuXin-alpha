


class BooksMacrosMixin:
    """
    Methods to deal with books.
    """
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK METHODS

    def update_book_last_modified(self, book_id, last_modified):
        """
        Update the last_modified value for the book.
        :param book_id:
        :param last_modified:
        :return:
        """
        update_stmt = "UPDATE books SET book_last_modified = ? WHERE books.book_id = ?;"
        self.db.driver.conn.execute(update_stmt, (last_modified, int(book_id)))
        self.db.driver.conn.commit()

    #
    # ------------------------------------------------------------------------------------------------------------------
