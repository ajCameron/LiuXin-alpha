
"""
Macros to deal with the books table.
"""

from __future__ import annotations

from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


# Todo: Clearly in the catalog
class BooksMacrosMixin:
    """
    Methods to deal with books.
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK METHODS

    def update_book_last_modified(self, book_id: int, last_modified: str) -> None:
        """
        Update the last_modified value for the book.

        :param book_id:
        :param last_modified:
        :return:
        """
        update_stmt = "UPDATE books SET book_last_modified = ? WHERE books.book_id = ?;"
        self.db.driver.conn.execute(update_stmt, (last_modified, int(book_id)))
        self.db.driver.conn.commit()

    def set_override_book_path(self, book_id, path):
        """
        A column called book_path is provided so that the user can set an override path for that book.

        :param book_id:
        :param path:
        :return:
        """
        self.execute("UPDATE books SET book_paths=? WHERE book_id=?", (path, book_id))

    #
    # ------------------------------------------------------------------------------------------------------------------

    def read_book_id_with_cover_id_and_cover_nmame(self):
        """
        Designed for the initial read of the covers table - returns a tuple of the form (book_id, cover_id, cover_fname)
        in priority order for the books (so if a book_id appears twice in the sequence the second time it appears
        will correspond to the second cover in the priority order for that book)
        :return:
        """
        stmt = """
                SELECT books.book_id, covers.cover_id, covers.cover_name
                  FROM books
                  JOIN book_cover_links
                    ON books.book_id = book_cover_links.book_cover_link_book_id
                  JOIN covers
                    ON book_cover_links.book_cover_link_cover_id = covers.cover_id
              ORDER BY book_cover_links.book_cover_link_priority DESC;"""
        return self.execute(stmt)

    def read_book_id_with_file_id_file_ext_file_name_and_file_size(self):
        """
        For the initial read of the formats table - returns a tuple of the form
        (book_id, file_id, fmt, file_name, file_size)
        in priority order for the format in the book.
        So, if a book_id appears twice in the sequence the second time it appears will be for the second format in the
        book.
        :return:
        """
        stmt = """
                SELECT books.book_id, files.file_id, files.file_extension, files.file_name, files.file_size
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
              ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(stmt)

    def read_file_backups_for_book(self, book_id):
        """
        One of the options available to the user is to back up a format before making changes to it.
        These backups are noted as such on the database with title-title links.
        Reads and returns the backup title-title links for the given book_id.
        :param book_id:
        :return:
        """
        backup_stmt = """
                SELECT file_file_intralinks.file_file_intralink_primary_id, file_file_intralinks.file_file_intralink_secondary_id
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
                  JOIN file_file_intralinks
                    ON files.file_id = file_file_intralinks.file_file_intralink_primary_id
                 WHERE books.book_id = ?
                 ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(backup_stmt, book_id)

    def read_file_properties_for_book(self, book_id):
        """
        Reads the file properties for a single database book.
        Returns an iterable of tuples - file_id, fmt, file_name, file_size in priority order.
        :return:
        """
        stmt = """
                SELECT files.file_id, files.file_extension, files.file_name, files.file_size
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
                 WHERE books.book_id = ?
                 ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(stmt, book_id)

    def read_book_sizes_sum_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the sum of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT SUM(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    def read_book_sizes_max_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the max of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT MAX(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    def read_book_sizes_min_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the min of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT MIN(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    def set_has_cover(self, book_id, value):
        """
        Set the has_cover field for the specified book.
        :param book_id:
        :param value:
        :return:
        """
        self.db.driver.conn.execute("UPDATE books SET book_has_cover=? WHERE book_id=?;", (value, book_id))
        self.db.driver.conn.commit()


    def set_conversion_options(self, book_id, fmt, options):
        """
        Set a conversion option for a book.
        :param book_id:
        :param fmt:
        :param options:
        :return:
        """
        data = sqlite.Binary(cPickle.dumps(options, -1))
        oid = self.db.driver.conn.get(
            "SELECT conversion_option_id FROM conversion_options "
            "WHERE conversion_option_book=? AND conversion_option_format=?",
            (book_id, fmt.upper()),
            all=False,
        )

        if oid:
            self.db.driver.conn.execute(
                "UPDATE conversion_options " "SET conversion_option_data=? " "WHERE conversion_option_id=?",
                (data, oid),
            )
        else:
            self.db.driver.conn.execute(
                "INSERT INTO conversion_options"
                "(conversion_option_book,"
                "conversion_option_format,"
                "conversion_option_data) VALUES (?,?,?)",
                (book_id, fmt.upper(), data),
            )
        self.db.driver.conn.commit()

    def delete_conversion_options(self, book_id, fmt, commit=True):
        """
        Delete a conversion option for a format from a given id.
        :param book_id:
        :param fmt:
        :param commit:
        :return:
        """
        stmt = "DELETE FROM conversion_options WHERE conversion_option_book=? AND conversion_option_format=?"
        self.db.driver.conn.execute(stmt, (book_id, fmt.upper()))
        if commit:
            self.db.driver.conn.commit()
