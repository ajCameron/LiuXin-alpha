
"""
Provide metadata SQL operations for books.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""

from __future__ import annotations

from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class BooksMacrosMixin:
    """
    Implement the books operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.update_book_last_modified(1, "2024-01-01")  # doctest: +SKIP
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK METHODS

    def update_book_last_modified(self, book_id: int, last_modified: str) -> None:
        """
        Update book_last_modified for one integer-coerced book ID and commit the live connection.

        Example:
            >>> metadata_sql.update_book_last_modified(1, "2024-01-01")  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :param last_modified: Replacement modification timestamp bound without conversion.
        :return: None.
        """
        update_stmt = "UPDATE books SET book_last_modified = ? WHERE books.book_id = ?;"
        self.db.driver.conn.execute(update_stmt, (last_modified, int(book_id)))
        self.db.driver.conn.commit()

    def set_override_book_path(self, book_id, path):
        """
        Write the book_paths override through the host execution helper.

        Does not create, move or inspect physical files.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.set_override_book_path(1, "books/example")  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :param path: Stored override path; this writes book_paths, not a filesystem entry.
        :return: None.
        """
        self.execute("UPDATE books SET book_paths=? WHERE book_id=?", (path, book_id))

    #
    # ------------------------------------------------------------------------------------------------------------------

    def read_book_id_with_cover_id_and_cover_nmame(self):
        """
        Read book, cover ID and cover-name triples ordered by descending link priority.

        Uses direct book_cover_links and returns all matching links; the historical
        misspelling is retained.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_book_id_with_cover_id_and_cover_nmame()  # doctest: +SKIP


        :return: Execution cursor/iterable of (book_id, cover_id, cover_name) rows.
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
        Read directly linked book/file metadata ordered by descending link priority.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_book_id_with_file_id_file_ext_file_name_and_file_size()  # doctest: +SKIP


        :return: Execution cursor/iterable of (book_id, file_id, extension, name, size)
            rows.
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
        Read all outgoing file intralink endpoint pairs for a book's linked files.

        Despite the name, no backup-type predicate is applied. Orders by descending
        book/file priority and passes the scalar book ID to the host helper.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_file_backups_for_book(1)  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :return: Execution cursor/iterable of (primary_file_id, secondary_file_id) pairs.
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
        Read file ID, extension, name and size for a book's direct file links.

        Orders by descending link priority and passes the book ID as a scalar binding
        argument.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_file_properties_for_book(1)  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :return: Execution cursor/iterable of four-cell file property rows.
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
        Read each book with the SUM of file sizes reached through its folders.

        Uses nested IN queries over book_folder_links and file_folder_links, avoiding
        duplicate file IDs. Unlinked books retain an aggregate NULL; no explicit ordering is
        applied.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_book_sizes_sum_mode()  # doctest: +SKIP


        :return: Execution cursor/iterable of (book_id, aggregate_size) rows.
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
        Read each book with the MAX of file sizes reached through its folders.

        Uses nested IN queries over book_folder_links and file_folder_links, avoiding
        duplicate file IDs. Unlinked books retain an aggregate NULL; no explicit ordering is
        applied.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_book_sizes_max_mode()  # doctest: +SKIP


        :return: Execution cursor/iterable of (book_id, aggregate_size) rows.
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
        Read each book with the MIN of file sizes reached through its folders.

        Uses nested IN queries over book_folder_links and file_folder_links, avoiding
        duplicate file IDs. Unlinked books retain an aggregate NULL; no explicit ordering is
        applied.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.read_book_sizes_min_mode()  # doctest: +SKIP


        :return: Execution cursor/iterable of (book_id, aggregate_size) rows.
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
        Write book_has_cover as supplied and commit the live connection.

        Does not verify that a cover file or link exists.

        Example:
            >>> metadata_sql.set_has_cover(1, True)  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :param value: Replacement stored value; not coerced by this helper.
        :return: None.
        """
        self.db.driver.conn.execute("UPDATE books SET book_has_cover=? WHERE book_id=?;", (value, book_id))
        self.db.driver.conn.commit()


    def set_conversion_options(self, book_id, fmt, options):
        """
        Attempt to pickle and upsert options for an uppercased format.

        The module does not import sqlite or cPickle: the first expression normally raises
        NameError before SQL. If those globals are supplied externally, it looks up an
        existing option row, updates or inserts serialized bytes, and commits.

        Example:
            >>> try:
            ...     BooksMacrosMixin().set_conversion_options(1, "epub", {})
            ... except NameError:
            ...     print("missing legacy serialization imports")
            missing legacy serialization imports


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :param fmt: Format string uppercased before matching conversion options.
        :param options: Conversion options intended for pickling; currently blocked by
            missing imports.
        :return: None only if the legacy path completes; normally raises NameError.
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
        Delete matching book/format conversion options, optionally committing.

        Uppercases fmt and binds both values. With commit=False, transaction ownership
        remains with the caller.

        Example:
            >>> metadata_sql.delete_conversion_options(1, "epub")  # doctest: +SKIP


        :param book_id: Book identifier bound to book_id or the owning relationship column.
        :param fmt: Format string uppercased before matching conversion options.
        :param commit: Whether to commit the live connection after deletion.
        :return: None.
        """
        stmt = "DELETE FROM conversion_options WHERE conversion_option_book=? AND conversion_option_format=?"
        self.db.driver.conn.execute(stmt, (book_id, fmt.upper()))
        if commit:
            self.db.driver.conn.commit()
