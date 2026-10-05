"""
Provide metadata SQL operations for files.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class CMFilesMacrosMixin:
    """
    Implement the files operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.set_file_name(1, "example.epub")  # doctest: +SKIP
    """


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FILE VALUES METHODS

    def set_file_name(self, file_id, new_fname):
        """
        Write a catalogue file_name value through the host execution helper.

        Does not rename a filesystem entry.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.set_file_name(1, "example.epub")  # doctest: +SKIP


        :param file_id: Catalogue file-row identifier; no physical file is accessed.
        :param new_fname: Replacement catalogue filename.
        :return: None.
        """
        stmt = "UPDATE files SET file_name = ? WHERE file_id = ?;"
        self.execute(stmt, (new_fname, file_id))

    def set_file_size(self, file_id, size):
        """
        Write the recorded file_size without measuring or validating a physical file.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.set_file_size(1, 123)  # doctest: +SKIP


        :param file_id: Catalogue file-row identifier; no physical file is accessed.
        :param size: Recorded size value; no filesystem measurement occurs.
        :return: None.
        """
        stmt = "UPDATE files SET file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (size, file_id))

    def set_file_size_and_name(self, file_id, size, fname):
        """
        Write recorded filename and size together for one catalogue row.

        Does not modify filesystem content.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.set_file_size_and_name(1, 123, "example.epub")  # doctest: +SKIP


        :param file_id: Catalogue file-row identifier; no physical file is accessed.
        :param size: Recorded size value; no filesystem measurement occurs.
        :param fname: Replacement catalogue filename.
        :return: None.
        """
        stmt = "UPDATE files SET file_name = ?, file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (fname, size, file_id))

    # Todo: Rename to make this clear it takes out a file row, not a physical file - and the one below it
    def delete_file_by_id(self, file_id):
        """
        Delete one files catalogue row without removing a physical file.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_file_by_id(1)  # doctest: +SKIP


        :param file_id: Catalogue file-row identifier; no physical file is accessed.
        :return: None.
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.execute(stmt, (file_id,))

    def delete_files_by_id(self, file_ids):
        """
        Batch-delete files catalogue rows using bindings passed unchanged to executemany.

        No physical files are removed.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.delete_files_by_id([(1,), (2,)])  # doctest: +SKIP


        :param file_ids: Batch bindings forwarded unchanged to executemany.
        :return: None.
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.executemany(stmt, file_ids)

    #
    # ------------------------------------------------------------------------------------------------------------------
