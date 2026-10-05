"""
Provide metadata SQL operations for folders.

These helpers target the stored schema named in their SQL. The host supplies db
and/or execution methods. Per-method notes distinguish explicit live-connection
commits from delegated transaction handling; filesystem assets are never moved by
these helpers.
"""




class FoldersMacrosMixin:
    """
    Implement the folders operations used by MetadataSQL.

    Requires a compatible owner database or host query methods. Backend/schema errors
    propagate except where a method explicitly documents suppression.

    Example:
        >>> metadata_sql.replace_in_folder_store_path("old/", "new/")  # doctest: +SKIP
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO REPLACE IN PHYSICAL ASSET PATHS

    def replace_in_folder_store_path(self, target_str: str, replacement: str) -> None:
        """
        Replace every occurrence of a literal substring in folder_stores.folder_store_path.

        Runs SQL replace() for all rows, with no WHERE filter or path-component checks.
        Values are parameter-bound; physical files and directories are untouched.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.replace_in_folder_store_path("old/", "new/")  # doctest: +SKIP


        :param target_str: Literal substring passed to SQL replace(), without path-component
            validation.
        :param replacement: Replacement substring stored in every matching path cell.
        :return: None.
        """
        replace_sql = "UPDATE folder_stores SET folder_store_path = replace(folder_store_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        """
        Replace every occurrence of a literal substring in folder_stores.folder_store_marker_path.

        Runs SQL replace() for all rows, with no WHERE filter or path-component checks.
        Values are parameter-bound; physical files and directories are untouched.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.replace_in_folder_store_marker_path("old/", "new/")  # doctest: +SKIP


        :param target_str: Literal substring passed to SQL replace(), without path-component
            validation.
        :param replacement: Replacement substring stored in every matching path cell.
        :return: None.
        """
        replace_sql = "UPDATE folder_stores SET folder_store_marker_path = replace(folder_store_marker_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_path(self, target_str, replacement):
        """
        Replace every occurrence of a literal substring in folders.folder_path.

        Runs SQL replace() for all rows, with no WHERE filter or path-component checks.
        Values are parameter-bound; physical files and directories are untouched.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.replace_in_folder_path("old/", "new/")  # doctest: +SKIP


        :param target_str: Literal substring passed to SQL replace(), without path-component
            validation.
        :param replacement: Replacement substring stored in every matching path cell.
        :return: None.
        """
        replace_sql = "UPDATE folders SET folder_path = replace(folder_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_cover_path(self, target_str, replacement):
        """
        Replace every occurrence of a literal substring in covers.cover_path.

        Runs SQL replace() for all rows, with no WHERE filter or path-component checks.
        Values are parameter-bound; physical files and directories are untouched.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> metadata_sql.replace_in_cover_path("old/", "new/")  # doctest: +SKIP


        :param target_str: Literal substring passed to SQL replace(), without path-component
            validation.
        :param replacement: Replacement substring stored in every matching path cell.
        :return: None.
        """
        replace_sql = "UPDATE covers SET cover_path = replace(cover_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_file_path(self, target_str, replacement):
        """
        Replace every occurrence of a literal substring in files.file_path.

        Runs SQL replace() for all rows, with no WHERE filter or path-component checks.
        Values are parameter-bound; physical files and directories are untouched.

        Transaction and cache behavior for delegated SQL follows the host
        execute/executemany implementation; this method adds no separate transaction guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(':memory:')
            >>> _ = conn.execute('CREATE TABLE files (file_path TEXT)')
            >>> _ = conn.executemany('INSERT INTO files VALUES (?)', [('old/old/book.epub',), ('keep/book.epub',)])
            >>> FoldersMacrosMixin.replace_in_file_path(SimpleNamespace(execute=conn.execute), 'old/', 'new/')
            >>> conn.execute('SELECT file_path FROM files ORDER BY file_path').fetchall()
            [('keep/book.epub',), ('new/new/book.epub',)]
            >>> conn.close()


        :param target_str: Literal substring passed to SQL replace(), without path-component
            validation.
        :param replacement: Replacement substring stored in every matching path cell.
        :return: None.
        """
        replace_sql = "UPDATE files SET file_path = replace(file_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    #
    # ------------------------------------------------------------------------------------------------------------------
