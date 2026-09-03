"""Metadata SQL macros for legacy file catalogue rows."""




class CMFilesMacrosMixin:
    """Implement legacy file-row metadata macros."""


    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FILE VALUES METHODS

    def set_file_name(self, file_id, new_fname):
        """
        Set the file name for a specific file stored in the files table.
        :param file_id:
        :param new_fname:
        :return:
        """
        stmt = "UPDATE files SET file_name = ? WHERE file_id = ?;"
        self.execute(stmt, (new_fname, file_id))

    def set_file_size(self, file_id, size):
        """
        Sets the size of a file.
        :param file_id:
        :param size:
        :return:
        """
        stmt = "UPDATE files SET file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (size, file_id))

    def set_file_size_and_name(self, file_id, size, fname):
        """
        Sets both the size and the name at the same time.
        :param file_id:
        :param size:
        :param fname:
        :return:
        """
        stmt = "UPDATE files SET file_name = ?, file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (fname, size, file_id))

    # Todo: Rename to make this clear it takes out a file row, not a physical file - and the one below it
    def delete_file_by_id(self, file_id):
        """
        Deletes the file given by the specified file_id.
        :param file_id:
        :return:
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.execute(stmt, (file_id,))

    def delete_files_by_id(self, file_ids):
        """
        Delete all the files given by the specified ids.
        :param file_ids:
        :return:
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.executemany(stmt, file_ids)

    #
    # ------------------------------------------------------------------------------------------------------------------
