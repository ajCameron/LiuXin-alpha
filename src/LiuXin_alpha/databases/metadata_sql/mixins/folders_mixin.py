"""Metadata SQL macros for folder catalogue rows."""




class FoldersMacrosMixin:
    """
    Macros to interact with folders.
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO REPLACE IN PHYSICAL ASSET PATHS

    def replace_in_folder_store_path(self, target_str: str, replacement: str) -> None:
        """
        Run a replacement in the folder_store_path column of the folder_stores table.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_path = replace(folder_store_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_store_marker_path(self, target_str: str, replacement: str) -> None:
        """
        Run a replacement in the folder_store_marker_path column of the folder stores table.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_marker_path = replace(folder_store_marker_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_path(self, target_str, replacement):
        """
        Run a replacement in the folder_path column of the folders table.

        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folders SET folder_path = replace(folder_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_cover_path(self, target_str, replacement):
        """
        Run a replacement in the cover_path column of the covers table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE covers SET cover_path = replace(cover_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_file_path(self, target_str, replacement):
        """
        Run a replacement in the cover_path column of the covers table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE files SET file_path = replace(file_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    #
    # ------------------------------------------------------------------------------------------------------------------
