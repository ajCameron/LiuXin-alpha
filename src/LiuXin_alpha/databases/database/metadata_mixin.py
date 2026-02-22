
import uuid

from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class DatabaseMetadataMixin:
    """
    Mixin to handle the database metadata.
    """
    @property
    def uuid(self):
        if self._uuid is not None:
            return self._uuid
        else:
            self._uuid = self.driver_wrapper.get_uuid()
            return self._uuid

    @uuid.setter
    def uuid(self, value):
        self._uuid = value
        self.driver_wrapper.set_uuid(value)

    @property
    def library_id(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.

        :return:
        """
        if getattr(self, "_library_id_", None) is None:
            ans = self.driver_wrapper.get("SELECT library_id_uuid FROM library_id", all=False)
            if ans is None:
                ans = str(uuid.uuid4())
                self.library_id = ans
            else:
                self._library_id_ = ans
        return self._library_id_

    @library_id.setter
    def library_id(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._library_id_ = six_unicode(value)
        self.macros.set_library_id(value)

    @property
    def database_version(self):
        """
        The UUID for this library. As long as the user only operates on libraries with LiuXin, it will be unique.
        :return:
        """
        if getattr(self, "_database_version_", None) is None:
            c = self.conn.cursor()
            version_val = None

            for row in c.execute("SELECT database_version_version FROM database_version;"):
                version_val = row[0]
            self._database_version_ = version_val
        return self._database_version_

    @database_version.setter
    def database_version(self, value):
        """
        Setter function for the library id - handles updating the database with the new id.
        :param value:
        :return:
        """
        self._database_version_ = six_unicode(value)
        self.macros.set_database_version(value)


    # ----------------------------------------------------------------
    #
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE

    def get_tables(self, force_refresh: bool = False):
        """
        Directly get the tables for the currently loaded database
        :return:
        """
        return self.driver_wrapper.get_tables(force_refresh=force_refresh)

    # Methods to get basic information about the database start here
    def get_column_headings(self, table):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_column_headings(table)

    def get_view_column_headings(self, view):
        """
        Gets the column headings for a table in the database.
        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_view_column_headings(view)

    def get_tables_and_columns(self):
        """
        Returns a dictionary keyed by the table name with the column headings as the values.
        :return table_and_columns:
        """
        return self.driver_wrapper.get_tables_and_columns()


    def get_record_count(self, target_table):
        """
        Returns the number of records in a given table.
        :param target_table:
        :return:
        """
        return self.driver_wrapper.get_record_count(target_table)

    def get_max(self, column):
        """
        Get the maximum value from the given column.
        :param column:
        :return:
        """
        return self.driver.direct_get_max(column)

    def get_min(self, column):
        """
        Get the minimum value from the given column.
        :param column:
        :return:
        """
        return self.driver.direct_get_min(column)


    def row_counts(self):
        """
        Returns a string representation of the row counts for every table in the DatabasePing.
        :return:
        """
        ans = list()
        ans.append("LiuXin _Database: Table row_counts")
        ans.append("database_uuid: {}".format(self.uuid))

        for table_type in [
            "main_tables",
            "interlink_tables",
            "intralink_tables",
            "helper_tables",
        ]:

            type_tables = sorted([t for t in deepcopy(object.__getattribute__(self, table_type))])
            ans.append("\n{}:\n".format(table_type))

            for table in type_tables:
                ans.append("{}: {}".format(table, self.get_record_count(table)))

        return "\n".join(ans)


    #
    # ----------------------------------------------------------------
