
"""
Driver wrapper mixin - for handling metadata concerning the database.
"""

from __future__ import absolute_import, division, print_function, unicode_literals, annotations


from typing import Optional, Iterable, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI



class DriverWrapperMetadataMixin:
    """
    Mixin to access and update metadata for a database.
    """

    driver: "DatabaseDriverAPI"

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Directly get the tables for the currently loaded database

        :return:
        """
        if force_refresh and hasattr(self, "_clear_derived_schema_caches"):
            self._clear_derived_schema_caches()
        return self.driver.direct_get_tables(force_refresh=force_refresh)

    def get_relation_type(self, name: str) -> Optional[str]:
        """Return the relation type for `name` (e.g. 'table' or 'view') if known.

        SQLite-backed schemas increasingly use *views* as compatibility surfaces.
        Attempting to create rows in a view fails at the database level; this helper
        lets higher-level methods provide a clearer error earlier.
        """
        name = str(name)
        try:
            cur = self.execute(
                "SELECT type FROM sqlite_master WHERE name = ? COLLATE NOCASE LIMIT 1;",
                (name,),
            )
            for row in cur:
                if row and row[0] is not None:
                    return str(row[0]).strip().lower()
        except Exception:
            return None
        return None

    def get_column_headings(self, table: str) -> Iterable[str]:
        """
        Gets the column headings for a table in the database.

        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver.direct_get_column_headings(table)

    def get_tables_and_columns(self) -> dict[str, set[str]]:
        """
        Returns a dictionary keyed by the table name with the column headings as the values.

        :return table_and_columns:
        """
        return self.driver.direct_get_tables_and_columns()

    def get_highest_id(self, target_table: str) -> int:
        """
        Gets and returns the highest id in the ids column of a table.

        :param target_table:
        :return:
        """
        return self.driver.direct_get_highest_id(target_table)

    @property
    def user_version(self) -> str:
        """
        Returns the user_version for this database.

        :return:
        """
        return self.driver.user_version

    # Todo: Need to standardize target_table, table and table_name to something
    def get_record_count(self, target_table):
        """
        Returns the number of records in a given table.

        :param target_table:
        :return:
        """
        return self.driver.direct_get_record_count(target_table)

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO READ AND WRITE METADATA TO THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Be nice to be able to get a full readout of all the metadata fields
    # Todo: ... Actually use this?
    # Todo: This maaayyy be typable with a protocol
    def read_metadata(self, field):
        """
        MetaData can be embedded directly into the database. This method allows you to read it.

        :param field: The field that will be read
        :return value: The value of the field from the MetaData table
        """
        return self.driver.direct_read_metadata(md_field_name=field)

    def write_metadata(self, field, value):
        """
        Write the given value to the specified field on the database.

        :param field:
        :param value:
        :return:
        """
        return self.driver.direct_write_metadata(md_field_name=field, md_field_value=value)

    def get_uuid(self):
        """
        Each database should have a unique identifier

        :return:
        """
        return self.driver.direct_get_db_unique_id()

    def set_uuid(self, new_force_value: Optional[str] = None) -> None:
        """
        Sets the database unique id to be a certain value.

        :param new_force_value: If provided the db_unique id will be set to this value. If not it'll be a random uuid4.
        :return status: True/False (actually wither True, or an error is raised)
        """
        status = self.driver.direct_set_db_unique_id(force_value=new_force_value)
        return status
