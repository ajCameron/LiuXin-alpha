
"""
Driver wrapper mixin - for handling metadata concerning the database.
"""

from __future__ import absolute_import, division, print_function, unicode_literals, annotations


from typing import Optional, Iterable, Iterator, TYPE_CHECKING

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec

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

    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        :param table:
        :param column:
        :return:
        """
        return self.driver.direct_get_declared_column_datatype(table, column)

    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Return whether text equality for this column is case-sensitive.

        :param table:
        :param column:
        :return:
        """
        return self.driver.direct_get_case_sensitivity(table, column)

    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """Return the complete semantic/writer policy for one column."""

        return self.driver.direct_get_column_metadata(table, column)

    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """Persist the complete semantic/writer policy for one column."""

        self.driver.direct_set_column_metadata(metadata)

    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """Return the semantic role for one column."""

        return self.driver.direct_get_semantic_role(table, column)

    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist the semantic role for one column."""

        self.driver.direct_set_semantic_role(table, column, semantic_role)

    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

        return self.driver.direct_get_normalization_profile(table, column)

    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist the comparison-normalization profile for one column."""

        self.driver.direct_set_normalization_profile(
            table,
            column,
            normalization_profile,
        )

    def get_comparison_column(self, table: str, column: str) -> str | None:
        """Return the derived comparison column, if any."""

        return self.driver.direct_get_comparison_column(table, column)

    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the normalized row-identity declaration for a display column."""

        return self.driver.direct_get_normalized_identity_spec(table, value_column)

    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized row identity declared by the database."""

        yield from self.driver.direct_iter_normalized_identity_specs()

    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist the derived comparison column for one column."""

        self.driver.direct_set_comparison_column(
            table,
            column,
            comparison_column,
        )

    def get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one column."""

        return self.driver.direct_get_empty_value_policy(table, column)

    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist the empty-value policy for one column."""

        self.driver.direct_set_empty_value_policy(
            table,
            column,
            empty_value_policy,
        )

    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """Return the merge policy for one column."""

        return self.driver.direct_get_merge_policy(table, column)

    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist the merge policy for one column."""

        self.driver.direct_set_merge_policy(table, column, merge_policy)

    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one column."""

        return self.driver.direct_get_validation_profile(table, column)

    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist the validation profile for one column."""

        self.driver.direct_set_validation_profile(
            table,
            column,
            validation_profile,
        )

    def set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Persist text equality policy for one column.

        :param table:
        :param column:
        :param case_sensitive:
        :return:
        """
        self.driver.direct_set_case_sensitivity(table, column, case_sensitive)

    def is_column_case_sensitive(self, table: str, column: str) -> bool:
        """Compatibility alias for :meth:`get_case_sensitivity`."""

        return self.get_case_sensitivity(table, column)

    def set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Compatibility alias for :meth:`set_case_sensitivity`."""

        self.set_case_sensitivity(table, column, case_sensitive)

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
