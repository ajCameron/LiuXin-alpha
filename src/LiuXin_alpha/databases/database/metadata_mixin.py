
"""
Mixin to handle metadata for the actual database.
"""

from __future__ import annotations

from typing import Any, Iterator, Mapping, TYPE_CHECKING

import uuid

from copy import deepcopy

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.macro_types import (
    CanonicalIdentity,
    NormalizedIdentityMigrationReport,
)
from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
from LiuXin_alpha.databases.schema_specs import LinkCapabilities
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class DatabaseMetadataMixin:
    """
    Mixin to handle the database metadata.
    """
    @property
    def uuid(self: "DatabaseAPI") -> str:
        """
        Return the uuid of the database.

        :return:
        """
        if self._uuid is not None:
            return self._uuid
        else:
            self._uuid = self.driver_wrapper.get_uuid()
            return self._uuid

    @uuid.setter
    def uuid(self: "DatabaseAPI", value: str) -> None:
        """
        Set the uuid of the database.

        :param value:
        :return:
        """
        self._uuid = value
        self.driver_wrapper.set_uuid(value)

    @property
    def library_id(self: "DatabaseAPI") -> str:
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
    def library_id(self: "DatabaseAPI", value: str) -> None:
        """
        Setter function for the library id - handles updating the database with the new id.

        :param value:
        :return:
        """
        self._library_id_ = six_unicode(value)
        self.macros.set_library_id(value)

    @property
    def database_version(self: "DatabaseAPI") -> str:
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
    def database_version(self: "DatabaseAPI", value: str) -> None:
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

    def get_tables(self: "DatabaseAPI", force_refresh: bool = False) -> list[str]:
        """
        Directly get the tables for the currently loaded database

        :return:
        """
        return self.driver_wrapper.get_tables(force_refresh=force_refresh)

    # Methods to get basic information about the database start here
    def get_column_headings(self: "DatabaseAPI", table: str) -> list[str]:
        """
        Gets the column headings for a table in the database.

        :param table:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_column_headings(table)

    def get_declared_column_datatype(self: "DatabaseAPI", table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        :param table:
        :param column:
        :return:
        """
        return self.driver_wrapper.get_declared_column_datatype(table, column)

    def get_link_capabilities(
        self: "DatabaseAPI",
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """Return the type/priority capabilities of one interlink or intralink."""

        return self.driver_wrapper.get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )

    def get_case_sensitivity(self: "DatabaseAPI", table: str, column: str) -> bool:
        """
        Return whether text equality for this column is case-sensitive.

        :param table:
        :param column:
        :return:
        """
        return self.driver_wrapper.get_case_sensitivity(table, column)

    def get_column_metadata(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnMetadata:
        """Return the complete semantic/writer policy for one column."""

        return self.driver_wrapper.get_column_metadata(table, column)

    def set_column_metadata(
        self: "DatabaseAPI",
        metadata: ColumnMetadata,
    ) -> None:
        """Persist the complete semantic/writer policy for one column."""

        self.driver_wrapper.set_column_metadata(metadata)

    def get_semantic_role(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """Return the semantic role for one column."""

        return self.driver_wrapper.get_semantic_role(table, column)

    def set_semantic_role(
        self: "DatabaseAPI",
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist the semantic role for one column."""

        self.driver_wrapper.set_semantic_role(table, column, semantic_role)

    def get_normalization_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

        return self.driver_wrapper.get_normalization_profile(table, column)

    def set_normalization_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist the comparison-normalization profile for one column."""

        self.driver_wrapper.set_normalization_profile(
            table,
            column,
            normalization_profile,
        )

    def get_comparison_column(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> str | None:
        """Return the derived comparison column, if any."""

        return self.driver_wrapper.get_comparison_column(table, column)

    def get_normalized_identity_spec(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the normalized row-identity declaration for a display column."""

        return self.driver_wrapper.get_normalized_identity_spec(table, value_column)

    def iter_normalized_identity_specs(
        self: "DatabaseAPI",
    ) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized row identity declared by the database."""

        yield from self.driver_wrapper.iter_normalized_identity_specs()

    def derive_identity_value(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """Derive the declared normalized identity for a display value."""

        return self.macros.derive_identity_value(table, value_column, value)

    def get_canonical_identity(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve a display value to its complete stored canonical identity."""

        return self.macros.get_canonical_identity(
            table,
            value_column,
            value,
            scope_values=scope_values,
            id_column=id_column,
        )

    def get_canonical_identity_by_key(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve an already-derived identity to its canonical stored row."""

        return self.macros.get_canonical_identity_by_key(
            table,
            value_column,
            identity_value,
            scope_values=scope_values,
            id_column=id_column,
        )

    def get_canonical_value(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a display value and return its canonical stored spelling."""

        return self.macros.get_canonical_value(
            table,
            value_column,
            value,
            scope_values=scope_values,
        )

    def get_canonical_value_by_identity(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a derived identity and return its canonical stored spelling."""

        return self.macros.get_canonical_value_by_identity(
            table,
            value_column,
            identity_value,
            scope_values=scope_values,
        )

    def audit_normalized_identities(
        self: "DatabaseAPI",
    ) -> NormalizedIdentityMigrationReport:
        """Report stale keys and collisions without changing the database."""

        return self.macros.audit_normalized_identities()

    def migrate_normalized_identities(
        self: "DatabaseAPI",
    ) -> NormalizedIdentityMigrationReport:
        """Install, backfill, and index normalized identities atomically."""

        return self.macros.migrate_normalized_identities()

    def set_comparison_column(
        self: "DatabaseAPI",
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist the derived comparison column for one column."""

        self.driver_wrapper.set_comparison_column(
            table,
            column,
            comparison_column,
        )

    def get_empty_value_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one column."""

        return self.driver_wrapper.get_empty_value_policy(table, column)

    def set_empty_value_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist the empty-value policy for one column."""

        self.driver_wrapper.set_empty_value_policy(
            table,
            column,
            empty_value_policy,
        )

    def get_merge_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnMergePolicy:
        """Return the merge policy for one column."""

        return self.driver_wrapper.get_merge_policy(table, column)

    def set_merge_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist the merge policy for one column."""

        self.driver_wrapper.set_merge_policy(table, column, merge_policy)

    def get_validation_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one column."""

        return self.driver_wrapper.get_validation_profile(table, column)

    def set_validation_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist the validation profile for one column."""

        self.driver_wrapper.set_validation_profile(
            table,
            column,
            validation_profile,
        )

    def set_case_sensitivity(
        self: "DatabaseAPI",
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
        self.driver_wrapper.set_case_sensitivity(table, column, case_sensitive)

    def is_column_case_sensitive(self: "DatabaseAPI", table: str, column: str) -> bool:
        """Compatibility alias for :meth:`get_case_sensitivity`."""

        return self.get_case_sensitivity(table, column)

    def set_column_case_sensitive(
        self: "DatabaseAPI",
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Compatibility alias for :meth:`set_case_sensitivity`."""

        self.set_case_sensitivity(table, column, case_sensitive)

    def get_view_column_headings(self: "DatabaseAPI", view: str) -> list[str]:
        """
        Gets the column headings for a table in the database.

        :param view:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver_wrapper.get_view_column_headings(view)

    def get_tables_and_columns(self: "DatabaseAPI") -> dict[str, list[str]]:
        """
        Returns a dictionary keyed by the table name with the column headings as the values.

        :return table_and_columns:
        """
        return self.driver_wrapper.get_tables_and_columns()

    def get_record_count(self: "DatabaseAPI", target_table: str) -> int:
        """
        Returns the number of records in a given table.

        :param target_table:
        :return:
        """
        return self.driver_wrapper.get_record_count(target_table)

    def get_max(self: "DatabaseAPI", column: str) -> int:
        """
        Get the maximum value from the given column.

        :param column:
        :return:
        """
        return self.driver.direct_get_max(column)

    def get_min(self: "DatabaseAPI", column: str) -> int:
        """
        Get the minimum value from the given column.

        :param column:
        :return:
        """
        return self.driver.direct_get_min(column)

    def row_counts(self: "DatabaseAPI") -> dict[str, int]:
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
