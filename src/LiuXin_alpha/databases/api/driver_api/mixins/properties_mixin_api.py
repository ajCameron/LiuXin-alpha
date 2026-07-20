from __future__ import annotations

import abc
from typing import Optional, Iterable, Iterator, Any

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
from LiuXin_alpha.databases.schema_specs import LinkCapabilities


class DriverDatabasePropertiesMixinAPI(abc.ABC):
    """
    Contains the API for driver level database properties mixins.
    """

    @abc.abstractmethod
    def direct_get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """Return type/priority capabilities for an interlink or intralink."""

    @abc.abstractmethod
    def direct_is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a type column."""

    @abc.abstractmethod
    def direct_is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a priority column."""

    @abc.abstractmethod
    def direct_get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        :param table:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Return whether text equality for this column is case-sensitive.

        :param table:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """Return the complete semantic/writer policy for one column."""

    @abc.abstractmethod
    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """Persist the complete semantic/writer policy for one column."""

    @abc.abstractmethod
    def direct_get_semantic_role(
        self,
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """Return the semantic role for one column."""

    @abc.abstractmethod
    def direct_set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist the semantic role for one column."""

    @abc.abstractmethod
    def direct_get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

    @abc.abstractmethod
    def direct_set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist the comparison-normalization profile for one column."""

    @abc.abstractmethod
    def direct_get_comparison_column(
        self,
        table: str,
        column: str,
    ) -> str | None:
        """Return the derived comparison column, if any."""

    @abc.abstractmethod
    def direct_get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the normalized row-identity declaration for a display column."""

    @abc.abstractmethod
    def direct_iter_normalized_identity_specs(
        self,
    ) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized row identity declared by the database."""

    @abc.abstractmethod
    def direct_set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist the derived comparison column for one column."""

    @abc.abstractmethod
    def direct_get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one column."""

    @abc.abstractmethod
    def direct_set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist the empty-value policy for one column."""

    @abc.abstractmethod
    def direct_get_merge_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnMergePolicy:
        """Return the merge policy for one column."""

    @abc.abstractmethod
    def direct_set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist the merge policy for one column."""

    @abc.abstractmethod
    def direct_get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one column."""

    @abc.abstractmethod
    def direct_set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist the validation profile for one column."""

    @abc.abstractmethod
    def direct_set_case_sensitivity(
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

    def direct_is_column_case_sensitive(self, table: str, column: str) -> bool:
        """Compatibility alias for :meth:`direct_get_case_sensitivity`."""

        return self.direct_get_case_sensitivity(table, column)

    def direct_set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Compatibility alias for :meth:`direct_set_case_sensitivity`."""

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    @abc.abstractmethod
    def direct_get_declared_types_for_table(self, table: str) -> dict[str, str]:
        """
        Get the declared column/type pairs for the given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def _invalidate_schema_caches(self) -> None:
        """
        Invalidate and clear the internal database schema caches.

        These are the tables/columns caches.
        :return:
        """

    @abc.abstractmethod
    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        """
        Direct get the column headings for a given table.

        :param table:
        :param normalize:
        :return:
        """

    @abc.abstractmethod
    def direct_get_record_count(self, target_table: str) -> int:
        """
        Get the number of records for a given table.

        :param target_table:
        :return:
        """

    # Todo: Merge with the above method
    @abc.abstractmethod
    def direct_get_row_count(self, table: str) -> int:
        """
        Get the number of rows for a given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tables(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Get all tables.

        :param force_refresh:
        :return:
        """

    @abc.abstractmethod
    def direct_get_tables_and_columns(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Direct get all tables and columns.

        :param force_refresh:
        :return:
        """

    # Todo: direct_*
    @abc.abstractmethod
    def direct_get_table_sqlite(self, table: str, conn: Any = None) -> str:
        """
        Get the SQLite which defines a table.

        :param table:
        :param conn:
        :return:
        """
