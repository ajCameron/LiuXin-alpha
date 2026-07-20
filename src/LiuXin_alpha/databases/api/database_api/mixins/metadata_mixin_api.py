from __future__ import annotations

import abc
from typing import Iterable, Iterator, Mapping, Any

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
from LiuXin_alpha.databases.macro_types import (
    CanonicalIdentity,
    NormalizedIdentityMigrationReport,
)
from LiuXin_alpha.databases.schema_specs import LinkCapabilities


class DatabaseMetadataMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseMetadataMixin``.
    """

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """
        Return the uuid for the database.

        :return:
        """

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        """
        Set the uuid for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """
        Get the library id for the database.

        :return:
        """

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        """
        Set the library id for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """
        Get the current database version.

        :return:
        """

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        """
        Set the database version for the database.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Get all the tables in the database.

        :param force_refresh: Bypass and refresh the cache
        :return:
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Get the column headings for the table of the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        :param table:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """Return type/priority capabilities for an interlink or intralink."""

    @abc.abstractmethod
    def is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a type column."""

    @abc.abstractmethod
    def is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a priority column."""

    @abc.abstractmethod
    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Return whether text equality for this column is case-sensitive.

        :param table:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """Return the complete semantic/writer policy for one column."""

    @abc.abstractmethod
    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """Persist the complete semantic/writer policy for one column."""

    @abc.abstractmethod
    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """Return the semantic role for one column."""

    @abc.abstractmethod
    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist the semantic role for one column."""

    @abc.abstractmethod
    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

    @abc.abstractmethod
    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist the comparison-normalization profile for one column."""

    @abc.abstractmethod
    def get_comparison_column(self, table: str, column: str) -> str | None:
        """Return the derived comparison column, if any."""

    @abc.abstractmethod
    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the normalized row-identity declaration for a display column."""

    @abc.abstractmethod
    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized row identity declared by the database."""

    @abc.abstractmethod
    def derive_identity_value(
        self,
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """Derive the declared normalized identity for a display value."""

    @abc.abstractmethod
    def get_canonical_identity(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve a display value to its complete stored canonical identity."""

    @abc.abstractmethod
    def get_canonical_identity_by_key(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
        id_column: str | None = None,
    ) -> CanonicalIdentity | None:
        """Resolve an already-derived identity to its canonical stored row."""

    @abc.abstractmethod
    def get_canonical_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a display value and return its canonical stored spelling."""

    @abc.abstractmethod
    def get_canonical_value_by_identity(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """Resolve a derived identity and return its canonical stored spelling."""

    @abc.abstractmethod
    def audit_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """Report stale keys and collisions without changing the database."""

    @abc.abstractmethod
    def migrate_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """Install, backfill, and index normalized identities atomically."""

    @abc.abstractmethod
    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist the derived comparison column for one column."""

    @abc.abstractmethod
    def get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one column."""

    @abc.abstractmethod
    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist the empty-value policy for one column."""

    @abc.abstractmethod
    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """Return the merge policy for one column."""

    @abc.abstractmethod
    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist the merge policy for one column."""

    @abc.abstractmethod
    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one column."""

    @abc.abstractmethod
    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist the validation profile for one column."""

    @abc.abstractmethod
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

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Get the column headings for a view.

        :param view:
        :return:
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Get all the tables and columns for the database.

        :return:
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Get the raw record count for the table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """
        Return the max value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """
        Return the min value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def row_counts(self) -> str:
        """
        Get the raw record count for the table.

        :return:
        """

    # ---------------------------------------------------------------------------------------------
    # Database metadata (uuid/library_id/version)
    # ---------------------------------------------------------------------------------------------
    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """Database UUID (used for cache keys, change detection, etc.)."""

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """Library UUID (unique identifier for the library itself)."""

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """Schema version string stored in the database."""

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        ...
