# Suggested additions to DatabaseDriverWrapperAPI (or a new SchemaIntrospectionAPI)

import abc
from abc import abstractmethod
from typing import Iterator, Optional

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
from LiuXin_alpha.databases.schema_specs import (
    LinkCapabilities,
    StorageSchemaSpec,
    StorageTableSpec,
    StorageLinkSpec,
)


class SchemaIntrospectionAPI(abc.ABC):

    @abstractmethod
    def get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """Return type/priority capabilities for an interlink or intralink."""

    @abstractmethod
    def is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a type column."""

    @abstractmethod
    def is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """Return whether an interlink or intralink has a priority column."""

    @abstractmethod
    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Return the database-native declared datatype for one column.

        Raises when the table or column does not exist. A valid SQLite column
        declared without a datatype returns an empty string.
        """

    @abstractmethod
    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """Return whether text equality for this column is case-sensitive."""

    @abstractmethod
    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """Return the complete semantic/writer policy for one column."""

    @abstractmethod
    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """Persist the complete semantic/writer policy for one column."""

    @abstractmethod
    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """Return the semantic role for one column."""

    @abstractmethod
    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """Persist the semantic role for one column."""

    @abstractmethod
    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """Return the comparison-normalization profile for one column."""

    @abstractmethod
    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """Persist the comparison-normalization profile for one column."""

    @abstractmethod
    def get_comparison_column(self, table: str, column: str) -> str | None:
        """Return the derived comparison column, if any."""

    @abstractmethod
    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """Return the normalized row-identity declaration for a display column."""

    @abstractmethod
    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """Yield every normalized row identity declared by the database."""

    @abstractmethod
    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """Persist the derived comparison column for one column."""

    @abstractmethod
    def get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """Return the empty-value policy for one column."""

    @abstractmethod
    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """Persist the empty-value policy for one column."""

    @abstractmethod
    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """Return the merge policy for one column."""

    @abstractmethod
    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """Persist the merge policy for one column."""

    @abstractmethod
    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """Return the validation profile for one column."""

    @abstractmethod
    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """Persist the validation profile for one column."""

    @abstractmethod
    def set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """Persist text equality policy for one column."""

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

    @abstractmethod
    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        """
        Return a dataclass describing one table or view.
        Raises if the relation does not exist.
        """

    @abstractmethod
    def iter_table_specs(
        self,
        *,
        force_refresh: bool = False,
        include_views: bool = True,
    ) -> Iterator[StorageTableSpec]:
        """
        Yield specs for all known tables (and optionally views).
        """

    @abstractmethod
    def get_link_spec(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> Optional["StorageLinkSpec"]:
        """
        Return the interlink spec between two tables, or None if no link exists.
        """

    @abstractmethod
    def get_intralink_spec(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[StorageLinkSpec]:
        """
        Return the self-link spec for a table, or None if no intralink exists.
        """

    @abstractmethod
    def iter_link_specs(
        self,
        *,
        force_refresh: bool = False,
        include_intralinks: bool = True,
    ) -> Iterator[StorageLinkSpec]:
        """
        Yield all discovered link specs.
        """

    @abstractmethod
    def get_schema_spec(self, force_refresh: bool = False) -> StorageSchemaSpec:
        """
        Return a snapshot of the whole schema as dataclasses.
        """

    @abstractmethod
    def get_row_dataclass(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> type:
        """
        Build and return a dataclass type for rows in the given table/view.
        """

    @abstractmethod
    def get_link_row_dataclass(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[type]:
        """
        Build and return a dataclass type for rows in the relevant link table.
        """
