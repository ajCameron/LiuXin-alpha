"""
Specify portable schema structure, relationship capabilities and column policies.

Abstract hooks provide no implementation and return None if their bodies are called
directly. Method contracts describe the reviewed DriverWrapper implementation and
its shared SQL backend. Two concrete case-sensitivity aliases forward to canonical
hooks.
"""

# Suggested additions to DatabaseDriverWrapperAPI (or a new SchemaIntrospectionAPI)

import abc
from abc import abstractmethod
from collections.abc import Mapping
from typing import Iterator, Optional

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnOptions,
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
    """
    Specify portable schema structure, relationship capabilities and column policies.

    Abstract hooks provide no implementation and return None if their bodies are called
    directly. Method contracts describe the reviewed DriverWrapper implementation and
    its shared SQL backend. Two concrete case-sensitivity aliases forward to canonical
    hooks.

    Example:
        >>> import inspect
        >>> inspect.isabstract(SchemaIntrospectionAPI)
        True
    """


    @abstractmethod
    def get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Read and cache the backend capabilities for an ordered pair of tables.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Cache keys stringify both names and retain endpoint order. Missing links are cached
        as None. force_refresh clears all derived wrapper caches and is also forwarded to
        the driver.

        Example:
            >>> wrapper.get_link_capabilities("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Backend LinkCapabilities, or None when no supported link is found.
        """

    @abstractmethod
    def is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Return whether the discovered link has a type column.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Uses get_link_capabilities() and its cache/refresh behavior. Missing capabilities
        give False.

        Example:
            >>> wrapper.is_link_typed("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: True when the capabilities expose that column; otherwise False.
        """

    @abstractmethod
    def is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Return whether the discovered link has a priority column.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Uses get_link_capabilities() and its cache/refresh behavior. Missing capabilities
        give False.

        Example:
            >>> wrapper.is_link_priority("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: True when the capabilities expose that column; otherwise False.
        """

    @abstractmethod
    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Read one column's backend-native type declaration from the type map.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_declared_column_datatype hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Preserve the backend spelling and size information; an untyped SQLite column returns
        empty text. Missing columns raise InputIntegrityError. A physical column missing
        from the type map raises DatabaseIntegrityError.

        Example:
            >>> wrapper.get_declared_column_datatype("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The original declared type string, including an empty string for no type.
        """

    @abstractmethod
    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the column policy flag controlling case-sensitive text equality.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_case_sensitivity hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Use the complete metadata lookup, including inferred defaults when the catalog or
        row is absent. Invalid targets and corrupt stored policy propagate their lookup
        errors.

        Example:
            >>> wrapper.get_case_sensitivity("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The case_sensitive boolean from the resolved column policy.
        """

    @abstractmethod
    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Resolve stored or inferred policy for an existing physical column.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_column_metadata hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Infer defaults from names and the declared type; unavailable type catalog entries
        fall back to inference without a declaration. If the metadata catalog or row is
        absent, return those defaults. A legacy catalog supplies only case sensitivity and
        retains other inferred fields. Expanded catalogs decode stored policy and optional
        presentation JSON. Invalid stored values or targets raise integrity errors. Close
        the read connection even if querying fails.

        Example:
            >>> wrapper.get_column_metadata("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: A ColumnMetadata record with immutable formatting and display options.
        """

    @abstractmethod
    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Validate and upsert the complete policy, then commit the catalog write.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_column_metadata hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Canonicalize the target and enum values, verify physical comparison columns and
        enforce any normalized identity declaration. Require the expanded catalog including
        both presentation-option columns; a missing or outdated catalog raises
        DatabaseIntegrityError. Invalid input raises InputIntegrityError. Close the
        connection after success or failure. This updates policy only and does not rebuild
        derived values.

        Example:
            >>> wrapper.set_column_metadata(column_metadata)  # doctest: +SKIP


        :param metadata: Complete ColumnMetadata record for an existing column.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """
        Read the resolved column policy for semantic meaning.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_semantic_role hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> wrapper.get_semantic_role("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnSemanticRole label describing the column's application meaning.
        """

    @abstractmethod
    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Persist a replacement policy for semantic meaning.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_semantic_role hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> wrapper.set_semantic_role("works", "work_title", ColumnSemanticRole.LABEL)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param semantic_role: ColumnSemanticRole label describing the column's application
            meaning.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the resolved column policy for comparison normalization.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_normalization_profile hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> wrapper.get_normalization_profile("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnNormalizationProfile identifying the comparison transformation.
        """

    @abstractmethod
    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Persist a replacement policy for comparison normalization.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_normalization_profile hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required. Changes that
        conflict with a normalized identity are rejected.

        Example:
            >>> wrapper.set_normalization_profile("works", "work_title", ColumnNormalizationProfile.UNICODE_NFC)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param normalization_profile: ColumnNormalizationProfile identifying the comparison
            transformation.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_comparison_column(self, table: str, column: str) -> str | None:
        """
        Read the physical column designated for normalized comparisons.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_comparison_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Resolve the complete policy using the same target validation and legacy fallbacks as
        direct_get_column_metadata. This does not compute or refresh comparison values.

        Example:
            >>> wrapper.get_comparison_column("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The comparison column name, or None when the policy names none.
        """

    @abstractmethod
    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Resolve a normalized identity declaration for one physical value column.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_normalized_identity_spec hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Validate the target first. Only an absent identity catalog enables built-in
        defaults, and those require all referenced columns. If the catalog exists but its
        row is missing, return None without defaulting. Decode a stored row and check its
        value, key and scope columns. Declaration decoding errors propagate; missing
        physical references raise DatabaseIntegrityError. The read connection always closes.

        Example:
            >>> wrapper.get_normalized_identity_spec("works", "tag")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param value_column: Existing physical value column whose identity is requested.
        :return: A validated NormalizedIdentitySpec, or None for an undeclared identity.
        """

    @abstractmethod
    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield database identity declarations after checking physical references.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_iter_normalized_identity_specs hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Without a catalog, walk built-in defaults and omit absent tables or unsupported
        columns. With a catalog, fetch rows ordered by table and value column, close the
        connection, then decode and validate each row as iteration advances. An invalid
        later declaration may raise after earlier declarations have been yielded.

        Example:
            >>> wrapper.iter_normalized_identity_specs()  # doctest: +SKIP


        :return: An iterator of NormalizedIdentitySpec records supported by the database.
        """

    @abstractmethod
    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Persist the physical column used for normalized comparisons.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_comparison_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Replace the field in the current policy and commit the complete record. A non- null
        name must refer to a column in the same table; an immutable normalized identity may
        prevent changing or clearing it. The expanded catalog is required. Stored data is
        not recomputed.

        Example:
            >>> wrapper.set_comparison_column("works", "work_title", "work_sort_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param comparison_column: Same-table physical column name, or None to clear the
            reference.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """
        Read the resolved column policy for empty-value handling.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_empty_value_policy hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> wrapper.get_empty_value_policy("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnEmptyValuePolicy describing which values count as missing.
        """

    @abstractmethod
    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Persist a replacement policy for empty-value handling.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_empty_value_policy hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> wrapper.set_empty_value_policy("works", "work_title", ColumnEmptyValuePolicy.PRESERVE)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param empty_value_policy: ColumnEmptyValuePolicy describing which values count as
            missing.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """
        Read the resolved column policy for merge behavior.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_merge_policy hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> wrapper.get_merge_policy("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnMergePolicy describing how writers combine incoming values.
        """

    @abstractmethod
    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Persist a replacement policy for merge behavior.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_merge_policy hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> wrapper.set_merge_policy("works", "work_title", ColumnMergePolicy.PRESERVE_EXISTING)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param merge_policy: ColumnMergePolicy describing how writers combine incoming
            values.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the resolved column policy for semantic validation.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_validation_profile hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> wrapper.get_validation_profile("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnValidationProfile naming the value validation policy.
        """

    @abstractmethod
    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Persist a replacement policy for semantic validation.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_validation_profile hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> wrapper.set_validation_profile("works", "work_title", ColumnValidationProfile.VERBATIM_TEXT)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param validation_profile: ColumnValidationProfile naming the value validation
            policy.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_formatting_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable value-formatting hints from the resolved column policy.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_formatting_options hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata. Nested
        mappings are read-only and nested sequences are tuples; this getter does not render
        a value or surface.

        Example:
            >>> wrapper.get_formatting_options("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The frozen option mapping, possibly empty.
        """

    @abstractmethod
    def set_formatting_options(
        self,
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement value-formatting hints.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_formatting_options hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Freeze a deep copy before replacing the field in the current policy. Reject non-
        string keys, unsupported objects and non-finite numbers with InputIntegrityError.
        Persist through the complete metadata writer, which requires the expanded catalog
        and retains other fields from its prior read. The hints are stored without rendering
        anything.

        Example:
            >>> wrapper.set_formatting_options("works", "work_title", {"precision": 2})  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param formatting_options: JSON-like option mapping; None is also accepted at
            runtime as an empty mapping.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def get_display_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable surface-display hints from the resolved column policy.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_get_display_options hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata. Nested
        mappings are read-only and nested sequences are tuples; this getter does not render
        a value or surface.

        Example:
            >>> wrapper.get_display_options("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The frozen option mapping, possibly empty.
        """

    @abstractmethod
    def set_display_options(
        self,
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement surface-display hints.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_display_options hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Freeze a deep copy before replacing the field in the current policy. Reject non-
        string keys, unsupported objects and non-finite numbers with InputIntegrityError.
        Persist through the complete metadata writer, which requires the expanded catalog
        and retains other fields from its prior read. The hints are stored without rendering
        anything.

        Example:
            >>> wrapper.set_display_options("works", "work_title", {"label": "Title"})  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param display_options: JSON-like option mapping; None is also accepted at runtime
            as an empty mapping.
        :return: None; the driver setter result is discarded.
        """

    @abstractmethod
    def set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Commit a case-sensitivity flag through the legacy-compatible catalog path.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Delegates to the driver's direct_set_case_sensitivity hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Validate the physical column, require an actual bool and enforce any normalized
        identity restriction. A missing catalog raises DatabaseIntegrityError. Upsert just
        the case flag, preserving other fields on an existing row; a new row receives the
        catalog defaults. Expanded policy columns are not required. Close the connection
        after the committed write or any error.

        Example:
            >>> wrapper.set_case_sensitivity("works", "work_title", False)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param case_sensitive: Exact bool: True for case-sensitive equality, False
            otherwise.
        :return: None; the driver setter result is discarded.
        """

    def is_column_case_sensitive(self, table: str, column: str) -> bool:
        """
        Forward the legacy getter name to get_case_sensitivity().

        Arguments and the getter result pass through unchanged; exceptions propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> calls = []
            >>> host = SimpleNamespace(get_case_sensitivity=lambda table, column: False)
            >>> SchemaIntrospectionAPI.is_column_case_sensitive(host, "works", "work_title")
            False


        :param table: Table name in the current schema.
        :param column: Column name in the selected table.
        :return: Result of the canonical case-sensitivity getter.
        """

        return self.get_case_sensitivity(table, column)

    def set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Forward the legacy setter name to set_case_sensitivity().

        Arguments pass through unchanged; the setter result is discarded and errors
        propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> calls = []
            >>> host = SimpleNamespace(set_case_sensitivity=lambda *args: calls.append(args))
            >>> SchemaIntrospectionAPI.set_column_case_sensitive(host, "works", "work_title", False)
            >>> calls
            [('works', 'work_title', False)]


        :param table: Table name in the current schema.
        :param column: Column name in the selected table.
        :param case_sensitive: Desired text-equality case-sensitivity flag.
        :return: None.
        """

        self.set_case_sensitivity(table, column, case_sensitive)

    @abstractmethod
    def get_table_spec(self, table: str, force_refresh: bool = False) -> StorageTableSpec:
        """
        Build or reuse the schema specification for a table or view.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        A missing relation raises ValueError. Headings preserve discovered order; optional
        backend type and affinity failures leave those values unset. Every column is
        currently marked nullable, so the result does not certify NOT NULL constraints.
        Conventional ID, date and scratch columns are inferred locally for tables only.
        Populated or inferred table groups set classification flags; related main tables are
        sorted. force_refresh clears wrapper caches but does not explicitly refresh backend
        table discovery.

        Example:
            >>> wrapper.get_table_spec("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Cached StorageTableSpec describing the requested relation.
        """

    @abstractmethod
    def iter_table_specs(
        self,
        *,
        force_refresh: bool = False,
        include_views: bool = True,
    ) -> Iterator[StorageTableSpec]:
        """
        Yield specifications for discovered tables and optionally views.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Tables come first in backend discovery order. View names already seen as tables are
        skipped. Refresh clears derived wrapper caches when iteration starts, but discovery
        uses force_refresh=False.

        Example:
            >>> wrapper.iter_table_specs()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :param include_views: Whether to include separately discovered views after tables.
        :return: Iterator yielding StorageTableSpec objects.
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
        Build or reuse an oriented relationship specification between two tables.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Missing backend capabilities are cached as None. Endpoint ID columns come from
        conventional name lookup; optional type and priority columns come from capabilities.
        Remaining physical columns become extra_link_columns. Registry discovery prefers
        <link_table>__types over allowed_types__<link_table>. Unique indexes determine
        cardinality and typed identity; lookup errors for required columns propagate. For a
        self-link, use get_intralink_spec() to resolve primary_id and secondary_id endpoint
        columns.

        Example:
            >>> wrapper.get_link_spec("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: StorageLinkSpec for this endpoint order, or None when no link exists.
        """

    @abstractmethod
    def get_intralink_spec(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> Optional[StorageLinkSpec]:
        """
        Build or reuse a self-relationship specification for one main table.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Uses primary_id and secondary_id link-column suffixes for the two endpoints.
        Type/priority capabilities, extra columns, optional type registries and unique-index
        cardinality follow the same rules as interlinks. Missing capabilities are cached as
        None; missing required columns raise lookup errors.

        Example:
            >>> wrapper.get_intralink_spec("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: StorageLinkSpec with matching endpoint tables, or None when no link exists.
        """

    @abstractmethod
    def iter_link_specs(
        self,
        *,
        force_refresh: bool = False,
        include_intralinks: bool = True,
    ) -> Iterator[StorageLinkSpec]:
        """
        Yield discovered interlink specifications and optionally intralinks.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Visits sorted main-table pairs once, using that orientation, and emits each
        interlink table only once. Requested intralinks follow, one per main table when
        available. Refresh clears derived caches when iteration starts.

        Example:
            >>> wrapper.iter_link_specs()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :param include_intralinks: Whether to append self-links after cross-table links.
        :return: Iterator yielding StorageLinkSpec objects.
        """

    @abstractmethod
    def get_schema_spec(self, force_refresh: bool = False) -> StorageSchemaSpec:
        """
        Assemble and cache the discovered table, interlink and intralink schema.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Includes views among table specifications. Relationship discovery uses main-table
        groups and conventional names. force_refresh clears derived wrapper caches; nested
        discovery otherwise uses its ordinary cached backend paths.

        Example:
            >>> wrapper.get_schema_spec()  # doctest: +SKIP


        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Cached StorageSchemaSpec containing tables and relationship tuples.
        """

    @abstractmethod
    def get_row_dataclass(
        self,
        table: str,
        *,
        force_refresh: bool = False,
    ) -> type:
        """
        Build and cache a row dataclass from a relation specification.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Uses build_row_dataclass_for_table() with the discovered table or view columns.
        Repeated calls return the same cached class until derived caches are cleared.
        Missing-relation and dataclass-construction errors propagate.

        Example:
            >>> wrapper.get_row_dataclass("works")  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Generated dataclass type, not an instance.
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
        Build and cache a row dataclass for the link between two tables.

        Abstract hook; the following behavior describes concrete DriverWrapper
        implementations.

        Equal endpoint names use intralink discovery; other pairs use oriented interlink
        discovery. Missing links are cached as None. Successful classes describe the
        physical link table and remain cached until invalidation.

        Example:
            >>> wrapper.get_link_row_dataclass("agents", "works")  # doctest: +SKIP


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Whether to clear derived wrapper caches before rebuilding this
            result.
        :return: Generated link-row dataclass type, or None for an absent link.
        """
