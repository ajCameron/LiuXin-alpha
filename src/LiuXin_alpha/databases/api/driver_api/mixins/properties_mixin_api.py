"""
Specify schema discovery, link capabilities and column policies.

Policy setters persist metadata and invalidate relevant caches; they do not rewrite
historical rows by themselves. Derived comparison columns and normalized identity
declarations serve different roles. Most members are abstract; the two
case-sensitivity aliases concretely forward to canonical hooks.
"""

from __future__ import annotations

import abc
from collections.abc import Mapping
from typing import Optional, Iterable, Iterator, Any

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
from LiuXin_alpha.databases.schema_specs import LinkCapabilities


class DriverDatabasePropertiesMixinAPI(abc.ABC):
    """
    Specify schema discovery, link capabilities and column policies.

    Policy setters persist metadata and invalidate relevant caches; they do not rewrite
    historical rows by themselves. Derived comparison columns and normalized identity
    declarations serve different roles. Most members are abstract; the two
    case-sensitivity aliases concretely forward to canonical hooks. Abstract members
    must be implemented by a backend; their empty bodies return None when called
    directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverDatabasePropertiesMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Inspect physical type and priority columns for an interlink or intralink.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Validate both canonical endpoint tables first. Different endpoints use sorted column
        bases to derive the link-table name; identical endpoints use the repeated base with
        an intralink suffix. Result endpoint order follows the caller. Missing endpoints
        raise InputIntegrityError, while a missing link table returns None.

        Example:
            >>> driver.direct_get_link_capabilities("agents", "works")  # doctest: +SKIP


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: LinkCapabilities for the physical link table, or None if it is absent.
        """

    @abc.abstractmethod
    def direct_is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Report whether the physical link has its conventional type column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        An absent link is false. Invalid endpoint tables propagate InputIntegrityError from
        capability discovery.

        Example:
            >>> driver.direct_is_link_typed("agents", "works")  # doctest: +SKIP


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: True only when a link table and its type column both exist.
        """

    @abc.abstractmethod
    def direct_is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Report whether the physical link has its conventional priority column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        An absent link is false. Invalid endpoint tables propagate InputIntegrityError from
        capability discovery.

        Example:
            >>> driver.direct_is_link_priority("agents", "works")  # doctest: +SKIP


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: True only when a link table and its priority column both exist.
        """

    @abc.abstractmethod
    def direct_get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Read one column's backend-native type declaration from the type map.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Preserve the backend spelling and size information; an untyped SQLite column returns
        empty text. Missing columns raise InputIntegrityError. A physical column missing
        from the type map raises DatabaseIntegrityError.

        Example:
            >>> driver.direct_get_declared_column_datatype("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The original declared type string, including an empty string for no type.
        """

    @abc.abstractmethod
    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the column policy flag controlling case-sensitive text equality.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Use the complete metadata lookup, including inferred defaults when the catalog or
        row is absent. Invalid targets and corrupt stored policy propagate their lookup
        errors.

        Example:
            >>> driver.direct_get_case_sensitivity("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The case_sensitive boolean from the resolved column policy.
        """

    @abc.abstractmethod
    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Resolve stored or inferred policy for an existing physical column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Infer defaults from names and the declared type; unavailable type catalog entries
        fall back to inference without a declaration. If the metadata catalog or row is
        absent, return those defaults. A legacy catalog supplies only case sensitivity and
        retains other inferred fields. Expanded catalogs decode stored policy and optional
        presentation JSON. Invalid stored values or targets raise integrity errors. Close
        the read connection even if querying fails.

        Example:
            >>> driver.direct_get_column_metadata("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: A ColumnMetadata record with immutable formatting and display options.
        """

    @abc.abstractmethod
    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Validate and upsert the complete policy, then commit the catalog write.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Canonicalize the target and enum values, verify physical comparison columns and
        enforce any normalized identity declaration. Require the expanded catalog including
        both presentation-option columns; a missing or outdated catalog raises
        DatabaseIntegrityError. Invalid input raises InputIntegrityError. Close the
        connection after success or failure. This updates policy only and does not rebuild
        derived values.

        Example:
            >>> driver.direct_set_column_metadata(column_metadata)  # doctest: +SKIP


        :param metadata: Complete ColumnMetadata record for an existing column.
        :return: None after the write commits; database errors propagate.
        """

    @abc.abstractmethod
    def direct_get_semantic_role(
        self,
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """
        Read the resolved column policy for semantic meaning.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> driver.direct_get_semantic_role("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnSemanticRole label describing the column's application meaning.
        """

    @abc.abstractmethod
    def direct_set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Persist a replacement policy for semantic meaning.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> driver.direct_set_semantic_role("works", "work_title", semantic_role)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param semantic_role: ColumnSemanticRole label describing the column's application
            meaning.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the resolved column policy for comparison normalization.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> driver.direct_get_normalization_profile("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnNormalizationProfile identifying the comparison transformation.
        """

    @abc.abstractmethod
    def direct_set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Persist a replacement policy for comparison normalization.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required. Changes that
        conflict with a normalized identity are rejected.

        Example:
            >>> driver.direct_set_normalization_profile("works", "work_title", normalization_profile)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param normalization_profile: ColumnNormalizationProfile identifying the comparison
            transformation.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_comparison_column(
        self,
        table: str,
        column: str,
    ) -> str | None:
        """
        Read the physical column designated for normalized comparisons.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Resolve the complete policy using the same target validation and legacy fallbacks as
        direct_get_column_metadata. This does not compute or refresh comparison values.

        Example:
            >>> driver.direct_get_comparison_column("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The comparison column name, or None when the policy names none.
        """

    @abc.abstractmethod
    def direct_get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Resolve a normalized identity declaration for one physical value column.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Validate the target first. Only an absent identity catalog enables built-in
        defaults, and those require all referenced columns. If the catalog exists but its
        row is missing, return None without defaulting. Decode a stored row and check its
        value, key and scope columns. Declaration decoding errors propagate; missing
        physical references raise DatabaseIntegrityError. The read connection always closes.

        Example:
            >>> driver.direct_get_normalized_identity_spec("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param value_column: Existing physical value column whose identity is requested.
        :return: A validated NormalizedIdentitySpec, or None for an undeclared identity.
        """

    @abc.abstractmethod
    def direct_iter_normalized_identity_specs(
        self,
    ) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield database identity declarations after checking physical references.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Without a catalog, walk built-in defaults and omit absent tables or unsupported
        columns. With a catalog, fetch rows ordered by table and value column, close the
        connection, then decode and validate each row as iteration advances. An invalid
        later declaration may raise after earlier declarations have been yielded.

        Example:
            >>> driver.direct_iter_normalized_identity_specs()  # doctest: +SKIP


        :return: An iterator of NormalizedIdentitySpec records supported by the database.
        """

    @abc.abstractmethod
    def direct_set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Persist the physical column used for normalized comparisons.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Replace the field in the current policy and commit the complete record. A non-null
        name must refer to a column in the same table; an immutable normalized identity may
        prevent changing or clearing it. The expanded catalog is required. Stored data is
        not recomputed.

        Example:
            >>> driver.direct_set_comparison_column("works", "work_title", "work_title_norm")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param comparison_column: Same-table physical column name, or None to clear the
            reference.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """
        Read the resolved column policy for empty-value handling.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> driver.direct_get_empty_value_policy("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnEmptyValuePolicy describing which values count as missing.
        """

    @abc.abstractmethod
    def direct_set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Persist a replacement policy for empty-value handling.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> driver.direct_set_empty_value_policy("works", "work_title", empty_policy)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param empty_value_policy: ColumnEmptyValuePolicy describing which values count as
            missing.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_merge_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnMergePolicy:
        """
        Read the resolved column policy for merge behavior.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> driver.direct_get_merge_policy("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnMergePolicy describing how writers combine incoming values.
        """

    @abc.abstractmethod
    def direct_set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Persist a replacement policy for merge behavior.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> driver.direct_set_merge_policy("works", "work_title", merge_policy)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param merge_policy: ColumnMergePolicy describing how writers combine incoming
            values.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the resolved column policy for semantic validation.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            >>> driver.direct_get_validation_profile("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: ColumnValidationProfile naming the value validation policy.
        """

    @abc.abstractmethod
    def direct_set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Persist a replacement policy for semantic validation.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Read the current record, replace this field, then validate and commit the complete
        policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            >>> driver.direct_set_validation_profile("works", "work_title", validation_profile)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param validation_profile: ColumnValidationProfile naming the value validation
            policy.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_formatting_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable value-formatting hints from the resolved column policy.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata. Nested
        mappings are read-only and nested sequences are tuples; this getter does not render
        a value or surface.

        Example:
            >>> driver.direct_get_formatting_options("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The frozen option mapping, possibly empty.
        """

    @abc.abstractmethod
    def direct_set_formatting_options(
        self,
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement value-formatting hints.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Freeze a deep copy before replacing the field in the current policy. Reject non-string keys, unsupported objects and non-finite numbers with InputIntegrityError.
        Persist through the complete metadata writer, which requires the expanded catalog
        and retains other fields from its prior read. The hints are stored without rendering
        anything.

        Example:
            >>> driver.direct_set_formatting_options("works", "work_title", {"precision": 2})  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param formatting_options: JSON-like option mapping; None is also accepted at
            runtime as an empty mapping.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_get_display_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable surface-display hints from the resolved column policy.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata. Nested
        mappings are read-only and nested sequences are tuples; this getter does not render
        a value or surface.

        Example:
            >>> driver.direct_get_display_options("works", "work_title")  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The frozen option mapping, possibly empty.
        """

    @abc.abstractmethod
    def direct_set_display_options(
        self,
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement surface-display hints.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Freeze a deep copy before replacing the field in the current policy. Reject non-string keys, unsupported objects and non-finite numbers with InputIntegrityError.
        Persist through the complete metadata writer, which requires the expanded catalog
        and retains other fields from its prior read. The hints are stored without rendering
        anything.

        Example:
            >>> driver.direct_set_display_options("works", "work_title", {"label": "Title"})  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param display_options: JSON-like option mapping; None is also accepted at runtime
            as an empty mapping.
        :return: None after the complete policy write commits.
        """

    @abc.abstractmethod
    def direct_set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Commit a case-sensitivity flag through the legacy-compatible catalog path.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Validate the physical column, require an actual bool and enforce any normalized
        identity restriction. A missing catalog raises DatabaseIntegrityError. Upsert just
        the case flag, preserving other fields on an existing row; a new row receives the
        catalog defaults. Expanded policy columns are not required. Close the connection
        after the committed write or any error.

        Example:
            >>> driver.direct_set_case_sensitivity("works", "work_title", False)  # doctest: +SKIP


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param case_sensitive: Exact bool: True for case-sensitive equality, False
            otherwise.
        :return: None after the flag write commits.
        """

    def direct_is_column_case_sensitive(self, table: str, column: str) -> bool:
        """
        Forward to the canonical case-sensitivity getter.

        This concrete compatibility method forwards arguments unchanged and performs no
        validation. Backend errors propagate. Return the canonical getter result unchanged.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(direct_get_case_sensitivity=lambda table, column: False)
            >>> DriverDatabasePropertiesMixinAPI.direct_is_column_case_sensitive(host, "works", "work_title")
            False


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :return: The case-sensitivity boolean from the resolved policy.
        """

        return self.direct_get_case_sensitivity(table, column)

    def direct_set_column_case_sensitive(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Forward to the canonical case-sensitivity setter.

        This concrete compatibility method forwards arguments unchanged and performs no
        validation. Backend errors propagate. Discard the canonical setter result and return
        None.

        Example:
            >>> from types import SimpleNamespace
            >>> calls = []
            >>> host = SimpleNamespace(direct_set_case_sensitivity=lambda *args: calls.append(args))
            >>> DriverDatabasePropertiesMixinAPI.direct_set_column_case_sensitive(host, "works", "work_title", False)
            >>> calls
            [('works', 'work_title', False)]


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against headings.
        :param case_sensitive: Exact bool controlling case-sensitive equality.
        :return: None after the delegated write commits.
        """

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    @abc.abstractmethod
    def direct_get_declared_types_for_table(self, table: str) -> dict[str, str]:
        """
        Cache SQLite PRAGMA declarations by the supplied table key.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Initialize the per-instance cache lazily, then return a cached dictionary directly
        on later calls. A miss reads table_info and stores column names mapped to their
        original type strings, replacing null declarations with empty text. Unknown tables
        can yield an empty mapping. The connection closes after successful iteration; this
        method has no finally cleanup on failure. Table text is interpolated into PRAGMA
        without quoting or validation.

        Example:
            >>> driver.direct_get_declared_types_for_table("works")  # doctest: +SKIP


        :param table: Trusted SQL table spelling suitable for direct PRAGMA interpolation;
            also the exact cache key.
        :return: The cached mutable column-to-declared-type dictionary, not a copy.
        """

    @abc.abstractmethod
    def _invalidate_schema_caches(self) -> None:
        """
        Clear table/column caches, declared-type entries and the cached schema version.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver._invalidate_schema_caches()  # doctest: +SKIP


        :return: ``None``.
        """

    @abc.abstractmethod
    def direct_get_column_headings(self, table: str, normalize: bool = False) -> list[str]:
        """
        Look up cached column names after canonicalizing the table identifier.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Populate the combined cache only when absent; an existing cache is not independently
        version-checked here. Unknown tables raise InputIntegrityError.

        Example:
            >>> driver.direct_get_column_headings("works")  # doctest: +SKIP


        :param table: Table or view name used for schema introspection.
        :param normalize: Accepted for callers; currently unused.
        :return: The cached column-name list.
        """

    @abc.abstractmethod
    def direct_get_record_count(self, target_table: str) -> int:
        """
        Validate a table name and count its rows using a fresh connection.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Close the connection after reading the count; an invalid table raises
        InputIntegrityError.

        Example:
            >>> driver.direct_get_record_count("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The raw COUNT result, normally an integer.
        """

    # Todo: Merge with the above method
    @abc.abstractmethod
    def direct_get_row_count(self, table: str) -> int:
        """
        Count rows for a trusted SQL table identifier and convert the count to int.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        This variant does not validate the identifier; it closes the connection after
        obtaining a result.

        Example:
            >>> driver.direct_get_row_count("works")  # doctest: +SKIP


        :param table: Trusted table identifier interpolated directly into SQL.
        :return: The integer row count.
        """

    @abc.abstractmethod
    def direct_get_tables(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Return cached table/view names, rebuilding when a known schema version changes.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Suppress an ``_v`` name when its unsuffixed counterpart exists. The returned list is
        the cache itself; forcing refresh closes and replaces the primary handle.

        Example:
            >>> driver.direct_get_tables()  # doctest: +SKIP


        :param force_refresh: Invalidate cached schema data and reopen the primary
            connection before introspection.
        :return: The mutable list of visible table and view names.
        """

    @abc.abstractmethod
    def direct_get_tables_and_columns(self, force_refresh: bool = False) -> dict[str, list[str]]:
        """
        Build or reuse the table/view-to-column-list cache with schema-version checks.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Use a fresh connection for PRAGMA table_info and close it on success. Return the
        cache object directly; force-refresh also replaces the primary connection.

        Example:
            >>> driver.direct_get_tables_and_columns()  # doctest: +SKIP


        :param force_refresh: Invalidate cached schema data and reopen the primary
            connection before introspection.
        :return: The cached mapping from visible table/view names to ordered column lists.
        """

    # Todo: direct_*
    @abc.abstractmethod
    def direct_get_table_sqlite(self, table: str, conn: Any = None) -> str:
        """
        Read a table's stored CREATE statement from sqlite_master.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Bind the name as a value. Missing tables, including view-only matches, raise
        InputIntegrityError; neither supplied nor acquired connections are closed here.

        Example:
            >>> driver.direct_get_table_sqlite("works")  # doctest: +SKIP


        :param table: Exact table name used in the bound sqlite_master lookup.
        :param conn: Optional existing connection; ``None`` acquires a new one.
        :return: The stored SQL text, which may be null for internal tables.
        """
