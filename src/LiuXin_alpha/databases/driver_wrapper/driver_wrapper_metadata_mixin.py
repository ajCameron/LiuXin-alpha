
"""
Expose database identity, relation discovery and column policy helpers.

Most methods delegate to the active driver. Policies describe column comparisons,
validation and presentation; policy changes do not themselves rewrite existing rows.
The relation-type helper queries SQLite catalog metadata directly.
"""

from __future__ import absolute_import, division, print_function, unicode_literals, annotations


from typing import Optional, Iterable, Iterator, Mapping, TYPE_CHECKING

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

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI



class DriverWrapperMetadataMixin:
    """
    Expose database identity, relation discovery and column policy helpers.

    Most methods delegate to the active driver. Policies describe column comparisons,
    validation and presentation; policy changes do not themselves rewrite existing rows.
    The relation-type helper queries SQLite catalog metadata directly.

    Example:
        >>> wrapper.get_tables()  # doctest: +SKIP
    """

    driver: "DatabaseDriverAPI"

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE
    # ------------------------------------------------------------------------------------------------------------------
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Return cached table/view names, rebuilding when a known schema version changes.

        Delegates to the driver's direct_get_tables hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Suppress an ``_v`` name when its unsuffixed counterpart exists. The returned list is
        the cache itself; forcing refresh closes and replaces the primary handle.

        A refresh first clears derived wrapper schema caches when that hook exists, then
        refreshes backend table discovery.

        Example:
            >>> wrapper.get_tables()  # doctest: +SKIP


        :param force_refresh: Invalidate cached schema data and reopen the primary
            connection before introspection.
        :return: The mutable list of visible table and view names.
        """
        if force_refresh and hasattr(self, "_clear_derived_schema_caches"):
            self._clear_derived_schema_caches()
        return self.driver.direct_get_tables(force_refresh=force_refresh)

    def get_relation_type(self, name: str) -> Optional[str]:
        """
        Look up a schema object's type in the SQLite catalog by name.

        Stringifies name, binds it in a case-insensitive sqlite_master query and
        lowercases/strips the first non-None type cell. Does not restrict catalog types to
        table/view, and does not inspect TEMP objects. Query/iteration exceptions are
        swallowed as None; errors stringifying name occur before that guard.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> connection = sqlite3.connect(":memory:")
            >>> _ = connection.execute("CREATE TABLE sample (id INTEGER)")
            >>> host = SimpleNamespace(execute=connection.execute)
            >>> DriverWrapperMetadataMixin.get_relation_type(host, "SAMPLE")
            'table'
            >>> DriverWrapperMetadataMixin.get_relation_type(host, "missing") is None
            True
            >>> connection.close()


        :param name: Schema object name to match case-insensitively.
        :return: Catalog type such as table, view, index or trigger; None for no result or a
            query failure.
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
        Look up cached column names after canonicalizing the table identifier.

        Delegates to the driver's direct_get_column_headings hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Populate the combined cache only when absent; an existing cache is not independently
        version-checked here. Unknown tables raise InputIntegrityError.

        Example:
            >>> wrapper.get_column_headings("works")  # doctest: +SKIP


        :param table: Table or view name used for schema introspection.
        :return: The cached column-name list.
        """
        return self.driver.direct_get_column_headings(table)

    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Read one column's backend-native type declaration from the type map.

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
        return self.driver.direct_get_declared_column_datatype(table, column)

    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the column policy flag controlling case-sensitive text equality.

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
        return self.driver.direct_get_case_sensitivity(table, column)

    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Resolve stored or inferred policy for an existing physical column.

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

        return self.driver.direct_get_column_metadata(table, column)

    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Validate and upsert the complete policy, then commit the catalog write.

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

        self.driver.direct_set_column_metadata(metadata)

    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """
        Read the resolved column policy for semantic meaning.

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

        return self.driver.direct_get_semantic_role(table, column)

    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Persist a replacement policy for semantic meaning.

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

        self.driver.direct_set_semantic_role(table, column, semantic_role)

    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the resolved column policy for comparison normalization.

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

        return self.driver.direct_get_normalization_profile(table, column)

    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Persist a replacement policy for comparison normalization.

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

        self.driver.direct_set_normalization_profile(
            table,
            column,
            normalization_profile,
        )

    def get_comparison_column(self, table: str, column: str) -> str | None:
        """
        Read the physical column designated for normalized comparisons.

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

        return self.driver.direct_get_comparison_column(table, column)

    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Resolve a normalized identity declaration for one physical value column.

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

        return self.driver.direct_get_normalized_identity_spec(table, value_column)

    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield database identity declarations after checking physical references.

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

        yield from self.driver.direct_iter_normalized_identity_specs()

    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Persist the physical column used for normalized comparisons.

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
        """
        Read the resolved column policy for empty-value handling.

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

        return self.driver.direct_get_empty_value_policy(table, column)

    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Persist a replacement policy for empty-value handling.

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

        self.driver.direct_set_empty_value_policy(
            table,
            column,
            empty_value_policy,
        )

    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """
        Read the resolved column policy for merge behavior.

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

        return self.driver.direct_get_merge_policy(table, column)

    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Persist a replacement policy for merge behavior.

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

        self.driver.direct_set_merge_policy(table, column, merge_policy)

    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the resolved column policy for semantic validation.

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

        return self.driver.direct_get_validation_profile(table, column)

    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Persist a replacement policy for semantic validation.

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

        self.driver.direct_set_validation_profile(
            table,
            column,
            validation_profile,
        )

    def get_formatting_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable value-formatting hints from the resolved column policy.

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

        return self.driver.direct_get_formatting_options(table, column)

    def set_formatting_options(
        self,
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement value-formatting hints.

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

        self.driver.direct_set_formatting_options(
            table,
            column,
            formatting_options,
        )

    def get_display_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable surface-display hints from the resolved column policy.

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

        return self.driver.direct_get_display_options(table, column)

    def set_display_options(
        self,
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement surface-display hints.

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

        self.driver.direct_set_display_options(
            table,
            column,
            display_options,
        )

    def set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Commit a case-sensitivity flag through the legacy-compatible catalog path.

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
        self.driver.direct_set_case_sensitivity(table, column, case_sensitive)

    def is_column_case_sensitive(self, table: str, column: str) -> bool:
        """
        Forward the legacy getter name to get_case_sensitivity().

        Arguments and the getter result pass through unchanged; exceptions propagate.

        Example:
            >>> from types import SimpleNamespace
            >>> host = SimpleNamespace(get_case_sensitivity=lambda table, column: False)
            >>> DriverWrapperMetadataMixin.is_column_case_sensitive(host, "works", "work_title")
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
            >>> wrapper.set_column_case_sensitive("works", "work_title", False)  # doctest: +SKIP


        :param table: Table name in the current schema.
        :param column: Column name in the selected table.
        :param case_sensitive: Desired text-equality case-sensitivity flag.
        :return: None.
        """

        self.set_case_sensitivity(table, column, case_sensitive)

    def get_tables_and_columns(self) -> dict[str, set[str]]:
        """
        Build or reuse the table/view-to-column-list cache with schema-version checks.

        Delegates to the driver's direct_get_tables_and_columns hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Use a fresh connection for PRAGMA table_info and close it on success. Return the
        cache object directly; force-refresh also replaces the primary connection.

        Example:
            >>> wrapper.get_tables_and_columns()  # doctest: +SKIP


        :return: The cached mapping from visible table/view names to ordered column lists.
        """
        return self.driver.direct_get_tables_and_columns()

    def get_highest_id(self, target_table: str) -> int:
        """
        Query the maximum ID value in a table, closing the connection after its result.

        Delegates to the driver's direct_get_highest_id hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Example:
            >>> wrapper.get_highest_id("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The scalar maximum ID, or ``None`` for an empty result/table.
        """
        return self.driver.direct_get_highest_id(target_table)

    @property
    def user_version(self) -> str:
        """
        Return the driver's application-controlled user_version value.

        This property simply forwards driver.user_version; it does not query or modify
        SQLite schema_version. SQLite backends currently return an integer despite the
        historical str annotation.

        Example:
            >>> wrapper.user_version  # doctest: +SKIP


        :return: Unmodified driver user_version value.
        """
        return self.driver.user_version

    # Todo: Need to standardize target_table, table and table_name to something
    def get_record_count(self, target_table):
        """
        Validate a table name and count its rows using a fresh connection.

        Delegates to the driver's direct_get_record_count hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Close the connection after reading the count; an invalid table raises
        InputIntegrityError.

        Example:
            >>> wrapper.get_record_count("works")  # doctest: +SKIP


        :param target_table: Existing table name used in the aggregate query.
        :return: The raw COUNT result, normally an integer.
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
        Read a validated metadata field, creating the placeholder row if needed.

        Delegates to the driver's direct_read_metadata hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        SQL null and values whose lowercase string is ``none`` return None. Other values are
        deep-copied. Unknown fields raise ValueError.

        Example:
            >>> wrapper.read_metadata("scratch")  # doctest: +SKIP


        :param field: Metadata column name, with or without the ``database_metadata_``
            prefix.
        :return: The copied field value, or ``None`` for null/none sentinels.
        """
        return self.driver.direct_read_metadata(md_field_name=field)

    def write_metadata(self, field, value):
        """
        Validate a metadata field, initialize the sole row and update its value.

        Delegates to the driver's direct_write_metadata hook. The following backend details
        describe the shared SQL/SQLite implementation; backend errors propagate.

        Unknown fields raise ValueError; multiple metadata rows raise
        DatabaseIntegrityError. Persistence is delegated to the row update helper.

        Example:
            >>> wrapper.write_metadata("scratch", "reviewed")  # doctest: +SKIP


        :param field: Metadata column name, with or without the ``database_metadata_``
            prefix.
        :param value: Value assigned to the validated metadata column.
        :return: ``None``.
        """
        return self.driver.direct_write_metadata(md_field_name=field, md_field_value=value)

    def get_uuid(self):
        """
        Read the database identity from its sole metadata row.

        Delegates to the driver's direct_get_db_unique_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Multiple rows raise DatabaseIntegrityError. Reuse the primary connection when
        available, otherwise close the temporary handle in finally.

        Example:
            >>> wrapper.get_uuid()  # doctest: +SKIP


        :return: The stored identity value, or ``None`` when there are no rows or the value
            is null.
        """
        return self.driver.direct_get_db_unique_id()

    def set_uuid(self, new_force_value: Optional[str] = None) -> None:
        """
        Write a supplied identity or new UUID4, commit, and verify it by rereading.

        Delegates to the driver's direct_set_db_unique_id hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        A non-null current identity updates row ID 1 without prompting. A null/missing
        identity takes the insert branch, so a pre-existing null-valued row can produce
        duplicate metadata rows. Verification failures raise DatabaseIntegrityError after
        the write commits.

        Example:
            >>> wrapper.set_uuid()  # doctest: +SKIP


        :param new_force_value: Identity value to write; ``None`` generates a UUID4 string.
        :return: ``True`` when rereading matches the requested value.
        """
        status = self.driver.direct_set_db_unique_id(force_value=new_force_value)
        return status
