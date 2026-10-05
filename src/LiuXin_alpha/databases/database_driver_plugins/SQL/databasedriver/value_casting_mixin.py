"""
Inspect SQL schema policy and convert row values using declared types.

The mixin combines link-capability discovery, database-owned column metadata, normalized
identity declarations and SQLite-oriented casting. Missing policy catalogs can use
built-in defaults; writes require the appropriate catalog schema. Declared numeric
affinities can parse numeric text, while malformed text stays visible. Untyped
conversion preserves numeric objects without parsing numeric strings.
"""

# Todo: Might be an idea to add a as_value to row - so we can be sure we're e.g. getting an int

from __future__ import annotations

from dataclasses import replace
import re

from collections.abc import Mapping
from typing import Any, Dict, Iterator, Optional, Sequence, Union

from LiuXin_alpha.databases.column_metadata import (
    COLUMN_METADATA_TABLE,
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnMetadata,
    ColumnNormalizationProfile,
    ColumnOptions,
    ColumnSemanticRole,
    ColumnValidationProfile,
    column_options_from_json,
    column_options_to_json,
    default_column_metadata,
    freeze_column_options,
    infer_column_metadata,
)
from LiuXin_alpha.databases.normalized_identities import (
    NORMALIZED_IDENTITIES_TABLE,
    NormalizedIdentitySpec,
    default_normalized_identity_spec,
    iter_normalized_identity_defaults,
    normalized_identity_from_db_values,
)
from LiuXin_alpha.databases.schema_specs import LinkCapabilities
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import force_unicode


class ValueCastingMixin:
    """
    Supply schema policy access and type-aware row conversion to SQL drivers.

    Concrete drivers provide connections, table-name canonicalization, headings and
    schema-cache operations. Policy setters commit their own catalog writes; storing a
    policy does not rewrite column data. The declared-type cache belongs to each
    instance and is shared with the driver's schema invalidation lifecycle.

    Example:
        >>> ValueCastingMixin._normalize_declared_type(" varchar(80) ")
        'VARCHAR'
    """

    _DECLARED_TYPES_CACHE_ATTR = "_declared_types_cache"

    def direct_get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Inspect physical type and priority columns for an interlink or intralink.

        Validate both canonical endpoint tables first. Different endpoints use sorted
        column bases to derive the link-table name; identical endpoints use the repeated
        base with an intralink suffix. Result endpoint order follows the caller. Missing
        endpoints raise InputIntegrityError, while a missing link table returns None.

        Example:
            With an open FRBR driver,
            ``driver.direct_get_link_capabilities("agents", "works")`` describes
            the columns present in ``agent_work_links``.


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an
            intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: LinkCapabilities for the physical link table, or None if it is absent.
        """

        primary_table = self._canonicalise_table_name_for_cache(table1)
        secondary_table = self._canonicalise_table_name_for_cache(table2)
        tables_and_columns = self.direct_get_tables_and_columns(
            force_refresh=force_refresh,
        )
        for endpoint in (primary_table, secondary_table):
            if endpoint not in tables_and_columns:
                raise InputIntegrityError(
                    f"link endpoint table {endpoint!r} not found"
                )

        primary_base = self.direct_get_table_col_base(primary_table)
        secondary_base = self.direct_get_table_col_base(secondary_table)
        if primary_table == secondary_table:
            link_column_base = f"{primary_base}_{primary_base}_intralink"
            link_table = f"{link_column_base}s"
        else:
            ordered_bases = sorted((primary_base, secondary_base))
            link_column_base = f"{ordered_bases[0]}_{ordered_bases[1]}_link"
            link_table = f"{link_column_base}s"

        headings = tables_and_columns.get(link_table)
        if headings is None:
            return None

        type_column = f"{link_column_base}_type"
        priority_column = f"{link_column_base}_priority"
        return LinkCapabilities(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_table=link_table,
            type_column=type_column if type_column in headings else None,
            priority_column=(
                priority_column
                if priority_column in headings
                else None
            ),
        )

    def direct_is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Report whether the physical link has its conventional type column.

        An absent link is false. Invalid endpoint tables propagate InputIntegrityError
        from capability discovery.

        Example:
            ``driver.direct_is_link_typed("agents", "works")`` is true when
            ``agent_work_link_type`` exists.


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an
            intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: True only when a link table and its type column both exist.
        """

        capabilities = self.direct_get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        return bool(capabilities is not None and capabilities.typed)

    def direct_is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Report whether the physical link has its conventional priority column.

        An absent link is false. Invalid endpoint tables propagate InputIntegrityError
        from capability discovery.

        Example:
            ``driver.direct_is_link_priority("tags", "works")`` is true when
            ``tag_work_link_priority`` exists.


        :param table1: First existing endpoint table; also the returned primary table.
        :param table2: Second existing endpoint table; may equal the first for an
            intralink.
        :param force_refresh: Refresh the tables-and-columns snapshot before inspecting
            links.
        :return: True only when a link table and its priority column both exist.
        """

        capabilities = self.direct_get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )
        return bool(capabilities is not None and capabilities.priority)

    def direct_get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Read one column's backend-native type declaration from the type map.

        Preserve the backend spelling and size information; an untyped SQLite column
        returns empty text. Missing columns raise InputIntegrityError. A physical column
        missing from the type map raises DatabaseIntegrityError.

        Example:
            ``driver.direct_get_declared_column_datatype("works", "work_title")``
            returns the declaration supplied by that driver, such as ``"TEXT"``.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: The original declared type string, including an empty string for no
            type.
        """
        table_name = self._canonicalise_table_name_for_cache(table)
        column_name = str(column)
        headings = self.direct_get_column_headings(table_name)
        if column_name not in headings:
            raise InputIntegrityError(f"column {column_name!r} not found in table {table_name!r}")

        declared_types = self.direct_get_declared_types_for_table(table_name)
        try:
            return declared_types[column_name]
        except KeyError as exc:
            raise DatabaseIntegrityError(
                f"column {column_name!r} exists in table {table_name!r} "
                "but has no declared-datatype catalog entry"
            ) from exc

    def direct_get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the column policy flag controlling case-sensitive text equality.

        Use the complete metadata lookup, including inferred defaults when the catalog
        or row is absent. Invalid targets and corrupt stored policy propagate their
        lookup errors.

        Example:
            ``driver.direct_get_case_sensitivity("tags", "tag")`` is false
            for the standard tag-search policy.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: The case_sensitive boolean from the resolved column policy.
        """
        return self.direct_get_column_metadata(table, column).case_sensitive

    def direct_get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Resolve stored or inferred policy for an existing physical column.

        Infer defaults from names and the declared type; unavailable type catalog
        entries fall back to inference without a declaration. If the metadata catalog or
        row is absent, return those defaults. A legacy catalog supplies only case
        sensitivity and retains other inferred fields. Expanded catalogs decode stored
        policy and optional presentation JSON. Invalid stored values or targets raise
        integrity errors. Close the read connection even if querying fails.

        Example:
            ``driver.direct_get_column_metadata("tags", "tag").comparison_column``
            is ``"tag_phash"`` with the standard tag identity declaration.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: A ColumnMetadata record with immutable formatting and display options.
        """

        table_name, column_name = self._validated_column_metadata_target(table, column)
        try:
            declared_type = self.direct_get_declared_column_datatype(
                table_name,
                column_name,
            )
        except DatabaseIntegrityError:
            declared_type = None
        fallback = infer_column_metadata(
            table_name,
            column_name,
            declared_type,
        )
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            return fallback

        catalog_columns = set(self.direct_get_column_headings(COLUMN_METADATA_TABLE))
        expanded_columns = {
            "column_metadata_semantic_role",
            "column_metadata_normalization_profile",
            "column_metadata_comparison_column",
            "column_metadata_empty_value_policy",
            "column_metadata_merge_policy",
            "column_metadata_validation_profile",
        }
        option_columns = {
            "column_metadata_formatting_options_json",
            "column_metadata_display_options_json",
        }
        conn = self.get_connection()
        try:
            if expanded_columns <= catalog_columns:
                option_selection = (
                    ",\n                      column_metadata_formatting_options_json,"
                    "\n                      column_metadata_display_options_json"
                    if option_columns <= catalog_columns
                    else ""
                )
                row = conn.execute(
                    f"""
                    SELECT
                      column_metadata_case_sensitive,
                      column_metadata_semantic_role,
                      column_metadata_normalization_profile,
                      column_metadata_comparison_column,
                      column_metadata_empty_value_policy,
                      column_metadata_merge_policy,
                      column_metadata_validation_profile
                      {option_selection}
                    FROM column_metadata
                    WHERE column_metadata_table_name = ?
                      AND column_metadata_column_name = ?
                    LIMIT 1;
                    """,
                    (table_name, column_name),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT column_metadata_case_sensitive
                    FROM column_metadata
                    WHERE column_metadata_table_name = ?
                      AND column_metadata_column_name = ?
                    LIMIT 1;
                    """,
                    (table_name, column_name),
                ).fetchone()
        finally:
            conn.close()
        if row is None:
            return fallback
        if not expanded_columns <= catalog_columns:
            return ColumnMetadata(
                table=fallback.table,
                column=fallback.column,
                case_sensitive=self._coerce_column_case_sensitivity(
                    row[0],
                    table_name,
                    column_name,
                ),
                semantic_role=fallback.semantic_role,
                normalization_profile=fallback.normalization_profile,
                comparison_column=fallback.comparison_column,
                empty_value_policy=fallback.empty_value_policy,
                merge_policy=fallback.merge_policy,
                validation_profile=fallback.validation_profile,
                formatting_options=fallback.formatting_options,
                display_options=fallback.display_options,
            )
        return self._column_metadata_from_values(
            table_name,
            column_name,
            *row,
        )

    def direct_set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Validate and upsert the complete policy, then commit the catalog write.

        Canonicalize the target and enum values, verify physical comparison columns and
        enforce any normalized identity declaration. Require the expanded catalog
        including both presentation-option columns; a missing or outdated catalog raises
        DatabaseIntegrityError. Invalid input raises InputIntegrityError. Close the
        connection after success or failure. This updates policy only and does not
        rebuild derived values.

        Example:
            To persist a changed merge policy on an open driver::

                current = driver.direct_get_column_metadata("works", "work_title")
                driver.direct_set_column_metadata(
                    replace(current, merge_policy=ColumnMergePolicy.PRESERVE_EXISTING)
                )


        :param metadata: Complete ColumnMetadata record for an existing column.
        :return: None after the write commits; database errors propagate.
        """

        metadata = self._validated_column_metadata_input(metadata)
        self._validate_normalized_identity_metadata(metadata)
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )
        required_columns = {
            "column_metadata_semantic_role",
            "column_metadata_normalization_profile",
            "column_metadata_comparison_column",
            "column_metadata_empty_value_policy",
            "column_metadata_merge_policy",
            "column_metadata_validation_profile",
            "column_metadata_formatting_options_json",
            "column_metadata_display_options_json",
        }
        if not required_columns <= set(self.direct_get_column_headings(COLUMN_METADATA_TABLE)):
            raise DatabaseIntegrityError(
                "column_metadata schema is outdated; migrate it before storing expanded policy"
            )

        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT INTO column_metadata (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile,
                  column_metadata_formatting_options_json,
                  column_metadata_display_options_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO UPDATE SET
                  column_metadata_case_sensitive = excluded.column_metadata_case_sensitive,
                  column_metadata_semantic_role = excluded.column_metadata_semantic_role,
                  column_metadata_normalization_profile = excluded.column_metadata_normalization_profile,
                  column_metadata_comparison_column = excluded.column_metadata_comparison_column,
                  column_metadata_empty_value_policy = excluded.column_metadata_empty_value_policy,
                  column_metadata_merge_policy = excluded.column_metadata_merge_policy,
                  column_metadata_validation_profile = excluded.column_metadata_validation_profile,
                  column_metadata_formatting_options_json = excluded.column_metadata_formatting_options_json,
                  column_metadata_display_options_json = excluded.column_metadata_display_options_json;
                """,
                self._column_metadata_db_values(metadata),
            )
            conn.commit()
        finally:
            conn.close()

    def _validate_normalized_identity_metadata(
        self,
        metadata: ColumnMetadata,
    ) -> None:
        """
        Reject policy changes that disagree with a declared normalized identity.

        No declaration means no extra restriction. Otherwise the comparison column and
        normalization profile must match the declaration, and case sensitivity must be
        true exactly for NONE or UNICODE_NFC. A mismatch raises InputIntegrityError;
        identity lookup errors propagate. This helper writes nothing.

        Example:
            For the standard tag identity, changing ``tag_phash`` to ``None``
            in a proposed comparison_column fails validation.


        :param metadata: Proposed policy already validated against the physical schema.
        :return: None when the proposal is consistent or no identity is declared.
        """

        identity_spec = self.direct_get_normalized_identity_spec(
            metadata.table,
            metadata.column,
        )
        if identity_spec is None:
            return
        if metadata.comparison_column != identity_spec.identity_column:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"its comparison column must remain "
                f"{identity_spec.identity_column!r}."
            )
        if metadata.normalization_profile is not identity_spec.normalization_profile:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"its normalization profile must remain "
                f"{identity_spec.normalization_profile.value!r}."
            )
        expected_case_sensitive = identity_spec.normalization_profile in {
            ColumnNormalizationProfile.NONE,
            ColumnNormalizationProfile.UNICODE_NFC,
        }
        if metadata.case_sensitive is not expected_case_sensitive:
            raise InputIntegrityError(
                f"{metadata.table}.{metadata.column} is a normalized identity; "
                f"case sensitivity must remain {expected_case_sensitive!r}."
            )

    def direct_get_semantic_role(
        self,
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """
        Read the resolved column policy for semantic meaning.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            On an open driver, ``driver.direct_get_semantic_role("works",
            "work_title")`` returns that field from the complete metadata record.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: ColumnSemanticRole label describing the column's application meaning.
        """

        return self.direct_get_column_metadata(table, column).semantic_role

    def direct_set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Persist a replacement policy for semantic meaning.

        Read the current record, replace this field, then validate and commit the
        complete policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            With a suitable open driver::

                driver.direct_set_semantic_role(
                    "works", "work_title", ColumnSemanticRole.LABEL
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param semantic_role: ColumnSemanticRole label describing the column's
            application meaning.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            semantic_role=semantic_role,
        )

    def direct_get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the resolved column policy for comparison normalization.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            On an open driver, ``driver.direct_get_normalization_profile("works",
            "work_title")`` returns that field from the complete metadata record.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: ColumnNormalizationProfile identifying the comparison transformation.
        """

        return self.direct_get_column_metadata(table, column).normalization_profile

    def direct_set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Persist a replacement policy for comparison normalization.

        Read the current record, replace this field, then validate and commit the
        complete policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required. Changes
        that conflict with a normalized identity are rejected.

        Example:
            With a suitable open driver::

                driver.direct_set_normalization_profile(
                    "works", "work_title", ColumnNormalizationProfile.UNICODE_NFC
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param normalization_profile: ColumnNormalizationProfile identifying the
            comparison transformation.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            normalization_profile=normalization_profile,
        )

    def direct_get_comparison_column(
        self,
        table: str,
        column: str,
    ) -> str | None:
        """
        Read the physical column designated for normalized comparisons.

        Resolve the complete policy using the same target validation and legacy
        fallbacks as direct_get_column_metadata. This does not compute or refresh
        comparison values.

        Example:
            The standard tag policy returns ``"tag_phash"`` from
            ``driver.direct_get_comparison_column("tags", "tag")``.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: The comparison column name, or None when the policy names none.
        """

        return self.direct_get_column_metadata(table, column).comparison_column

    def _default_normalized_identity_if_supported(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Return a built-in identity only if all of its physical columns exist.

        Require the value, identity-key and every scope column. An unknown built-in
        identity returns None without inspecting headings. Callers provide an already
        validated target; heading lookup errors propagate.

        Example:
            A legacy tags table lacking ``tag_phash`` yields None for
            ``driver._default_normalized_identity_if_supported("tags", "tag")``.


        :param table: Canonical table name whose headings can be inspected.
        :param value_column: Value column used to select the built-in identity
            declaration.
        :return: The built-in NormalizedIdentitySpec, or None if unknown or unsupported.
        """
        spec = default_normalized_identity_spec(table, value_column)
        if spec is None:
            return None
        available = set(self.direct_get_column_headings(table))
        required = {
            spec.value_column,
            spec.identity_column,
            *spec.scope_columns,
        }
        return spec if required <= available else None

    def direct_get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Resolve a normalized identity declaration for one physical value column.

        Validate the target first. Only an absent identity catalog enables built-in
        defaults, and those require all referenced columns. If the catalog exists but
        its row is missing, return None without defaulting. Decode a stored row and
        check its value, key and scope columns. Declaration decoding errors propagate;
        missing physical references raise DatabaseIntegrityError. The read connection
        always closes.

        Example:
            ``driver.direct_get_normalized_identity_spec("genres", "genre")``
            returns a parent-scoped declaration on the standard FRBR schema.


        :param table: Existing table name, canonicalized by the driver.
        :param value_column: Existing physical value column whose identity is requested.
        :return: A validated NormalizedIdentitySpec, or None for an undeclared identity.
        """

        table_name, column_name = self._validated_column_metadata_target(
            table,
            value_column,
        )
        if NORMALIZED_IDENTITIES_TABLE not in set(self.direct_get_tables()):
            return self._default_normalized_identity_if_supported(
                table_name,
                column_name,
            )

        conn = self.get_connection()
        try:
            row = conn.execute(
                """
                SELECT
                  normalized_identity_table_name,
                  normalized_identity_value_column,
                  normalized_identity_key_column,
                  normalized_identity_normalization_profile,
                  normalized_identity_scope_columns_json,
                  normalized_identity_unique
                FROM normalized_identities
                WHERE normalized_identity_table_name = ?
                  AND normalized_identity_value_column = ?
                LIMIT 1;
                """,
                (table_name, column_name),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        spec = normalized_identity_from_db_values(*row)
        available = set(self.direct_get_column_headings(table_name))
        required = {
            spec.value_column,
            spec.identity_column,
            *spec.scope_columns,
        }
        if not required <= available:
            raise DatabaseIntegrityError(
                f"Normalized identity declaration for {table_name}.{column_name} "
                "references missing physical columns."
            )
        return spec

    def direct_iter_normalized_identity_specs(
        self,
    ) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield database identity declarations after checking physical references.

        Without a catalog, walk built-in defaults and omit absent tables or unsupported
        columns. With a catalog, fetch rows ordered by table and value column, close the
        connection, then decode and validate each row as iteration advances. An invalid
        later declaration may raise after earlier declarations have been yielded.

        Example:
            ``tuple(driver.direct_iter_normalized_identity_specs())`` materializes
            the declarations and forces validation of every returned row.


        :return: An iterator of NormalizedIdentitySpec records supported by the
            database.
        """

        if NORMALIZED_IDENTITIES_TABLE not in set(self.direct_get_tables()):
            for spec in iter_normalized_identity_defaults():
                if spec.table not in set(self.direct_get_tables()):
                    continue
                supported = self._default_normalized_identity_if_supported(
                    spec.table,
                    spec.value_column,
                )
                if supported is not None:
                    yield supported
            return

        conn = self.get_connection()
        try:
            rows = list(
                conn.execute(
                    """
                    SELECT
                      normalized_identity_table_name,
                      normalized_identity_value_column,
                      normalized_identity_key_column,
                      normalized_identity_normalization_profile,
                      normalized_identity_scope_columns_json,
                      normalized_identity_unique
                    FROM normalized_identities
                    ORDER BY normalized_identity_table_name,
                             normalized_identity_value_column;
                    """
                )
            )
        finally:
            conn.close()
        for row in rows:
            spec = normalized_identity_from_db_values(*row)
            available = set(self.direct_get_column_headings(spec.table))
            required = {
                spec.value_column,
                spec.identity_column,
                *spec.scope_columns,
            }
            if not required <= available:
                raise DatabaseIntegrityError(
                    f"Normalized identity declaration for "
                    f"{spec.table}.{spec.value_column} references missing physical columns."
                )
            yield spec

    def direct_set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Persist the physical column used for normalized comparisons.

        Replace the field in the current policy and commit the complete record. A non-
        null name must refer to a column in the same table; an immutable normalized
        identity may prevent changing or clearing it. The expanded catalog is required.
        Stored data is not recomputed.

        Example:
            For an ordinary title column::

                driver.direct_set_comparison_column(
                    "works", "work_title", "work_sort_title"
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param comparison_column: Same-table physical column name, or None to clear the
            reference.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            comparison_column=comparison_column,
        )

    def direct_get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """
        Read the resolved column policy for empty-value handling.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            On an open driver, ``driver.direct_get_empty_value_policy("works",
            "work_title")`` returns that field from the complete metadata record.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: ColumnEmptyValuePolicy describing which values count as missing.
        """

        return self.direct_get_column_metadata(table, column).empty_value_policy

    def direct_set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Persist a replacement policy for empty-value handling.

        Read the current record, replace this field, then validate and commit the
        complete policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            With a suitable open driver::

                driver.direct_set_empty_value_policy(
                    "works", "work_title", ColumnEmptyValuePolicy.PRESERVE
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param empty_value_policy: ColumnEmptyValuePolicy describing which values count
            as missing.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            empty_value_policy=empty_value_policy,
        )

    def direct_get_merge_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnMergePolicy:
        """
        Read the resolved column policy for merge behavior.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            On an open driver, ``driver.direct_get_merge_policy("works",
            "work_title")`` returns that field from the complete metadata record.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: ColumnMergePolicy describing how writers combine incoming values.
        """

        return self.direct_get_column_metadata(table, column).merge_policy

    def direct_set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Persist a replacement policy for merge behavior.

        Read the current record, replace this field, then validate and commit the
        complete policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            With a suitable open driver::

                driver.direct_set_merge_policy(
                    "works", "work_title", ColumnMergePolicy.PRESERVE_EXISTING
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param merge_policy: ColumnMergePolicy describing how writers combine incoming
            values.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            merge_policy=merge_policy,
        )

    def direct_get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the resolved column policy for semantic validation.

        Delegate to the complete metadata lookup, including catalog fallbacks and target
        validation. Reading the policy performs no transformation of stored values.

        Example:
            On an open driver, ``driver.direct_get_validation_profile("works",
            "work_title")`` returns that field from the complete metadata record.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: ColumnValidationProfile naming the value validation policy.
        """

        return self.direct_get_column_metadata(table, column).validation_profile

    def direct_set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Persist a replacement policy for semantic validation.

        Read the current record, replace this field, then validate and commit the
        complete policy. Other fields are retained from that read; this is not an atomic
        read-modify-write operation. The expanded metadata catalog is required.

        Example:
            With a suitable open driver::

                driver.direct_set_validation_profile(
                    "works", "work_title", ColumnValidationProfile.VERBATIM_TEXT
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param validation_profile: ColumnValidationProfile naming the value validation
            policy.
        :return: None after the complete policy write commits.
        """

        self._direct_replace_column_metadata(
            table,
            column,
            validation_profile=validation_profile,
        )

    def direct_get_formatting_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable value-formatting hints from the resolved column policy.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata.
        Nested mappings are read-only and nested sequences are tuples; this getter does
        not render a value or surface.

        Example:
            ``dict(driver.direct_get_formatting_options("works", "work_title"))``
            creates a shallow mutable copy of the top-level options.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: The frozen option mapping, possibly empty.
        """

        return self.direct_get_column_metadata(table, column).formatting_options

    def direct_set_formatting_options(
        self,
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement value-formatting hints.

        Freeze a deep copy before replacing the field in the current policy. Reject non-
        string keys, unsupported objects and non-finite numbers with
        InputIntegrityError. Persist through the complete metadata writer, which
        requires the expanded catalog and retains other fields from its prior read. The
        hints are stored without rendering anything.

        Example:
            With an open driver::

                driver.direct_set_formatting_options(
                    "works", "work_title", {"template": "{value}"}
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param formatting_options: JSON-like option mapping; None is also accepted at
            runtime as an empty mapping.
        :return: None after the complete policy write commits.
        """

        try:
            validated = freeze_column_options(
                formatting_options,
                field_name="formatting_options",
            )
        except (TypeError, ValueError) as exc:
            raise InputIntegrityError(str(exc)) from exc
        self._direct_replace_column_metadata(
            table,
            column,
            formatting_options=validated,
        )

    def direct_get_display_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read immutable surface-display hints from the resolved column policy.

        Catalog and legacy fallback behavior comes from direct_get_column_metadata.
        Nested mappings are read-only and nested sequences are tuples; this getter does
        not render a value or surface.

        Example:
            ``dict(driver.direct_get_display_options("works", "work_title"))``
            creates a shallow mutable copy of the top-level options.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: The frozen option mapping, possibly empty.
        """

        return self.direct_get_column_metadata(table, column).display_options

    def direct_set_display_options(
        self,
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Validate and persist replacement surface-display hints.

        Freeze a deep copy before replacing the field in the current policy. Reject non-
        string keys, unsupported objects and non-finite numbers with
        InputIntegrityError. Persist through the complete metadata writer, which
        requires the expanded catalog and retains other fields from its prior read. The
        hints are stored without rendering anything.

        Example:
            With an open driver::

                driver.direct_set_display_options(
                    "works", "work_title", {"label": "Title", "width": 42}
                )


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param display_options: JSON-like option mapping; None is also accepted at
            runtime as an empty mapping.
        :return: None after the complete policy write commits.
        """

        try:
            validated = freeze_column_options(
                display_options,
                field_name="display_options",
            )
        except (TypeError, ValueError) as exc:
            raise InputIntegrityError(str(exc)) from exc
        self._direct_replace_column_metadata(
            table,
            column,
            display_options=validated,
        )

    def _direct_replace_column_metadata(
        self,
        table: str,
        column: str,
        **changes: Any,
    ) -> None:
        """
        Read the complete policy, replace named fields and persist the result.

        Unspecified fields retain their values from the read. dataclasses.replace
        validates construction and the full writer validates schema and identities
        before committing. Unknown fields raise TypeError. The read and write are
        separate operations with no protection against concurrent policy changes.

        Example:
            ``driver._direct_replace_column_metadata("works", "work_title",
            merge_policy=ColumnMergePolicy.REPLACE)`` changes only that policy field.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param changes: ColumnMetadata field names and replacement values forwarded to
            dataclasses.replace.
        :return: None after the complete policy write commits.
        """

        current = self.direct_get_column_metadata(table, column)
        self.direct_set_column_metadata(replace(current, **changes))

    def direct_set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Commit a case-sensitivity flag through the legacy-compatible catalog path.

        Validate the physical column, require an actual bool and enforce any normalized
        identity restriction. A missing catalog raises DatabaseIntegrityError. Upsert
        just the case flag, preserving other fields on an existing row; a new row
        receives the catalog defaults. Expanded policy columns are not required. Close
        the connection after the committed write or any error.

        Example:
            ``driver.direct_set_case_sensitivity("works", "work_title", True)``
            changes equality policy without rewriting title values.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param case_sensitive: Exact bool: True for case-sensitive equality, False
            otherwise.
        :return: None after the flag write commits.
        """
        table_name, column_name = self._validated_column_metadata_target(table, column)
        if type(case_sensitive) is not bool:
            raise InputIntegrityError("case_sensitive must be a bool")
        self._validate_normalized_identity_metadata(
            replace(
                self.direct_get_column_metadata(table_name, column_name),
                case_sensitive=case_sensitive,
            )
        )
        if COLUMN_METADATA_TABLE not in set(self.direct_get_tables()):
            raise DatabaseIntegrityError(
                "database has no column_metadata table; migrate the schema before storing column policy"
            )

        conn = self.get_connection()
        try:
            conn.execute(
                """
                INSERT INTO column_metadata (
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive
                ) VALUES (?, ?, ?)
                ON CONFLICT (
                  column_metadata_table_name,
                  column_metadata_column_name
                ) DO UPDATE SET
                  column_metadata_case_sensitive = excluded.column_metadata_case_sensitive;
                """,
                (table_name, column_name, int(case_sensitive)),
            )
            conn.commit()
        finally:
            conn.close()

    def direct_is_column_case_sensitive(self, table: str, column: str) -> bool:
        """
        Expose the compatibility name for the resolved case-sensitivity flag.

        Delegate to direct_get_case_sensitivity with identical validation and catalog
        fallbacks.

        Example:
            ``driver.direct_is_column_case_sensitive("tags", "tag")`` agrees
            with ``driver.direct_get_case_sensitivity("tags", "tag")``.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
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
        Persist case sensitivity through the compatibility setter name.

        Delegate to direct_set_case_sensitivity, including exact-bool validation,
        identity checks and its committed legacy-compatible catalog write.

        Example:
            ``driver.direct_set_column_case_sensitive("works", "work_title", False)``
            uses the same write path as direct_set_case_sensitivity.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param case_sensitive: Exact bool controlling case-sensitive equality.
        :return: None after the delegated write commits.
        """

        self.direct_set_case_sensitivity(table, column, case_sensitive)

    def _validated_column_metadata_target(self, table: str, column: str) -> tuple[str, str]:
        """
        Canonicalize a table and require its physical column to exist.

        Convert the column argument with str and check driver headings. Missing columns
        raise InputIntegrityError; table canonicalization or lookup errors propagate.

        Example:
            ``driver._validated_column_metadata_target("works", "work_title")``
            returns ``("works", "work_title")`` for the standard schema.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :return: A pair containing the canonical table name and string column name.
        """
        table_name = self._canonicalise_table_name_for_cache(table)
        column_name = str(column)
        headings = self.direct_get_column_headings(table_name)
        if column_name not in headings:
            raise InputIntegrityError(f"column {column_name!r} not found in table {table_name!r}")
        return table_name, column_name

    def _validated_column_metadata_input(self, metadata: ColumnMetadata) -> ColumnMetadata:
        """
        Copy policy into canonical, schema-checked metadata with enum members.

        Require a ColumnMetadata instance and an exact bool flag. Canonicalize the
        target and check any comparison column in the same table. Coerce policy labels
        to their enums and reconstruct the record, freezing presentation options.
        Invalid inputs raise InputIntegrityError. This helper does not check normalized
        identity restrictions or write a catalog row.

        Example:
            A proposal with ``comparison_column="missing"`` raises
            InputIntegrityError when that physical column is absent.


        :param metadata: ColumnMetadata proposal whose target, policies and options will
            be checked.
        :return: A new validated ColumnMetadata record.
        """
        if not isinstance(metadata, ColumnMetadata):
            raise InputIntegrityError("metadata must be a ColumnMetadata instance")
        table_name, column_name = self._validated_column_metadata_target(
            metadata.table,
            metadata.column,
        )
        if type(metadata.case_sensitive) is not bool:
            raise InputIntegrityError("ColumnMetadata.case_sensitive must be a bool")
        comparison_column = metadata.comparison_column
        if comparison_column is not None:
            comparison_column = str(comparison_column)
            if comparison_column not in set(self.direct_get_column_headings(table_name)):
                raise InputIntegrityError(
                    f"comparison column {comparison_column!r} not found in table {table_name!r}"
                )
        try:
            return ColumnMetadata(
                table=table_name,
                column=column_name,
                case_sensitive=metadata.case_sensitive,
                semantic_role=ColumnSemanticRole(metadata.semantic_role),
                normalization_profile=ColumnNormalizationProfile(
                    metadata.normalization_profile
                ),
                comparison_column=comparison_column,
                empty_value_policy=ColumnEmptyValuePolicy(metadata.empty_value_policy),
                merge_policy=ColumnMergePolicy(metadata.merge_policy),
                validation_profile=ColumnValidationProfile(
                    metadata.validation_profile
                ),
                formatting_options=metadata.formatting_options,
                display_options=metadata.display_options,
            )
        except (TypeError, ValueError) as exc:
            raise InputIntegrityError(f"invalid column metadata policy: {exc}") from exc

    def _column_metadata_from_values(
        self,
        table: str,
        column: str,
        case_sensitive: Any,
        semantic_role: Any,
        normalization_profile: Any,
        comparison_column: Any,
        empty_value_policy: Any,
        merge_policy: Any,
        validation_profile: Any,
        formatting_options_json: Any = None,
        display_options_json: Any = None,
    ) -> ColumnMetadata:
        """
        Decode an expanded catalog row and validate its physical target.

        Coerce the case flag, decode enum labels and presentation JSON, then apply input
        validation. A null validation label uses the built-in default; null option JSON
        becomes an empty mapping. Decode/type errors become DatabaseIntegrityError.
        Subsequent physical-target and comparison-column validation can raise
        InputIntegrityError.

        Example:
            A legacy expanded row can pass None for both presentation JSON
            arguments to obtain empty immutable option maps.


        :param table: Existing table name, canonicalized by the driver.
        :param column: Physical column name, converted to text and checked against
            headings.
        :param case_sensitive: Stored flag accepted by _coerce_column_case_sensitivity.
        :param semantic_role: Stored semantic-role label, converted through str to its
            enum.
        :param normalization_profile: Stored comparison-normalization label.
        :param comparison_column: Stored physical comparison column, or None.
        :param empty_value_policy: Stored missing-value policy label.
        :param merge_policy: Stored merge-policy label.
        :param validation_profile: Stored validation label, or None to use the built-in
            default.
        :param formatting_options_json: Serialized formatting options, or None for an
            empty mapping.
        :param display_options_json: Serialized display options, or None for an empty
            mapping.
        :return: The decoded and schema-checked ColumnMetadata record.
        """
        try:
            metadata = ColumnMetadata(
                table=table,
                column=column,
                case_sensitive=self._coerce_column_case_sensitivity(
                    case_sensitive,
                    table,
                    column,
                ),
                semantic_role=ColumnSemanticRole(str(semantic_role)),
                normalization_profile=ColumnNormalizationProfile(
                    str(normalization_profile)
                ),
                comparison_column=(
                    str(comparison_column) if comparison_column is not None else None
                ),
                empty_value_policy=ColumnEmptyValuePolicy(str(empty_value_policy)),
                merge_policy=ColumnMergePolicy(str(merge_policy)),
                validation_profile=(
                    ColumnValidationProfile(str(validation_profile))
                    if validation_profile is not None
                    else default_column_metadata(table, column).validation_profile
                ),
                formatting_options=column_options_from_json(
                    formatting_options_json,
                    field_name="formatting_options",
                ),
                display_options=column_options_from_json(
                    display_options_json,
                    field_name="display_options",
                ),
            )
        except (TypeError, ValueError) as exc:
            raise DatabaseIntegrityError(
                f"invalid column metadata for {table}.{column}: {exc}"
            ) from exc
        return self._validated_column_metadata_input(metadata)

    @staticmethod
    def _column_metadata_db_values(metadata: ColumnMetadata) -> tuple[Any, ...]:
        """
        Serialize a validated policy into the eleven catalog parameter values.

        Keep table and column names first, encode case sensitivity as an integer and
        enums by their string values, then serialize both option mappings as JSON. This
        helper does not validate physical columns or write SQL.

        Example:
            >>> metadata = default_column_metadata("works", "work_title")
            >>> values = ValueCastingMixin._column_metadata_db_values(metadata)
            >>> values[:2], len(values)
            (('works', 'work_title'), 11)
            >>> values[-2:]
            ('{}', '{}')


        :param metadata: Validated ColumnMetadata with enum-valued policy fields.
        :return: An eleven-item tuple in the complete metadata INSERT parameter order.
        """
        return (
            metadata.table,
            metadata.column,
            int(metadata.case_sensitive),
            metadata.semantic_role.value,
            metadata.normalization_profile.value,
            metadata.comparison_column,
            metadata.empty_value_policy.value,
            metadata.merge_policy.value,
            metadata.validation_profile.value,
            column_options_to_json(metadata.formatting_options),
            column_options_to_json(metadata.display_options),
        )

    @staticmethod
    def _coerce_column_case_sensitivity(value: Any, table: str, column: str) -> bool:
        """
        Decode a stored zero-or-one flag using Python equality comparisons.

        Accept 0, False and "0" as false, and 1, True and "1" as true. Equality also
        admits numeric equivalents such as 0.0; this is not the exact-bool validation
        used by setters. Other values raise DatabaseIntegrityError with the table and
        column in the diagnostic.

        Example:
            >>> ValueCastingMixin._coerce_column_case_sensitivity("0", "works", "work_title")
            False
            >>> ValueCastingMixin._coerce_column_case_sensitivity(1.0, "works", "work_title")
            True


        :param value: Stored case flag to compare with accepted zero and one forms.
        :param table: Table name used only in an invalid-value diagnostic.
        :param column: Column name used only in an invalid-value diagnostic.
        :return: The decoded bool.
        """
        if value in (0, False, "0"):
            return False
        if value in (1, True, "1"):
            return True
        raise DatabaseIntegrityError(
            f"invalid case-sensitivity metadata for {table}.{column}: {value!r}"
        )

    def direct_get_declared_types_for_table(self, table: str) -> Dict[str, str]:
        """
        Cache SQLite PRAGMA declarations by the supplied table key.

        Initialize the per-instance cache lazily, then return a cached dictionary
        directly on later calls. A miss reads table_info and stores column names mapped
        to their original type strings, replacing null declarations with empty text.
        Unknown tables can yield an empty mapping. The connection closes after
        successful iteration; this method has no finally cleanup on failure. Table text
        is interpolated into PRAGMA without quoting or validation.

        Example:
            Repeated ``driver.direct_get_declared_types_for_table("works")``
            calls return the same dictionary until the schema cache is invalidated.


        :param table: Trusted SQL table spelling suitable for direct PRAGMA
            interpolation; also the exact cache key.
        :return: The cached mutable column-to-declared-type dictionary, not a copy.
        """
        cache = getattr(self, self._DECLARED_TYPES_CACHE_ATTR, None)
        if cache is None:
            cache = {}
            setattr(self, self._DECLARED_TYPES_CACHE_ATTR, cache)

        if table in cache:
            return cache[table]

        stmt = f"PRAGMA table_info({table})"
        conn = self.get_connection()
        c = conn.cursor()
        types: Dict[str, str] = {}
        for row in c.execute(stmt):
            # row: (cid, name, type, notnull, dflt_value, pk)
            name = row[1]
            decl = row[2] or ""
            types[name] = decl
        conn.close()

        cache[table] = types
        return types

    @staticmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        """
        Uppercase the first type token and remove a parenthesized size suffix.

        Convert non-null input with str, trim surrounding whitespace and discard tokens
        after the first. This is a lexical convenience rather than a SQL declaration
        parser.

        Example:
            >>> ValueCastingMixin._normalize_declared_type(" varchar(255) NOT NULL ")
            'VARCHAR'
            >>> ValueCastingMixin._normalize_declared_type(None)
            ''
            >>> ValueCastingMixin._normalize_declared_type("double precision")
            'DOUBLE'


        :param declared_type: Backend type declaration or another stringifiable value;
            None means no type.
        :return: Uppercase first token without its size suffix, or empty text.
        """
        if declared_type is None:
            return ""
        dt = str(declared_type).strip().upper()
        if not dt:
            return ""
        # Strip constraints / extras and size spec (e.g. VARCHAR(255))
        parts = dt.split()
        if not parts:
            return ""
        dt = parts[0]
        dt = dt.split("(", 1)[0]
        return dt

    @classmethod
    def _sqlite_affinity(cls, declared_type: Any) -> str:
        """
        Select this mixin's conversion bucket from the normalized first type token.

        Check substrings in order: INT, then CHAR/CLOB/TEXT, then BLOB, then
        REAL/FLOA/DOUB. Empty and unrecognized tokens fall through to NUMERIC.
        The preceding normalization discards size suffixes and later tokens;
        this is a simplified classification, not a complete SQL type parser.

        Example:
            >>> ValueCastingMixin._sqlite_affinity("varchar(255)")
            'TEXT'
            >>> ValueCastingMixin._sqlite_affinity("INTTEXT")
            'INTEGER'
            >>> ValueCastingMixin._sqlite_affinity(None)
            'NUMERIC'


        :param declared_type: Type declaration passed to _normalize_declared_type;
            None or blank text produces the NUMERIC fallback.
        :return: One of INTEGER, TEXT, BLOB, REAL or NUMERIC.
        """
        dt = cls._normalize_declared_type(declared_type)

        # SQLite affinity rules (simplified)
        if "INT" in dt:
            return "INTEGER"
        if any(x in dt for x in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        if "BLOB" in dt:
            return "BLOB"
        if any(x in dt for x in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        return "NUMERIC"

    # Todo: We can... possibly make this better with some protocol work
    def _coerce_db_value(
            self,
            value: Any,
            declared_type: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Convert a database cell according to the declared type's conversion bucket.

        Preserve None. Numeric buckets convert booleans to numbers and retain
        numeric objects; INTEGER turns integral floats into ints, while REAL
        converts numeric objects to floats. INTEGER and NUMERIC try integer text
        before decimal or exponent text; REAL parses matching text as a float.
        Whitespace is stripped for parsing only. Malformed numeric text retains
        its original spelling, and failed numeric-text conversions are suppressed.
        No finite-value check is applied, so overflow text can produce infinity.

        BLOB converts bytes, bytearray and memoryview to bytes. Other fallback
        paths use force_unicode, currently an alias for str: byte-like inputs
        outside BLOB are represented as text rather than decoded, so b"12" does
        not become the number 12. TEXT stringifies non-null values. Errors from
        stringification or direct numeric-object conversion can propagate.

        Example:
            >>> caster = ValueCastingMixin()
            >>> caster._coerce_db_value(" +12 ", "INTEGER")
            12
            >>> caster._coerce_db_value(2.25, "INTEGER")
            2.25
            >>> caster._coerce_db_value("1e2", "INTEGER")
            100.0
            >>> caster._coerce_db_value("  invalid  ", "REAL")
            '  invalid  '
            >>> caster._coerce_db_value(b"12", "INTEGER")
            "b'12'"
            >>> caster._coerce_db_value(memoryview(b"data"), "BLOB")
            b'data'
            >>> caster._coerce_db_value(True, "REAL")
            1.0
            >>> caster._coerce_db_value("1e999", "NUMERIC")
            inf
            >>> caster._coerce_db_value(12, "TEXT")
            '12'
            >>> caster._coerce_db_value(None, "INTEGER") is None
            True


        :param value: Database cell to convert; no container or text decoding
            protocol is imposed on fallback objects.
        :param declared_type: Backend declaration used by _sqlite_affinity; an
            empty or unknown declaration still selects NUMERIC conversion.
        :return: None, an int or float, bytes for byte-like BLOB input, or text.
        """
        if value is None:
            return None

        affinity = self._sqlite_affinity(declared_type)

        if affinity == "INTEGER":
            # Coerce common string/bytes representations of integers.
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            # SQLite is dynamically typed: even an INTEGER-affinity column may
            # legitimately contain REAL values (e.g. priority=2.25). Preserve
            # non-integer floats rather than stringifying them.
            if isinstance(value, float):
                return int(value) if value.is_integer() else float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    # Preserve float-ish numeric strings in INTEGER columns
                    # (SQLite allows this; callers often expect numeric back).
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "REAL":
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        if affinity == "BLOB":
            if isinstance(value, memoryview):
                return bytes(value)
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
            # If the driver hands us something odd, keep it visible as text
            return force_unicode(value)

        if affinity == "NUMERIC":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float):
                return float(value)

            if isinstance(value, (bytes, bytearray, memoryview)):
                s = force_unicode(value)
                if isinstance(s, str):
                    s2 = s.strip()
                    if re.fullmatch(r"[+-]?\d+", s2):
                        try:
                            return int(s2)
                        except Exception:
                            pass
                    if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                        try:
                            return float(s2)
                        except Exception:
                            pass
                return s

            if isinstance(value, str):
                s2 = value.strip()
                if re.fullmatch(r"[+-]?\d+", s2):
                    try:
                        return int(s2)
                    except Exception:
                        pass
                if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", s2):
                    try:
                        return float(s2)
                    except Exception:
                        pass
                return value

            return force_unicode(value)

        # TEXT
        return force_unicode(value)

    @staticmethod
    def _coerce_untyped_value(value: Any) -> Optional[Union[bool, int, float, str, bytes]]:
        """
        Preserve scalar numeric kinds and stringify values without a declared type.

        Return None unchanged and coerce bool, int and float instances to their
        corresponding built-in types, testing bool first. Convert memoryview to
        bytes. For bytes and bytearray, first make bytes and then call force_unicode,
        currently str, so the result includes the bytes representation rather than
        decoded content. Other values also use str; its errors propagate.

        Numeric strings retain their text and whitespace because no affinity is
        available to justify parsing them.

        Example:
            >>> ValueCastingMixin._coerce_untyped_value(" 12 ")
            ' 12 '
            >>> ValueCastingMixin._coerce_untyped_value(True)
            True
            >>> ValueCastingMixin._coerce_untyped_value(bytearray(b"12"))
            "b'12'"
            >>> ValueCastingMixin._coerce_untyped_value(memoryview(b"12"))
            b'12'
            >>> ValueCastingMixin._coerce_untyped_value(None) is None
            True


        :param value: Cell whose type is not described by a table declaration.
        :return: None, a built-in bool/int/float, bytes for memoryview, or text.
        """
        if value is None:
            return None

        if isinstance(value, bool):
            # bool is a subtype of int; preserve intent
            return bool(value)

        if isinstance(value, int):
            return int(value)

        if isinstance(value, float):
            return float(value)

        if isinstance(value, memoryview):
            return bytes(value)

        if isinstance(value, (bytes, bytearray)):
            return force_unicode(bytes(value))

        return force_unicode(value)

    def _row_to_dict(
        self,
        *,
        table: Optional[str] = None,
        headings: Sequence[Any],
        row: Sequence[Any],
    ) -> Dict[Any, Any]:
        """
        Pair headings with converted row cells, retaining set-valued cells by identity.

        A truthy table selects its cached declared-type map and typed conversion.
        Missing entries use an empty declaration, which selects NUMERIC; a falsey
        table instead uses untyped conversion. Fetch the map before iterating,
        even for empty headings. Set-valued cells bypass conversion without copying.

        Iterate headings and index the row: surplus cells are ignored and too few
        cells raise IndexError. Duplicate headings overwrite earlier values.
        Headings must be hashable; the legacy set-heading branch still attempts
        dictionary insertion, so a plain set heading raises TypeError. Schema
        lookup and cell conversion errors propagate.

        Example:
            >>> caster = ValueCastingMixin()
            >>> caster._row_to_dict(headings=["value"], row=["12"])
            {'value': '12'}
            >>> caster._declared_types_cache = {"sample": {"value": "INTEGER"}}
            >>> caster._row_to_dict(table="sample", headings=["value"], row=["12"])
            {'value': 12}
            >>> paths = {"a.epub"}
            >>> caster._row_to_dict(headings=["paths"], row=[paths])["paths"] is paths
            True
            >>> caster._row_to_dict(headings=["value", "value"], row=[1, 2, 3])
            {'value': 2}


        :param table: Trusted table spelling for declared-type lookup, or a falsey
            value such as None to avoid that lookup and use untyped conversion.
        :param headings: Ordered dictionary keys; may contain non-string hashable
            values and duplicates, but plain set keys are unsupported.
        :param row: Indexable cell sequence with at least one value per heading.
        :return: A new heading-to-value dictionary; retained sets are shared with row.
        """
        declared_types = self.direct_get_declared_types_for_table(table) if table else {}
        result: Dict[Any, Any] = {}
        for i, head in enumerate(headings):
            val = row[i]
            # Preserve set-valued cells used by legacy 'set column' code paths
            if isinstance(val, set):
                result[head] = val
                continue
            # Some legacy code can yield non-string headings (e.g. set markers for set columns).
            if isinstance(head, set):
                result[head] = val
                continue
            if table:
                result[head] = self._coerce_db_value(val, declared_types.get(head, ""))
            else:
                result[head] = self._coerce_untyped_value(val)
        return result
