
"""
Expose schema metadata, column policies and canonical identities through a database facade.

Most methods delegate unchanged arguments to the wrapper or macros. UUID/version properties add local caches with their own refresh and write behavior. Column-policy reads may support older schemas through driver fallbacks; facade setters do not migrate those schemas automatically.
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
    ColumnOptions,
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
    Combine cached library identifiers with schema and column-policy delegates.

    Requires initialized driver, wrapper and macros collaborators. Metadata policy setters and identity migration can write to the database; getters normally query or return cached values, except library_id can generate and store a missing identifier.

    Example:
        For an open db, metadata = db.get_column_metadata("works", "work_title") reads its column policy; db.get_tables(force_refresh=True) asks the wrapper for fresh schema names.
    """
    @property
    def uuid(self: "DatabaseAPI") -> str:
        """
        Return the cached database UUID or load it once from the wrapper.

        A None cache triggers another load. The _uuid attribute must already exist; this getter does not generate a missing UUID itself.

        Example:
            >>> state = DatabaseMetadataMixin()
            >>> state._uuid = "cached-id"
            >>> state.uuid
            'cached-id'


        :return: Cached wrapper UUID value.
        """
        if self._uuid is not None:
            return self._uuid
        else:
            self._uuid = self.driver_wrapper.get_uuid()
            return self._uuid

    @uuid.setter
    def uuid(self: "DatabaseAPI", value: str) -> None:
        """
        Assign the UUID cache before asking the wrapper to persist it.

        If persistence fails, the assigned cache is not restored.

        Example:
            For an open db, db.uuid = replacement_uuid updates the local cache and delegates storage.


        :param value: UUID value passed unchanged to the wrapper.
        :return: None.
        """
        self._uuid = value
        self.driver_wrapper.set_uuid(value)

    @property
    def library_id(self: "DatabaseAPI") -> str:
        """
        Read a cached library identifier, generating one when the query returns None.

        The nonempty query result is retained directly without extracting its first column; its shape therefore depends on wrapper.get(all=False). A missing value invokes the setter and writes through macros. Reading this property is not always read-only.

        Example:
            >>> state = DatabaseMetadataMixin()
            >>> state._library_id_ = "cached-library"
            >>> state.library_id
            'cached-library'


        :return: Cached wrapper query result, or generated UUID text.
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
        Cache the textual library identifier and delegate storage of the original value.

        The cache changes before storage and is not restored on failure.

        Example:
            For an open db, db.library_id = replacement_uuid caches its text and calls the library-ID macro.


        :param value: Identifier converted to text for the cache but passed unchanged to macros.
        :return: None.
        """
        self._library_id_ = six_unicode(value)
        self.macros.set_library_id(value)

    @property
    def database_version(self: "DatabaseAPI") -> str:
        """
        Return the cached version or read the last version row from the primary connection.

        A None result is queried again on the next access. This method does not create a version row or explicitly close its cursor.

        Example:
            >>> state = DatabaseMetadataMixin()
            >>> state._database_version_ = "v1"
            >>> state.database_version
            'v1'


        :return: Last queried version value, or None when there are no rows.
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
        Cache version text before passing the original value to its storage macro.

        The cached value remains changed if the macro rejects a protected version table or otherwise fails.

        Example:
            When the backend permits version changes, db.database_version = version delegates persistence after updating the cache.


        :param value: Version converted to text for the cache and passed unchanged to macros.
        :return: None.
        """
        self._database_version_ = six_unicode(value)
        self.macros.set_database_version(value)


    # ----------------------------------------------------------------
    #
    # - METHODS TO GET BASIC INFORMATION ABOUT THE DATABASE START HERE

    def get_tables(self: "DatabaseAPI", force_refresh: bool = False) -> list[str]:
        """
        Delegate table discovery and optional schema-cache refresh to the wrapper.

        Example:
            For an open db, db.get_tables(force_refresh=True) requests refreshed schema names.


        :param force_refresh: Request fresh backend discovery when True.
        :return: Wrapper table-name list.
        """
        return self.driver_wrapper.get_tables(force_refresh=force_refresh)

    # Methods to get basic information about the database start here
    def get_column_headings(self: "DatabaseAPI", table: str) -> list[str]:
        """
        Delegate ordered column discovery for a table.

        Example:
            For an open db, db.get_column_headings("works") identifies available work fields.


        :param table: Target table name.
        :return: Wrapper column-name list in backend order.
        """
        return self.driver_wrapper.get_column_headings(table)

    def get_declared_column_datatype(self: "DatabaseAPI", table: str, column: str) -> str:
        """
        Read the backend-declared type name for one column.

        Example:
            For the current schema, db.get_declared_column_datatype("database_metadata", "database_metadata_unique_id") returns "TEXT".


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: Datatype string returned by the wrapper.
        """
        return self.driver_wrapper.get_declared_column_datatype(table, column)

    def get_link_capabilities(
        self: "DatabaseAPI",
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Retrieve the schema capabilities of an interlink or self-link.

        Example:
            For an open db, capabilities = db.get_link_capabilities("agents", "works") describes its type/priority columns.


        :param table1: First endpoint table.
        :param table2: Second endpoint table; may match the first for a self-link.
        :param force_refresh: Request refreshed schema capability discovery.
        :return: LinkCapabilities, or None when no supported relationship is found.
        """

        return self.driver_wrapper.get_link_capabilities(
            table1,
            table2,
            force_refresh=force_refresh,
        )

    def is_link_typed(
        self: "DatabaseAPI",
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Ask the wrapper whether a relationship supports a type column.

        Example:
            For an open db, db.is_link_typed("agents", "works") tests schema capability rather than a particular stored link type.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Refresh capability discovery when True.
        :return: Wrapper boolean capability result.
        """

        return self.driver_wrapper.is_link_typed(
            table1,
            table2,
            force_refresh=force_refresh,
        )

    def is_link_priority(
        self: "DatabaseAPI",
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Ask the wrapper whether a relationship supports priority ordering.

        Example:
            For an open db, db.is_link_priority("agents", "works") checks whether its relationship schema supports priority.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Refresh capability discovery when True.
        :return: Wrapper boolean capability result.
        """

        return self.driver_wrapper.is_link_priority(
            table1,
            table2,
            force_refresh=force_refresh,
        )

    def get_case_sensitivity(self: "DatabaseAPI", table: str, column: str) -> bool:
        """
        Read the case-sensitive comparison flag.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_case_sensitivity("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper bool flag result.
        """
        return self.driver_wrapper.get_case_sensitivity(table, column)

    def get_column_metadata(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnMetadata:
        """
        Retrieve the effective policy record for a database column.

        Example:
            For an open db, policy = db.get_column_metadata("works", "work_title") combines normalization, merge and presentation settings.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: ColumnMetadata returned by the wrapper, including driver-supported legacy read fallbacks.
        """

        return self.driver_wrapper.get_column_metadata(table, column)

    def set_column_metadata(
        self: "DatabaseAPI",
        metadata: ColumnMetadata,
    ) -> None:
        """
        Delegate persistence of a complete column-policy record.

        No validation or schema migration is added at this facade boundary; driver errors propagate. Legacy read fallbacks do not imply an old schema supports policy writes.

        Example:
            After deriving an updated policy with dataclasses.replace, db.set_column_metadata(policy) delegates its persistence.


        :param metadata: ColumnMetadata identifying its table/column and replacement policy.
        :return: None.
        """

        self.driver_wrapper.set_column_metadata(metadata)

    def get_semantic_role(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnSemanticRole:
        """
        Read the semantic role of the stored value.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_semantic_role("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnSemanticRole describing the value meaning result.
        """

        return self.driver_wrapper.get_semantic_role(table, column)

    def set_semantic_role(
        self: "DatabaseAPI",
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Delegate storage of the semantic role of the stored value.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_semantic_role("works", "work_title", ColumnSemanticRole.LABEL) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param semantic_role: ColumnSemanticRole describing the value meaning to persist.
        :return: None.
        """

        self.driver_wrapper.set_semantic_role(table, column, semantic_role)

    def get_normalization_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the normalization profile for comparable values.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_normalization_profile("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnNormalizationProfile result.
        """

        return self.driver_wrapper.get_normalization_profile(table, column)

    def set_normalization_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Delegate storage of the normalization profile for comparable values.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_normalization_profile("works", "work_title", ColumnNormalizationProfile.UNICODE_NFC) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param normalization_profile: ColumnNormalizationProfile to persist.
        :return: None.
        """

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
        """
        Read the companion column used for comparisons.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_comparison_column("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Companion column name, or None result.
        """

        return self.driver_wrapper.get_comparison_column(table, column)

    def get_normalized_identity_spec(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Read the normalized-identity declaration for a display-value column.

        Example:
            For the current schema, db.get_normalized_identity_spec("tags", "tag") returns the declaration used to identify canonical tag rows.


        :param table: Table containing the value column.
        :param value_column: Display-value column whose identity is declared.
        :return: NormalizedIdentitySpec, or None when no declaration applies.
        """

        return self.driver_wrapper.get_normalized_identity_spec(table, value_column)

    def iter_normalized_identity_specs(
        self: "DatabaseAPI",
    ) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield normalized-identity declarations from the wrapper iterator.

        Iteration is lazy: wrapper iteration and its errors occur when the returned generator is consumed.

        Example:
            For an open db, specs = tuple(db.iter_normalized_identity_specs()) consumes all current declarations.


        :return: Iterator yielding NormalizedIdentitySpec objects.
        """

        yield from self.driver_wrapper.iter_normalized_identity_specs()

    def derive_identity_value(
        self: "DatabaseAPI",
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """
        Normalize a display value using its declared identity profile.

        The macros layer validates identifiers and requires an applicable normalized-identity declaration.

        Example:
            For an open db, key = db.derive_identity_value("tags", "tag", " Example ") derives the key used for canonical lookup.


        :param table: Table containing the display value.
        :param value_column: Declared display-value column.
        :param value: Value to normalize.
        :return: Derived identity key returned by macros, without creating a row.
        """

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
        """
        Normalize a display value and look up its unique canonical identity.

        Delegates normalization and scoped lookup to macros; multiple matches raise DatabaseIntegrityError. This does not insert missing values.

        Example:
            For an open db, identity = db.get_canonical_identity("tags", "tag", " EXAMPLE ") finds the stored spelling and row ID for an equivalent value.


        :param table: Table containing the canonical row.
        :param value_column: Declared display-value column.
        :param value: Display value to normalize before lookup.
        :param scope_values: Optional values for the declaration scope columns.
        :param id_column: Optional row-ID column override.
        :return: CanonicalIdentity for a unique match, or None when absent.
        """

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
        """
        Resolve an already-derived identity key without normalizing it again.

        Macros enforce scope and ID-column validity and reject ambiguous matches; the facade forwards all arguments unchanged.

        Example:
            After key = db.derive_identity_value("tags", "tag", text), db.get_canonical_identity_by_key("tags", "tag", key) performs the canonical lookup.


        :param table: Table containing the canonical row.
        :param value_column: Declared display-value column.
        :param identity_value: Non-None normalized key used directly for lookup.
        :param scope_values: Optional values for the declaration scope columns.
        :param id_column: Optional row-ID column override.
        :return: CanonicalIdentity for a unique match, or None when absent.
        """

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
        """
        Return the stored spelling matching a normalized display-value lookup.

        Example:
            For an existing canonical tag, db.get_canonical_value("tags", "tag", alternate_spelling) returns its stored spelling.


        :param table: Table containing the canonical value.
        :param value_column: Declared display-value column.
        :param value: Display value normalized by macros.
        :param scope_values: Optional values for declared scope columns.
        :return: Canonical stored value, or None when no row matches.
        """

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
        """
        Return the stored spelling matching an already-derived identity key.

        Example:
            For an open db and derived tag key, db.get_canonical_value_by_identity("tags", "tag", key) reads the canonical spelling without re-normalizing the key.


        :param table: Table containing the canonical value.
        :param value_column: Declared display-value column.
        :param identity_value: Normalized key passed directly to macros.
        :param scope_values: Optional values for declared scope columns.
        :return: Canonical stored value, or None when no row matches.
        """

        return self.macros.get_canonical_value_by_identity(
            table,
            value_column,
            identity_value,
            scope_values=scope_values,
        )

    def audit_normalized_identities(
        self: "DatabaseAPI",
    ) -> NormalizedIdentityMigrationReport:
        """
        Delegate inspection of stale identity keys and collisions without migration writes.

        Example:
            For an open db, report = db.audit_normalized_identities() reports collisions before a planned migration.


        :return: NormalizedIdentityMigrationReport describing examined declarations and rows.
        """

        return self.macros.audit_normalized_identities()

    def migrate_normalized_identities(
        self: "DatabaseAPI",
    ) -> NormalizedIdentityMigrationReport:
        """
        Delegate transactional identity-catalog installation, backfill and indexing.

        The macros layer inspects for collisions before applying changes and raises DatabaseIntegrityError when collisions prevent migration. The facade adds no outer transaction or cache management.

        Example:
            After reviewing an identity audit, report = db.migrate_normalized_identities() applies the macros migration to a writable database.


        :return: NormalizedIdentityMigrationReport describing completed changes.
        """

        return self.macros.migrate_normalized_identities()

    def set_comparison_column(
        self: "DatabaseAPI",
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Delegate storage of the companion column used for comparisons.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_comparison_column("works", "work_title", "work_sort_title") requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param comparison_column: Companion column name, or None to persist.
        :return: None.
        """

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
        """
        Read the policy for empty input values.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_empty_value_policy("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnEmptyValuePolicy result.
        """

        return self.driver_wrapper.get_empty_value_policy(table, column)

    def set_empty_value_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Delegate storage of the policy for empty input values.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_empty_value_policy("works", "work_title", ColumnEmptyValuePolicy.PRESERVE) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param empty_value_policy: ColumnEmptyValuePolicy to persist.
        :return: None.
        """

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
        """
        Read the policy for combining an incoming value with existing data.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_merge_policy("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnMergePolicy result.
        """

        return self.driver_wrapper.get_merge_policy(table, column)

    def set_merge_policy(
        self: "DatabaseAPI",
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Delegate storage of the policy for combining an incoming value with existing data.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_merge_policy("works", "work_title", ColumnMergePolicy.PRESERVE_EXISTING) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param merge_policy: ColumnMergePolicy to persist.
        :return: None.
        """

        self.driver_wrapper.set_merge_policy(table, column, merge_policy)

    def get_validation_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the validation profile for incoming values.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_validation_profile("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnValidationProfile result.
        """

        return self.driver_wrapper.get_validation_profile(table, column)

    def set_validation_profile(
        self: "DatabaseAPI",
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Delegate storage of the validation profile for incoming values.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_validation_profile("works", "work_title", ColumnValidationProfile.VERBATIM_TEXT) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param validation_profile: ColumnValidationProfile to persist.
        :return: None.
        """

        self.driver_wrapper.set_validation_profile(
            table,
            column,
            validation_profile,
        )

    def get_formatting_options(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read the options for formatting a value.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_formatting_options("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Mapping of formatting option names to values result.
        """

        return self.driver_wrapper.get_formatting_options(table, column)

    def set_formatting_options(
        self: "DatabaseAPI",
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Delegate storage of the options for formatting a value.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_formatting_options("works", "work_title", {"template": "{value}"}) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param formatting_options: Mapping of formatting option names to values to persist.
        :return: None.
        """

        self.driver_wrapper.set_formatting_options(
            table,
            column,
            formatting_options,
        )

    def get_display_options(
        self: "DatabaseAPI",
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read the presentation options for the column.

        Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_display_options("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Mapping of presentation option names to values result.
        """

        return self.driver_wrapper.get_display_options(table, column)

    def set_display_options(
        self: "DatabaseAPI",
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Delegate storage of the presentation options for the column.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_display_options("works", "work_title", {"label": "Title", "visible": True}) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param display_options: Mapping of presentation option names to values to persist.
        :return: None.
        """

        self.driver_wrapper.set_display_options(
            table,
            column,
            display_options,
        )

    def set_case_sensitivity(
        self: "DatabaseAPI",
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Delegate storage of the case-sensitive comparison flag.

        The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_case_sensitivity("works", "work_title", True) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param case_sensitive: Whether comparisons should be case-sensitive.
        :return: None.
        """
        self.driver_wrapper.set_case_sensitivity(table, column, case_sensitive)

    def is_column_case_sensitive(self: "DatabaseAPI", table: str, column: str) -> bool:
        """
        Read the case-sensitivity flag through the facade alias.

        Example:
            For an open db, db.is_column_case_sensitive("works", "work_title") uses the same read path as db.get_case_sensitivity(...).


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: Result of get_case_sensitivity for the same table and column.
        """

        return self.get_case_sensitivity(table, column)

    def set_column_case_sensitive(
        self: "DatabaseAPI",
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Set the case-sensitivity flag through the facade alias.

        Example:
            For an open writable db, db.set_column_case_sensitive("works", "work_title", True) delegates to set_case_sensitivity.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param case_sensitive: Whether comparisons should be case-sensitive.
        :return: None.
        """

        self.set_case_sensitivity(table, column, case_sensitive)

    def get_view_column_headings(self: "DatabaseAPI", view: str) -> list[str]:
        """
        Delegate ordered column discovery for a view.

        Example:
            For an open db with the compatibility view, db.get_view_column_headings("titles") lists its projected fields.


        :param view: Target view name.
        :return: Wrapper column-name list.
        """
        return self.driver_wrapper.get_view_column_headings(view)

    def get_tables_and_columns(self: "DatabaseAPI") -> dict[str, list[str]]:
        """
        Return the wrapper mapping of tables to column collections.

        Iterate items() to receive table/column pairs rather than table-name keys.

        Example:
            For an open db, columns = db.get_tables_and_columns()["works"] retrieves its known work columns.


        :return: Table-to-columns mapping; concrete backend collections may differ from the list annotation.
        """
        return self.driver_wrapper.get_tables_and_columns()

    def get_record_count(self: "DatabaseAPI", target_table: str) -> int:
        """
        Delegate the current row count of a table.

        Example:
            For an open db, db.get_record_count("works") counts its current work records.


        :param target_table: Target table name.
        :return: Wrapper integer row count.
        """
        return self.driver_wrapper.get_record_count(target_table)

    def get_max(self: "DatabaseAPI", column: str) -> int:
        """
        Delegate a column maximum directly to the backend driver.

        Example:
            For an open db, db.get_max("work_id") queries the driver maximum rather than inspecting loaded Rows.


        :param column: Column identifier understood by the driver.
        :return: Driver maximum result; an empty column may produce None despite the annotation.
        """
        return self.driver.direct_get_max(column)

    def get_min(self: "DatabaseAPI", column: str) -> int:
        """
        Delegate a column minimum directly to the backend driver.

        Example:
            For an open db, db.get_min("work_id") queries its driver minimum.


        :param column: Column identifier understood by the driver.
        :return: Driver minimum result; an empty column may produce None despite the annotation.
        """
        return self.driver.direct_get_min(column)

    def row_counts(self: "DatabaseAPI") -> dict[str, int]:
        """
        Format record counts for each cached main, interlink, self-link and helper table.

        Sort table names within each category and query them individually. This is not an atomic snapshot; absent cached categories or missing tables can fail. The result is returned, not printed.

        Example:
            For a fully initialized db, report = db.row_counts() captures the categorized table counts as text.


        :return: Multiline string, despite the legacy dictionary annotation.
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
