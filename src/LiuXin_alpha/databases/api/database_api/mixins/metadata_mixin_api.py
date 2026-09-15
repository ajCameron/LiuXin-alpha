"""
Declare DatabaseMetadataMixinAPI operations for database facade implementations.

Repeated declarations are retained; the later definition supplies the runtime member. Abstract bodies perform no backend work. Concrete behavior and its limitations are described for callers without changing that implementation.
"""

from __future__ import annotations

import abc
from typing import Iterable, Iterator, Mapping, Any

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
from LiuXin_alpha.databases.macro_types import (
    CanonicalIdentity,
    NormalizedIdentityMigrationReport,
)
from LiuXin_alpha.databases.schema_specs import LinkCapabilities


class DatabaseMetadataMixinAPI(abc.ABC):
    """
    Specify schema metadata, cached identifiers and column/identity policies.

    Implement every abstract member before instantiating this interface. Backend resource and transaction policies remain the concrete implementation responsibility. Unlike the abstract hooks, the two case-sensitivity aliases have concrete forwarding bodies. Later UUID/library/version properties replace their earlier declarations.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseMetadataMixinAPI)
        True
    """

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """
        Return the cached database UUID or load it once from the wrapper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A None cache triggers another load. The _uuid attribute must already exist; this getter does not generate a missing UUID itself.

        Example:
            For an initialized concrete db, db.uuid reads its cached value or follows the documented backend lookup path.


        :return: Cached wrapper UUID value.
        """

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        """
        Assign the UUID cache before asking the wrapper to persist it.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: If persistence fails, the assigned cache is not restored.

        Example:
            For an open db, db.uuid = replacement_uuid updates the local cache and delegates storage.


        :param value: UUID value passed unchanged to the wrapper.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """
        Read a cached library identifier, generating one when the query returns None.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The nonempty query result is retained directly without extracting its first column; its shape therefore depends on wrapper.get(all=False). A missing value invokes the setter and writes through macros. Reading this property is not always read-only.

        Example:
            For an initialized concrete db, db.library_id reads its cached value or follows the documented backend lookup path.


        :return: Cached wrapper query result, or generated UUID text.
        """

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        """
        Cache the textual library identifier and delegate storage of the original value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The cache changes before storage and is not restored on failure.

        Example:
            For an open db, db.library_id = replacement_uuid caches its text and calls the library-ID macro.


        :param value: Identifier converted to text for the cache but passed unchanged to macros.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """
        Return the cached version or read the last version row from the primary connection.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A None result is queried again on the next access. This method does not create a version row or explicitly close its cursor.

        Example:
            For an initialized concrete db, db.database_version reads its cached value or follows the documented backend lookup path.


        :return: Last queried version value, or None when there are no rows.
        """

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        """
        Cache version text before passing the original value to its storage macro.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The cached value remains changed if the macro rejects a protected version table or otherwise fails.

        Example:
            When the backend permits version changes, db.database_version = version delegates persistence after updating the cache.


        :param value: Version converted to text for the cache and passed unchanged to macros.
        :return: None.
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Delegate table discovery and optional schema-cache refresh to the wrapper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate table discovery and optional schema-cache refresh to the wrapper.

        Example:
            For an open db, db.get_tables(force_refresh=True) requests refreshed schema names.


        :param force_refresh: Request fresh backend discovery when True.
        :return: Wrapper table-name list.
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Delegate ordered column discovery for a table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate ordered column discovery for a table.

        Example:
            For an open db, db.get_column_headings("works") identifies available work fields.


        :param table: Target table name.
        :return: Wrapper column-name list in backend order.
        """

    @abc.abstractmethod
    def get_declared_column_datatype(self, table: str, column: str) -> str:
        """
        Read the backend-declared type name for one column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read the backend-declared type name for one column.

        Example:
            For the current schema, db.get_declared_column_datatype("database_metadata", "database_metadata_unique_id") returns "TEXT".


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: Datatype string returned by the wrapper.
        """

    @abc.abstractmethod
    def get_link_capabilities(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> LinkCapabilities | None:
        """
        Retrieve the schema capabilities of an interlink or self-link.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Retrieve the schema capabilities of an interlink or self-link.

        Example:
            For an open db, capabilities = db.get_link_capabilities("agents", "works") describes its type/priority columns.


        :param table1: First endpoint table.
        :param table2: Second endpoint table; may match the first for a self-link.
        :param force_refresh: Request refreshed schema capability discovery.
        :return: LinkCapabilities, or None when no supported relationship is found.
        """

    @abc.abstractmethod
    def is_link_typed(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Ask the wrapper whether a relationship supports a type column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Ask the wrapper whether a relationship supports a type column.

        Example:
            For an open db, db.is_link_typed("agents", "works") tests schema capability rather than a particular stored link type.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Refresh capability discovery when True.
        :return: Wrapper boolean capability result.
        """

    @abc.abstractmethod
    def is_link_priority(
        self,
        table1: str,
        table2: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        """
        Ask the wrapper whether a relationship supports priority ordering.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Ask the wrapper whether a relationship supports priority ordering.

        Example:
            For an open db, db.is_link_priority("agents", "works") checks whether its relationship schema supports priority.


        :param table1: First endpoint table.
        :param table2: Second endpoint table.
        :param force_refresh: Refresh capability discovery when True.
        :return: Wrapper boolean capability result.
        """

    @abc.abstractmethod
    def get_case_sensitivity(self, table: str, column: str) -> bool:
        """
        Read the case-sensitive comparison flag.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_case_sensitivity("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper bool flag result.
        """

    @abc.abstractmethod
    def get_column_metadata(self, table: str, column: str) -> ColumnMetadata:
        """
        Retrieve the effective policy record for a database column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Retrieve the effective policy record for a database column.

        Example:
            For an open db, policy = db.get_column_metadata("works", "work_title") combines normalization, merge and presentation settings.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: ColumnMetadata returned by the wrapper, including driver-supported legacy read fallbacks.
        """

    @abc.abstractmethod
    def set_column_metadata(self, metadata: ColumnMetadata) -> None:
        """
        Delegate persistence of a complete column-policy record.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: No validation or schema migration is added at this facade boundary; driver errors propagate. Legacy read fallbacks do not imply an old schema supports policy writes.

        Example:
            After deriving an updated policy with dataclasses.replace, db.set_column_metadata(policy) delegates its persistence.


        :param metadata: ColumnMetadata identifying its table/column and replacement policy.
        :return: None.
        """

    @abc.abstractmethod
    def get_semantic_role(self, table: str, column: str) -> ColumnSemanticRole:
        """
        Read the semantic role of the stored value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_semantic_role("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnSemanticRole describing the value meaning result.
        """

    @abc.abstractmethod
    def set_semantic_role(
        self,
        table: str,
        column: str,
        semantic_role: ColumnSemanticRole,
    ) -> None:
        """
        Delegate storage of the semantic role of the stored value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_semantic_role("works", "work_title", ColumnSemanticRole.LABEL) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param semantic_role: ColumnSemanticRole describing the value meaning to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_normalization_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnNormalizationProfile:
        """
        Read the normalization profile for comparable values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_normalization_profile("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnNormalizationProfile result.
        """

    @abc.abstractmethod
    def set_normalization_profile(
        self,
        table: str,
        column: str,
        normalization_profile: ColumnNormalizationProfile,
    ) -> None:
        """
        Delegate storage of the normalization profile for comparable values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_normalization_profile("works", "work_title", ColumnNormalizationProfile.UNICODE_NFC) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param normalization_profile: ColumnNormalizationProfile to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_comparison_column(self, table: str, column: str) -> str | None:
        """
        Read the companion column used for comparisons.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_comparison_column("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Companion column name, or None result.
        """

    @abc.abstractmethod
    def get_normalized_identity_spec(
        self,
        table: str,
        value_column: str,
    ) -> NormalizedIdentitySpec | None:
        """
        Read the normalized-identity declaration for a display-value column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read the normalized-identity declaration for a display-value column.

        Example:
            For the current schema, db.get_normalized_identity_spec("tags", "tag") returns the declaration used to identify canonical tag rows.


        :param table: Table containing the value column.
        :param value_column: Display-value column whose identity is declared.
        :return: NormalizedIdentitySpec, or None when no declaration applies.
        """

    @abc.abstractmethod
    def iter_normalized_identity_specs(self) -> Iterator[NormalizedIdentitySpec]:
        """
        Yield normalized-identity declarations from the wrapper iterator.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Iteration is lazy: wrapper iteration and its errors occur when the returned generator is consumed.

        Example:
            For an open db, specs = tuple(db.iter_normalized_identity_specs()) consumes all current declarations.


        :return: Iterator yielding NormalizedIdentitySpec objects.
        """

    @abc.abstractmethod
    def derive_identity_value(
        self,
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """
        Normalize a display value using its declared identity profile.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The macros layer validates identifiers and requires an applicable normalized-identity declaration.

        Example:
            For an open db, key = db.derive_identity_value("tags", "tag", " Example ") derives the key used for canonical lookup.


        :param table: Table containing the display value.
        :param value_column: Declared display-value column.
        :param value: Value to normalize.
        :return: Derived identity key returned by macros, without creating a row.
        """

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
        """
        Normalize a display value and look up its unique canonical identity.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegates normalization and scoped lookup to macros; multiple matches raise DatabaseIntegrityError. This does not insert missing values.

        Example:
            For an open db, identity = db.get_canonical_identity("tags", "tag", " EXAMPLE ") finds the stored spelling and row ID for an equivalent value.


        :param table: Table containing the canonical row.
        :param value_column: Declared display-value column.
        :param value: Display value to normalize before lookup.
        :param scope_values: Optional values for the declaration scope columns.
        :param id_column: Optional row-ID column override.
        :return: CanonicalIdentity for a unique match, or None when absent.
        """

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
        """
        Resolve an already-derived identity key without normalizing it again.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Macros enforce scope and ID-column validity and reject ambiguous matches; the facade forwards all arguments unchanged.

        Example:
            After key = db.derive_identity_value("tags", "tag", text), db.get_canonical_identity_by_key("tags", "tag", key) performs the canonical lookup.


        :param table: Table containing the canonical row.
        :param value_column: Declared display-value column.
        :param identity_value: Non-None normalized key used directly for lookup.
        :param scope_values: Optional values for the declaration scope columns.
        :param id_column: Optional row-ID column override.
        :return: CanonicalIdentity for a unique match, or None when absent.
        """

    @abc.abstractmethod
    def get_canonical_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Return the stored spelling matching a normalized display-value lookup.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Return the stored spelling matching a normalized display-value lookup.

        Example:
            For an existing canonical tag, db.get_canonical_value("tags", "tag", alternate_spelling) returns its stored spelling.


        :param table: Table containing the canonical value.
        :param value_column: Declared display-value column.
        :param value: Display value normalized by macros.
        :param scope_values: Optional values for declared scope columns.
        :return: Canonical stored value, or None when no row matches.
        """

    @abc.abstractmethod
    def get_canonical_value_by_identity(
        self,
        table: str,
        value_column: str,
        identity_value: Any,
        *,
        scope_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Return the stored spelling matching an already-derived identity key.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Return the stored spelling matching an already-derived identity key.

        Example:
            For an open db and derived tag key, db.get_canonical_value_by_identity("tags", "tag", key) reads the canonical spelling without re-normalizing the key.


        :param table: Table containing the canonical value.
        :param value_column: Declared display-value column.
        :param identity_value: Normalized key passed directly to macros.
        :param scope_values: Optional values for declared scope columns.
        :return: Canonical stored value, or None when no row matches.
        """

    @abc.abstractmethod
    def audit_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Delegate inspection of stale identity keys and collisions without migration writes.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate inspection of stale identity keys and collisions without migration writes.

        Example:
            For an open db, report = db.audit_normalized_identities() reports collisions before a planned migration.


        :return: NormalizedIdentityMigrationReport describing examined declarations and rows.
        """

    @abc.abstractmethod
    def migrate_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Delegate transactional identity-catalog installation, backfill and indexing.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The macros layer inspects for collisions before applying changes and raises DatabaseIntegrityError when collisions prevent migration. The facade adds no outer transaction or cache management.

        Example:
            After reviewing an identity audit, report = db.migrate_normalized_identities() applies the macros migration to a writable database.


        :return: NormalizedIdentityMigrationReport describing completed changes.
        """

    @abc.abstractmethod
    def set_comparison_column(
        self,
        table: str,
        column: str,
        comparison_column: str | None,
    ) -> None:
        """
        Delegate storage of the companion column used for comparisons.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_comparison_column("works", "work_title", "work_sort_title") requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param comparison_column: Companion column name, or None to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_empty_value_policy(
        self,
        table: str,
        column: str,
    ) -> ColumnEmptyValuePolicy:
        """
        Read the policy for empty input values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_empty_value_policy("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnEmptyValuePolicy result.
        """

    @abc.abstractmethod
    def set_empty_value_policy(
        self,
        table: str,
        column: str,
        empty_value_policy: ColumnEmptyValuePolicy,
    ) -> None:
        """
        Delegate storage of the policy for empty input values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_empty_value_policy("works", "work_title", ColumnEmptyValuePolicy.PRESERVE) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param empty_value_policy: ColumnEmptyValuePolicy to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_merge_policy(self, table: str, column: str) -> ColumnMergePolicy:
        """
        Read the policy for combining an incoming value with existing data.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_merge_policy("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnMergePolicy result.
        """

    @abc.abstractmethod
    def set_merge_policy(
        self,
        table: str,
        column: str,
        merge_policy: ColumnMergePolicy,
    ) -> None:
        """
        Delegate storage of the policy for combining an incoming value with existing data.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_merge_policy("works", "work_title", ColumnMergePolicy.PRESERVE_EXISTING) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param merge_policy: ColumnMergePolicy to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_validation_profile(
        self,
        table: str,
        column: str,
    ) -> ColumnValidationProfile:
        """
        Read the validation profile for incoming values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_validation_profile("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper ColumnValidationProfile result.
        """

    @abc.abstractmethod
    def set_validation_profile(
        self,
        table: str,
        column: str,
        validation_profile: ColumnValidationProfile,
    ) -> None:
        """
        Delegate storage of the validation profile for incoming values.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_validation_profile("works", "work_title", ColumnValidationProfile.VERBATIM_TEXT) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param validation_profile: ColumnValidationProfile to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_formatting_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read the options for formatting a value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_formatting_options("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Mapping of formatting option names to values result.
        """

    @abc.abstractmethod
    def set_formatting_options(
        self,
        table: str,
        column: str,
        formatting_options: Mapping[str, object],
    ) -> None:
        """
        Delegate storage of the options for formatting a value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_formatting_options("works", "work_title", {"template": "{value}"}) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param formatting_options: Mapping of formatting option names to values to persist.
        :return: None.
        """

    @abc.abstractmethod
    def get_display_options(
        self,
        table: str,
        column: str,
    ) -> ColumnOptions:
        """
        Read the presentation options for the column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read behavior and legacy defaults belong to the driver; this facade forwards the table and column unchanged.

        Example:
            For an open db, db.get_display_options("works", "work_title") reads that column setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :return: The wrapper Mapping of presentation option names to values result.
        """

    @abc.abstractmethod
    def set_display_options(
        self,
        table: str,
        column: str,
        display_options: Mapping[str, object],
    ) -> None:
        """
        Delegate storage of the presentation options for the column.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_display_options("works", "work_title", {"label": "Title", "visible": True}) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param display_options: Mapping of presentation option names to values to persist.
        :return: None.
        """

    @abc.abstractmethod
    def set_case_sensitivity(
        self,
        table: str,
        column: str,
        case_sensitive: bool,
    ) -> None:
        """
        Delegate storage of the case-sensitive comparison flag.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The facade does not validate, normalize or migrate the supplied policy; wrapper/driver errors propagate.

        Example:
            For an open writable db with a current policy schema, db.set_case_sensitivity("works", "work_title", True) requests the new setting.


        :param table: Table containing the column.
        :param column: Column name within that table.
        :param case_sensitive: Whether comparisons should be case-sensitive.
        :return: None.
        """

    def is_column_case_sensitive(self, table: str, column: str) -> bool:
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
        self,
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

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Delegate ordered column discovery for a view.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate ordered column discovery for a view.

        Example:
            For an open db with the compatibility view, db.get_view_column_headings("titles") lists its projected fields.


        :param view: Target view name.
        :return: Wrapper column-name list.
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Return the wrapper mapping of tables to column collections.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Iterate items() to receive table/column pairs rather than table-name keys.

        Example:
            For an open db, columns = db.get_tables_and_columns()["works"] retrieves its known work columns.


        :return: Table-to-columns mapping; concrete backend collections may differ from the list annotation.
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Delegate the current row count of a table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate the current row count of a table.

        Example:
            For an open db, db.get_record_count("works") counts its current work records.


        :param target_table: Target table name.
        :return: Wrapper integer row count.
        """

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """
        Delegate a column maximum directly to the backend driver.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate a column maximum directly to the backend driver.

        Example:
            For an open db, db.get_max("work_id") queries the driver maximum rather than inspecting loaded Rows.


        :param column: Column identifier understood by the driver.
        :return: Backend extremum value, or its empty-result value such as None.
        """

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """
        Delegate a column minimum directly to the backend driver.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Delegate a column minimum directly to the backend driver.

        Example:
            For an open db, db.get_min("work_id") queries its driver minimum.


        :param column: Column identifier understood by the driver.
        :return: Backend extremum value, or its empty-result value such as None.
        """

    @abc.abstractmethod
    def row_counts(self) -> str:
        """
        Format record counts for each cached main, interlink, self-link and helper table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Sort table names within each category and query them individually. This is not an atomic snapshot; absent cached categories or missing tables can fail. The result is returned, not printed.

        Example:
            For a fully initialized db, report = db.row_counts() captures the categorized table counts as text.


        :return: Multiline string summarizing table counts.
        """

    # ---------------------------------------------------------------------------------------------
    # Database metadata (uuid/library_id/version)
    # ---------------------------------------------------------------------------------------------
    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """
        Return the cached database UUID or load it once from the wrapper.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A None cache triggers another load. The _uuid attribute must already exist; this getter does not generate a missing UUID itself.

        Example:
            For an initialized concrete db, db.uuid reads its cached value or follows the documented backend lookup path.


        :return: Cached wrapper UUID value.
        """

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        """
        Assign the UUID cache before asking the wrapper to persist it.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: If persistence fails, the assigned cache is not restored.

        Example:
            For an open db, db.uuid = replacement_uuid updates the local cache and delegates storage.


        :param value: UUID value passed unchanged to the wrapper.
        :return: None.
        """

        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """
        Read a cached library identifier, generating one when the query returns None.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The nonempty query result is retained directly without extracting its first column; its shape therefore depends on wrapper.get(all=False). A missing value invokes the setter and writes through macros. Reading this property is not always read-only.

        Example:
            For an initialized concrete db, db.library_id reads its cached value or follows the documented backend lookup path.


        :return: Cached wrapper query result, or generated UUID text.
        """

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        """
        Cache the textual library identifier and delegate storage of the original value.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The cache changes before storage and is not restored on failure.

        Example:
            For an open db, db.library_id = replacement_uuid caches its text and calls the library-ID macro.


        :param value: Identifier converted to text for the cache but passed unchanged to macros.
        :return: None.
        """

        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """
        Return the cached version or read the last version row from the primary connection.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A None result is queried again on the next access. This method does not create a version row or explicitly close its cursor.

        Example:
            For an initialized concrete db, db.database_version reads its cached value or follows the documented backend lookup path.


        :return: Last queried version value, or None when there are no rows.
        """

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        """
        Cache version text before passing the original value to its storage macro.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: The cached value remains changed if the macro rejects a protected version table or otherwise fails.

        Example:
            When the backend permits version changes, db.database_version = version delegates persistence after updating the cache.


        :param value: Version converted to text for the cache and passed unchanged to macros.
        :return: None.
        """

        ...
