"""
Define portable relational operations for rows, links and normalized identities.

PortableMacrosAPI is an abstract contract. The SQLPortableMacrosMixin supplies the
shared SQLite/PostgreSQL implementation described here. Callers obtain concrete macros
from their database driver; these abstract method bodies do not execute SQL.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from LiuXin_alpha.databases.db_types.macro_types import (
    LINK_TYPE_UNSET,
    CanonicalIdentity,
    LinkedTableRow,
    LinkRow,
    LinkValue,
    NormalizedIdentityMigrationReport,
    UnreferencedRowsSpec,
)
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


class PortableMacrosAPI(abc.ABC):
    """
    Specify schema-aware row, relationship and identity operations for SQL backends.

    Values are bound separately from validated identifiers. Reads use the active
    portable transaction connection when present; otherwise they use the driver
    connection. Ordinary compound writes open or join transaction(). Temporary-table
    contexts use the active connection and an owned transaction or borrowed savepoint
    covering their body and cleanup. Backend constraint and connection errors propagate
    unless a method describes a narrower translation.

    Example:
        Given an open database db, use db.driver.macros.get_rows("tags",
        order_by=("tag_id",)) to retrieve tag rows in ID order.
    """

    @abc.abstractmethod
    def transaction(self, *, serialized: bool = False, require_outermost: bool = False) -> AbstractContextManager[Any]:
        """
        Open or join the transaction shared by nested portable macro calls.

        The outer context uses a dedicated connection when available, otherwise the
        persistent connection. It starts and owns a transaction only on a fresh or idle
        connection; an already active persistent transaction is borrowed through a
        savepoint and remains pending. Each nested call uses a savepoint on the shared
        connection, so a caught inner failure undoes only that unit. Only an owned
        transaction commits on success or rolls back wholesale on failure. Owned
        connections close separately; successful outer exits invalidate driver caches.
        The facade mutex serializes cooperating threads without committing SQL.

        Example:
            With concrete macros, place insert_row("tags", {"tag": "Travel"}) and a
            following update_row call inside one with macros.transaction(): block to
            commit them together.


        :param serialized: Reserve SQLite's writer boundary or a shared PostgreSQL
            advisory transaction lock before reading. Requires an owned outer unit,
            or nesting inside an already serialized unit. Ordinary and borrowed
            transactions cannot be upgraded. External bytes and privileged direct
            SQL writers are outside the advisory locking contract.
        :param require_outermost: Refuse nesting/borrowing so successful exit commits
            before a caller performs irreversible external effects.
        :return: Context manager yielding the connection used by portable reads and
            writes in this thread.
        :raises DatabaseIntegrityError: The driver cannot provide a transaction
            connection.
        """

    @abc.abstractmethod
    def get_row(
        self,
        table: str,
        row_id: Any,
        *,
        id_column: str | None = None,
    ) -> Mapping[str, Any] | None:
        """
        Read every column of the row matching one ID on the current connection.

        Example:
            For concrete macros, macros.get_row("tags", tag_id) returns the stored tag
            fields or None after that row has been deleted.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param row_id: ID matched by SQL equality.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: Dictionary of column names to stored values, or None when no row
            matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: The selected ID column matches more than one
            row.
        """

    @abc.abstractmethod
    def get_rows(
        self,
        table: str,
        *,
        where: Mapping[str, Any] | None = None,
        order_by: Iterable[str] = (),
    ) -> tuple[Mapping[str, Any], ...]:
        """
        Read complete rows matching optional equality filters and requested ordering.

        Supply an ordering that breaks ties when stable row order matters. The
        implementation does not promise an implicit ID ordering.

        Example:
            With concrete macros, macros.get_rows("tags", where={"tag": "Travel"},
            order_by=("tag_id",)) selects exact stored display values in ID order.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param where: Equality predicates combined with AND; None values become IS NULL,
            and an empty mapping selects all rows.
        :param order_by: Columns to sort ascending; the default empty iterable adds no
            ORDER BY.
        :return: Tuple of newly materialized row dictionaries; an empty tuple means no
            matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def get_rows_page(
        self,
        table: str,
        *,
        where: Mapping[str, Any] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[tuple[Mapping[str, Any], ...], bool]:
        """Read an ID-ordered physical-table page and whether it reaches the end.

        Use exact equality/NULL filters, strict nonnegative integer pagination
        and at most limit + 1 materialized rows. A zero-length page is complete
        only at/beyond the end. Reuse the current connection without committing.
        """

    @abc.abstractmethod
    def get_column_extrema(self, table: str, column: str) -> tuple[Any, Any]:
        """Read a column's minimum and maximum using the active transaction connection.

        SQL ignores nulls and uses the column's native ordering. Empty or all-null
        columns return (None, None); other values are returned without coercion.

        Example:
            macros.get_column_extrema("works", "work_id") returns the ID bounds.

        :param table: Existing table with a validated SQL identifier.
        :param column: Existing physical column with a validated SQL identifier.
        :return: Minimum and maximum values, in that order.
        :raises InputIntegrityError: An identifier or column fails schema validation.
        """

    @abc.abstractmethod
    def insert_row(
        self,
        table: str,
        values: Mapping[str, Any],
        *,
        id_column: str | None = None,
    ) -> Any:
        """
        Insert a nonempty column mapping in a portable transaction.

        Omitted columns use database defaults. For the portable return contract, use the
        table’s actual generated ID column; an alternate SQLite id_column does not
        change lastrowid into that column’s value.

        Example:
            With concrete macros, tag_id = macros.insert_row("tags", {"tag": "Travel"})
            inserts the supplied fields and returns its generated row ID.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param values: Nonempty mapping of canonical column names to bound values.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: Inserted ID: PostgreSQL returns the requested ID column; SQLite uses
            cursor.lastrowid.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: PostgreSQL INSERT RETURNING yields no row.
        """

    @abc.abstractmethod
    def update_row(
        self,
        table: str,
        row_id: Any,
        values: Mapping[str, Any],
        *,
        id_column: str | None = None,
    ) -> None:
        """
        Update supplied fields for rows matching the selected ID column.

        The ID column cannot appear in values. Use a unique ID column: the SQL UPDATE
        itself does not enforce that only one row matches. Omitted fields remain
        unchanged.

        Example:
            With concrete macros, macros.update_row("tags", tag_id, {"tag": "Journeys"})
            replaces the display field on the matching row.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param row_id: ID matched by SQL equality.
        :param values: Fields to overwrite; an empty mapping returns before validation
            or transaction setup.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: None; no affected-row count or missing-row indication is returned.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def delete_link_rows(
        self,
        link_table: str,
        link_col: str,
        remove_id: Any,
        link_type: str | None = None,
    ) -> None:
        """Delete links by endpoint, validating schema and the complete ID batch.

        Accept an integer, iterable of integers, or one-cell integer bindings.
        Optional type values are bound. The entire batch shares the outer
        portable transaction; no bibliographic policy is applied.
        """

    @abc.abstractmethod
    def delete_link_pair(
        self,
        link_table: str,
        left_link_col: str,
        right_link_col: str,
        left_id: Any,
        right_id: Any,
    ) -> None:
        """Delete all link types for validated physical columns and bound IDs."""

    @abc.abstractmethod
    def delete_row(
        self,
        table: str,
        row_id: Any,
        *,
        id_column: str | None = None,
    ) -> None:
        """
        Delete rows matching the selected ID in a portable transaction.

        Use a unique ID column; no pre-read verifies uniqueness or existence. Database
        foreign-key and trigger behavior determines related effects.

        Example:
            With concrete macros, macros.delete_row("tags", tag_id) removes the matching
            database row subject to schema constraints.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param row_id: ID matched by SQL equality.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: None, including when no row matched.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def get_link_rows(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """
        Read a source row’s links with complete declared relationship properties.

        A type filter requires a typed specification. Selected rows include declared
        extra columns as well as endpoint, type and priority fields.

        Example:
            Given macros and a valid author-link spec, macros.get_link_rows(spec,
            title_id, link_type="authors") returns that title’s author links in priority
            order.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param primary_id: Source row ID in the spec’s primary table.
        :param link_type: LINK_TYPE_UNSET selects all types; None selects SQL NULL;
            another value selects that exact type.
        :return: Tuple of LinkRow values, sorted by descending priority when present and
            then ascending destination ID.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def get_link_rows_bulk(
        self,
        link_spec: StorageLinkSpec,
        primary_ids: Iterable[Any] | None = None,
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Read and group link rows for requested owners or every linked owner.

        With primary_ids=None, only owners represented in the selected links appear. An
        empty iterable returns an empty mapping. The implementation binds requested IDs
        in one query rather than splitting large selections into chunks.

        Example:
            Given macros and a valid spec, macros.get_link_rows_bulk(spec, (1, 2))
            includes keys 1 and 2 even when either has no links.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param primary_ids: Hashable source IDs, deduplicated in encounter order; None
            reads all links.
        :param link_type: LINK_TYPE_UNSET selects all types; None selects SQL NULL;
            another value selects that exact type.
        :return: Dictionary from owner ID to sorted LinkRow tuples; requested owners
            with no matches have empty tuples.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def upsert_link(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        link: LinkValue,
    ) -> LinkRow:
        """
        Create or update one logical link while preserving omitted extra fields.

        Identity uses both endpoints and also type when the spec declares it part of
        identity. Otherwise a supplied type, including None, replaces the existing type.
        None priority preserves an existing priority and leaves an insert to its schema
        default. Declared writable extras are updated only when present; explicit None
        is a supplied value. Typed registries and finite numeric priorities are
        validated before writing.

        Example:
            Given concrete macros and a compatible spec, macros.upsert_link(spec, 1,
            LinkValue(10, priority=2, extra={"note": "primary"})) updates the link
            without clearing omitted extras.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param primary_id: Source row ID in the spec’s primary table.
        :param link: LinkValue containing destination ID and optional type, priority and
            extra properties.
        :return: LinkRow read back for the resolved logical identity.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """

    @abc.abstractmethod
    def upsert_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
    ) -> tuple[LinkRow, ...]:
        """
        Upsert a materialized batch of distinct links for one owner atomically.

        Validate the complete batch before writing and reject duplicate identities. Type
        registry reads are shared within the batch. Per-link rules are those of
        upsert_link, including preservation of omitted extras and existing priorities.

        Example:
            Given concrete macros and a compatible spec, macros.upsert_links(spec, 1,
            (LinkValue(10, priority=2), LinkValue(11, priority=1))) returns the two rows
            in that order.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param primary_id: Source row ID in the spec’s primary table.
        :param links: Iterable of LinkValue entries with distinct logical identities.
        :return: Tuple of LinkRow values in input order, rather than sorted by priority.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """

    @abc.abstractmethod
    def replace_links(
        self,
        link_spec: StorageLinkSpec,
        primary_id: Any,
        links: Iterable[LinkValue],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> tuple[LinkRow, ...]:
        """
        Synchronize one owner’s desired links, deleting absent relationships.

        For ordered links, missing priorities are assigned from input position, highest
        first. Duplicate priorities within an identity-type group are rejected. Existing
        priorities are temporarily moved to avoid uniqueness conflicts during
        reordering. Omitted extras survive on retained links. A scoped replacement
        leaves other types alone; entries with no type inherit the scope, while
        conflicting types are rejected. Deleting a link does not itself delete its
        destination row.

        Example:
            Given concrete macros and a compatible spec, macros.replace_links(spec, 1,
            (LinkValue(11), LinkValue(10))) assigns priorities 2 and 1 when the spec is
            ordered.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param primary_id: Source row ID in the spec’s primary table.
        :param links: Desired distinct LinkValue entries; an empty iterable clears the
            selected link set.
        :param link_type: LINK_TYPE_UNSET replaces all types; an explicit scope requires
            type to be part of logical identity.
        :return: Current selected LinkRow tuple ordered by descending priority when
            present, then destination ID.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """

    @abc.abstractmethod
    def replace_links_bulk(
        self,
        link_spec: StorageLinkSpec,
        replacements: Mapping[Any, Iterable[LinkValue]],
        *,
        link_type: Any = LINK_TYPE_UNSET,
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Synchronize several owner link sets within one portable transaction.

        Each owner follows replace_links rules. An escaping validation or SQL error
        rolls back the outer transaction; existing nested transaction semantics still
        apply when a caller catches an error inside a surrounding transaction.

        Example:
            Given concrete macros and a compatible spec, macros.replace_links_bulk(spec,
            {1: (LinkValue(10),), 2: ()}) sets one owner’s links and clears the other’s
            in one transaction.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param replacements: Mapping of owner IDs to desired LinkValue iterables,
            materialized before writing.
        :param link_type: Optional type scope with the same restrictions as
            replace_links.
        :return: Dictionary of requested owner IDs to their resulting sorted LinkRow
            tuples.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """

    @abc.abstractmethod
    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[Any, Any | None],
    ) -> dict[Any, tuple[LinkRow, ...]]:
        """
        Update, create or unlink destination values owned through one-to-one links.

        Require ONE_TO_ONE cardinality. Update an existing destination in place;
        otherwise insert a row containing the value and link its returned ID. Multiple
        current links for one source are an integrity error. None removes only the link
        and leaves the destination row. Destination defaults and constraints must permit
        a value-only insert. An empty mapping performs no writes.

        Example:
            Given an appropriate one-to-one spec,
            macros.replace_owned_one_to_one_values_bulk(spec, "comment_text", {1:
            "Revised", 2: None}) updates or creates one value and unlinks the other.


        :param link_spec: Schema-backed StorageLinkSpec describing endpoints, columns,
            type and ordering rules.
        :param value_column: Existing destination value column, distinct from its ID
            column.
        :param replacements: Mapping of source IDs to desired values; None removes that
            source’s link.
        :return: Dictionary of source IDs to their remaining LinkRow tuples, empty after
            unlinking.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """

    @abc.abstractmethod
    def find_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any | None:
        """
        Find an existing logical value using database-owned comparison policy.

        Match a configured comparison column, or normalize display values in Python when
        the policy requires it. Otherwise use the backend’s case-sensitive or
        insensitive comparison. Only declared identity-scope fields from
        additional_values constrain lookup; other fields are not extra equality
        predicates. No row is inserted or updated.

        Example:
            With concrete macros, macros.find_table_value("tags", "tag", "Science
            Fiction") can find the ID stored under the tag’s normalized identity.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Display-value column whose metadata controls matching.
        :param value: Value subject to that column’s empty-value and normalization
            policies.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :param additional_values: Additional columns validated against the schema;
            declared identity scope columns must be present.
        :return: Matching row ID, or None when no logical value matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Comparison policy matches more than one row.
        """

    @abc.abstractmethod
    def ensure_table_value(
        self,
        table: str,
        value_column: str,
        value: Any,
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Return a logical value’s existing ID or insert its original display value.

        Use the same matching policy as find_table_value. If a match exists, preserve
        its display spelling and other fields. Otherwise insert additional_values plus
        the original value and any configured derived comparison key, then look up the
        resulting ID. Caller-supplied display/comparison entries in additional_values
        are replaced by those derived payload fields. A conflicting constraint that
        prevents a recoverable match raises an integrity error.

        Example:
            With concrete macros, first = macros.ensure_table_value("tags", "tag",
            "Science Fiction") and a later equivalent normalized spelling resolve to the
            same ID.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Display-value column whose metadata governs comparison and
            empty-value handling.
        :param value: Display value to preserve on insertion.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :param additional_values: Insert defaults and required identity-scope fields;
            not an update payload for an existing row.
        :return: ID of the matching existing row or the row recovered after insertion.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup finds duplicate logical identities or
            cannot recover a required inserted row.
        """


    @abc.abstractmethod
    def derive_identity_value(
        self,
        table: str,
        value_column: str,
        value: Any,
    ) -> Any:
        """
        Normalize a display value using its declared identity specification.

        Validate that the declaration’s display, identity and scope columns exist. This
        does not enforce the display column’s empty-value policy or check uniqueness;
        canonical lookup rejects a None key separately.

        Example:
            With the standard tag identity declaration,
            macros.derive_identity_value("tags", "tag", "Science Fiction") returns
            "sciencefiction".


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Display column with an existing normalized identity
            declaration.
        :param value: Display value; non-strings, including None, pass through the
            normalization helper.
        :return: Derived key without querying for a matching row or inserting anything.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
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
        Resolve a display value to the full stored canonical identity.

        Derive the key with derive_identity_value, then apply
        get_canonical_identity_by_key. Do not create rows or replace an existing display
        spelling.

        Example:
            With concrete macros and an existing tag,
            macros.get_canonical_identity("tags", "tag", " sciencefiction ") returns
            that row’s stored spelling and ID.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Declared identity’s display column.
        :param value: Display value normalized before lookup.
        :param scope_values: Mapping containing exactly the declared scope columns,
            including explicit None for a NULL scope; omit for unscoped identities.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: CanonicalIdentity containing the stored row ID, display value, derived
            key and scope, or None.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: More than one stored row matches the key and
            scope.
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
        Resolve an already-derived non-NULL identity key and exact scope.

        Scope keys must exactly match the declaration. None scope values use IS NULL;
        the identity key itself cannot be None. Lookup checks ambiguity even if the
        declaration does not request a unique index.

        Example:
            Given a scoped genre identity,
            macros.get_canonical_identity_by_key("genres", "genre", key,
            scope_values={"genre_parent_id": None}) restricts lookup to root genres.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Declared identity’s display column.
        :param identity_value: Derived key used as supplied, without further
            normalization.
        :param scope_values: Mapping containing exactly the declared scope columns,
            including explicit None for a NULL scope; omit for unscoped identities.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :return: CanonicalIdentity for the unique match, or None when no row matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: The key and scope match multiple rows.
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
        Return only the stored display value for a normalized display lookup.

        Delegate to get_canonical_identity and discard its ID, key and scope fields. A
        nullable stored display value is also returned as None.

        Example:
            With a stored "Science Fiction" tag, macros.get_canonical_value("tags",
            "tag", "sciencefiction") returns its original stored spelling.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Declared identity’s display column.
        :param value: Display value normalized before lookup.
        :param scope_values: Mapping containing exactly the declared scope columns,
            including explicit None for a NULL scope; omit for unscoped identities.
        :return: Canonical stored display value, or None when no identity matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup matches multiple canonical rows.
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
        Return the stored display value for an already-derived identity key.

        Delegate to get_canonical_identity_by_key using the discovered row-ID column.
        The result omits the key, scope and ID; a stored None display value is
        indistinguishable from no match.

        Example:
            With a stored tag, macros.get_canonical_value_by_identity("tags", "tag",
            "sciencefiction") retrieves its display spelling without deriving the
            supplied key again.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param value_column: Declared identity’s display column.
        :param identity_value: Non-NULL derived key used without normalization.
        :param scope_values: Mapping containing exactly the declared scope columns,
            including explicit None for a NULL scope; omit for unscoped identities.
        :return: Canonical stored display value, or None when no identity matches.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises DatabaseIntegrityError: Lookup matches multiple canonical rows.
        """

    @abc.abstractmethod
    def audit_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Inspect existing identity declarations for stale keys and scoped collisions.

        Inspect applicable built-in declarations plus database declarations, which
        override built-ins for the same display column. Skip declarations whose table,
        display or scope columns are absent; missing identity columns count as needing
        backfill for non-NULL derived values. Collisions are reported only for
        declarations requesting uniqueness, grouped by derived non-NULL key and scope.
        Rows are counted per declaration, so a table with two identities is examined
        twice. No schema or data is changed.

        Example:
            With concrete macros, report = macros.audit_normalized_identities() lets a
            caller inspect report.collisions before requesting migration.


        :return: NormalizedIdentityMigrationReport with examined/update-needed counts,
            collisions and zero rows_updated.
        """

    @abc.abstractmethod
    def migrate_normalized_identities(self) -> NormalizedIdentityMigrationReport:
        """
        Install identity metadata, backfill derived keys and request uniqueness indexes.

        Inspect applicable built-in and database declarations, reject derived-key
        collisions before changes, then create the declaration catalog, add missing
        nullable TEXT identity columns and backfill stale keys. Seed compatible
        column-metadata catalogs and request partial unique indexes, including separate
        NULL-scope patterns. Existing index names are included in indexes_created
        because CREATE INDEX IF NOT EXISTS does not report whether an index was newly
        created. Changes share a portable transaction; unrelated tables are not created
        or merged.

        Example:
            After inspecting an audit report without collisions, use
            macros.migrate_normalized_identities() to install missing keys and report
            the backfill count.


        :return: NormalizedIdentityMigrationReport with rows updated, added column names
            and requested index names.
        :raises DatabaseIntegrityError: Normalization reveals duplicate scoped keys for
            a unique identity declaration.
        """

    @abc.abstractmethod
    def temporary_value_table(
        self,
        values: Iterable[Any],
        *,
        column: str = "value",
        declared_type: str = "TEXT",
        prefix: str = "liuxin_values",
    ) -> AbstractContextManager[str]:
        """
        Return a context manager that populates a uniquely named connection-local
        temporary table.

        On entry, validate a simple column/prefix, require a prefix of at most 30
        characters, and accept only BLOB, INTEGER, NUMERIC, REAL or TEXT after
        trimming/uppercasing. Schema-bearing drivers map BLOB to BYTEA. Append a UUID to
        the prefix, create the table and consume values through executemany, preserving
        duplicates and nulls subject to backend conversion. Yield the unqualified name.

        An explicit transaction or borrowed savepoint covers creation, population,
        the body and removal. Success drops the table; failure rolls back this
        boundary, including table creation and body writes on this connection. Existing
        caller work remains pending. Never commit or roll back a caller's whole
        transaction to recover a failed drop. The connection stays open.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> macros = SQLPortableMacrosMixin()
            >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn))
            >>> with macros.temporary_value_table(("a", "a", None)) as table:
            ...     print(
            ...         conn.execute(f'SELECT COUNT(*) FROM temp."{table}"').fetchone()[
            ...             0
            ...         ]
            ...     )
            3
            >>> conn.execute(
            ...     "SELECT COUNT(*) FROM sqlite_temp_master WHERE name=?", (table,)
            ... ).fetchone()[0]
            0
            >>> conn.close()


        :param values: Iterable consumed once during context entry, one value per
            inserted row.
        :param column: Validated temporary-column name, default value.
        :param declared_type: Allowlisted backend datatype name; values themselves are
            not pre-coerced.
        :param prefix: Validated table-name prefix, at most 30 characters before the
            UUID suffix.
        :return: A context manager yielding the generated table-name string while the
            table exists.
        """

    @abc.abstractmethod
    def temporary_id_table(
        self,
        values: Iterable[Any],
        *,
        prefix: str = "liuxin_ids",
    ) -> AbstractContextManager[str]:
        """
        Return the temporary-value context manager configured with an INTEGER column
        named id.

        Forward values and prefix without eagerly consuming or validating them. Context
        entry, transaction ownership, cleanup and error behavior are those of
        temporary_value_table. Duplicates and None are not
        rejected, and Python integer types are not independently enforced.

        Example:
            ``with macros.temporary_id_table((10, 11, 11)) as table: ...``
            creates three rows in the id column for the duration of the context.


        :param values: Iterable of backend-compatible ID values, consumed on context
            entry.
        :param prefix: Simple temporary-table prefix, checked on entry and limited to 30
            characters.
        :return: The delegated context manager yielding an unqualified temporary-table
            name.
        """

    @abc.abstractmethod
    def delete_unreferenced_rows(
        self,
        table: str,
        link_specs: Iterable[StorageLinkSpec],
        *,
        id_column: str | None = None,
        protected_ids: Iterable[Any] = (),
    ) -> tuple[Any, ...]:
        """
        Delete target IDs unreferenced by all supplied relationships.

        Only supplied link specs participate; callers must provide every relationship
        they intend to protect. A self-link checks both endpoints. Other schema
        constraints can still reject deletion. Avoid None in protected_ids: SQL NOT IN
        with NULL can suppress all candidates. Delete candidate IDs in chunks within one
        portable transaction.

        Example:
            Given a tag relationship spec, macros.delete_unreferenced_rows("tags",
            (spec,), protected_ids=(reserved_tag_id,)) prunes unlinked tags while
            preserving the reserved ID.


        :param table: Existing table name accepted by schema introspection and
            identifier validation.
        :param link_specs: Link specs to consult; at least one must reference the target
            as a primary or secondary endpoint.
        :param id_column: Unique row-ID column; None discovers the table ID column.
        :param protected_ids: IDs exempt from deletion, bound in one NOT IN predicate.
        :return: Tuple of deleted candidate IDs in ascending ID order.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def delete_unreferenced_rows_bulk(
        self,
        specs: Iterable[UnreferencedRowsSpec],
    ) -> dict[str, tuple[Any, ...]]:
        """
        Prune several distinct tables in input order within one transaction.

        Materialize and prepare requests before the transaction. Each request follows
        delete_unreferenced_rows rules and sees deletions performed for earlier
        requests, so order can affect subsequent orphan checks. An empty iterable
        returns an empty mapping.

        Example:
            Given prepared pruning specs,
            macros.delete_unreferenced_rows_bulk((tag_pruning, genre_pruning)) performs
            the two requested sweeps in that order.


        :param specs: Iterable of UnreferencedRowsSpec values; duplicate target tables
            are rejected.
        :return: Dictionary mapping each target table to its ascending tuple of deleted
            IDs.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        """

    @abc.abstractmethod
    def fingerprint_table(
        self,
        target_table: str,
        columns: Iterable[str] | None = None,
        *,
        order_by: Iterable[str] | None = None,
        where: Mapping[str, Any] | None = None,
        algorithm: str = "sha256",
    ) -> str:
        """
        Hash selected table values with typed encoding and explicit row ordering.

        Column order and typed values contribute to the digest. Choose ordering that
        breaks ties for repeatability. Canonical encodings distinguish common
        scalar/container types; unknown objects fall back to their qualified type and
        string representation, whose stability is caller-dependent. Variable-length
        digest algorithms requiring a length argument are not supported by the final
        no-argument hexdigest call.

        Example:
            With concrete macros, macros.fingerprint_table("tags", columns=("tag_id",
            "tag"), order_by=("tag_id",)) can detect changes to those ordered tag
            values.


        :param target_table: Existing table name accepted by schema introspection and
            identifier validation.
        :param columns: Ordered selected columns, or None for all schema columns; cannot
            be empty.
        :param order_by: Ascending ordering columns; None uses the discovered ID,
            falling back to selected columns; an explicit empty iterable is invalid.
        :param where: Equality filters combined with AND; None values use IS NULL.
        :param algorithm: Hashlib algorithm name, stripped and lowercased; use a
            fixed-length digest such as the default sha256.
        :return: Hexadecimal digest of a length-prefixed table/column header and encoded
            rows.
        :raises InputIntegrityError: An identifier, schema column, link specification or
            supplied value violates the operation’s validation rules.
        :raises TypeError: The chosen digest requires a length argument for hexdigest.
        """

    @abc.abstractmethod
    def delete_rows_by_id(self, table: str, row_ids: Iterable[int]) -> None:
        """Delete physical rows in bounded IN batches within one transaction.

        Validate the table and all integer IDs before effects. Duplicate IDs
        collapse and missing IDs are no-ops. Schema cascades apply. This neither
        chooses retention policy nor removes physical files.
        """

    @abc.abstractmethod
    def update_rows(self, table: str, updates: Mapping[int, Mapping[str, Any]]) -> None:
        """Update physical rows atomically, batching consecutive matching shapes.

        Copy and validate every payload before writes; reject ID-column changes.
        Derive comparison keys through the same declarations as update_row.
        Empty payloads and missing IDs are no-ops. Preserve input order so trigger
        and uniqueness behavior does not depend on grouping by column names.
        """

    @abc.abstractmethod
    def find_table_values(
        self,
        table: str,
        value_column: str,
        values: Iterable[Any],
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> dict[Any, Any | None]:
        """Find many physical vocabulary rows using the scalar matching policy.

        Return original hashable inputs mapped to IDs or None. Equivalent text
        spellings can share an ID; duplicate inputs collapse. Declared stored text
        keys use bounded IN queries. Other policies retain scalar matching rules.
        Scope fields are required exactly as for ensure_table_values. No writes
        or independent snapshot transaction are made.
        """

    @abc.abstractmethod
    def ensure_table_values(
        self,
        table: str,
        value_column: str,
        values: Iterable[Any],
        *,
        id_column: str | None = None,
        additional_values: Mapping[str, Any] | None = None,
    ) -> dict[Any, Any]:
        """Ensure physical vocabulary rows atomically using shared matching rules.

        Materialize and preflight input before writes. Declared stored text keys
        use bounded batch lookups and insert each missing normalized key once,
        preserving its first supplied spelling. Existing spellings remain intact.
        Other policies retain scalar ensure behavior. Reread after conflict-aware
        inserts and reject unresolved values or duplicate matches; late failures
        roll back the whole batch, including inside an enclosing transaction.
        """

    @abc.abstractmethod
    def audit_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Audit a table's declared keys without changing rows or schema.

        Select declarations by display-column name and optionally select row IDs.
        Counts include all scanned rows: collisions compare proposed selected keys
        with unchanged stored keys on other rows. Unrelated collisions are omitted.
        An empty selection is a no-op. Missing IDs select no rows. This read uses
        the current connection without opening a new snapshot transaction.
        """

    @abc.abstractmethod
    def rebuild_normalized_keys(
        self,
        table: str,
        *,
        value_columns: Iterable[str] | None = None,
        row_ids: Iterable[int] | None = None,
    ) -> NormalizedIdentityMigrationReport:
        """Atomically repair selected derived keys without schema or display edits.

        Audit within the write transaction and reject collisions before effects.
        Clear changing keys before setting their final values to support key swaps
        under unique indexes. Existing constraints can reject repair (for example,
        a nonnullable key); any failure rolls back this entire call. Source text,
        scope fields, identity declarations and indexes are never rewritten here.
        """

    @abc.abstractmethod
    def get_rows_by_ids(
        self, table: str, row_ids: Iterable[int]
    ) -> dict[int, Mapping[str, Any]]:
        """Read physical rows by integer ID using batches of at most 500 bindings.

        Validate the table and materialize all IDs before reading row data.
        Duplicate requests collapse; missing IDs are omitted. Result keys follow
        first request order. Booleans and projection rays are invalid IDs. Use
        the current connection without starting a snapshot or committing it.

        Example:
            ``macros.get_rows_by_ids("works", [7, 9, 7])`` returns up to two
            independent row dictionaries, keyed by the IDs that exist.

        :param table: Physical table, never a view; its ID column is discovered.
        :param row_ids: Finite iterable of integer physical row IDs.
        :return: Found IDs mapped to fresh column/value dictionaries.
        :raises DatabaseIntegrityError: A discovered ID matches multiple rows.
        """

    @abc.abstractmethod
    def get_linked_rows(
        self,
        link_spec: StorageLinkSpec,
        primary_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Read links once and fetch distinct destinations in bounded ID batches.

        Preserve get_link_rows ordering, filters, extras and repeated destinations.
        Each result owns a shallow row copy; missing destinations remain as None.
        Require physical endpoint/link tables and specs addressing their discovered
        ID columns. Source existence and dangling-link policy belong to the caller.
        Reads use the current connection and establish no new snapshot transaction.

        Example:
            ``macros.get_linked_rows(spec, work_id)`` returns link/row pairs;
            inspect ``result.row is None`` before rendering a destination.

        :param link_spec: Directional relationship over physical integer row IDs.
        :param primary_id: Integer source ID; booleans and rays are rejected.
        :param link_type: Omitted sentinel for all types, None for NULL, or a type.
        :return: One LinkedTableRow per link, including dangling relationships.
        """

    @abc.abstractmethod
    def get_rows_for_ids(
        self,
        table: str,
        foreign_id_column: str,
        owner_ids: Iterable[int],
        *,
        where: Mapping[str, Any] | None = None,
    ) -> dict[int, tuple[Mapping[str, Any], ...]]:
        """Group physical rows by a requested integer reference-column value.

        Materialize and validate all IDs and equality filters before row reads.
        Empty groups are retained in first-request order. Each group is ordered
        by the table's physical ID, with no normalization, deduplication of rows
        or owner-existence checks. NULL references match no requested integer.
        Bound owner IDs in chunks of at most 500, reduced for other bindings.
        Use the caller's current connection without starting or committing a
        transaction; all inputs and results are materialized.

        Example:
            ``macros.get_rows_for_ids("items", "item_manifestation_id", [3, 4])``
            returns Item rows for each requested physical Manifestation ID.

        :param table: Physical table whose rows are returned; views are rejected.
        :param foreign_id_column: Existing integer reference column to group by.
        :param owner_ids: Physical integer references; duplicates collapse.
        :param where: Additional exact equality filters; None values use IS NULL.
        :return: Requested IDs mapped to row tuples, including empty tuples.
        """


__all__ = ["PortableMacrosAPI"]
