"""
Implement schema-aware Catalog CRUD and shared relationship mechanics.

BaseRepository delegates SQL, constraints, and transaction management to portable
macros and wrapper introspection. Reads copy row mappings; helpers translate aliases,
validate IDs, bind composition dependencies, and traverse or upsert schema links.
WEMI_TABLES maps the four semantic levels to storage names. normalise_text provides
punctuation-preserving comparison text, distinct from fuzzy matching normalization.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Sequence
import unicodedata

from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkRow, LinkValue
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

from ..api.common import (
    CatalogMutationError,
    CatalogNotFoundError,
    DatabaseHandle,
    EntityId,
    RowInput,
    RowMapping,
    WemiLevel,
)

if TYPE_CHECKING:
    from ..matching.policy import MatchingPolicy


WEMI_TABLES: Mapping[WemiLevel, str] = {
    "work": "works",
    "expression": "expressions",
    "manifestation": "manifestations",
    "item": "items",
}


def normalise_text(value: object) -> str:
    """
    Produce NFKC, case-folded, whitespace-collapsed comparison text.

    Convert any input with str first, so None becomes "none" and bytes keep their string
    representation. Preserve punctuation and word boundaries; whitespace runs become one space,
    rather than being removed. Do not strip accents or validate semantic identity. This differs from
    punctuation-tolerant match text.

    Example:
        >>> normalise_text("  Ａ & B!  ")
        'a & b!'
        >>> normalise_text(None)
        'none'


    :param value: Object whose string representation supplies the comparison text.
    :return: Normalized string; an all-whitespace input becomes empty.
    """

    text = unicodedata.normalize("NFKC", str(value))
    return " ".join(text.casefold().split())


class BaseRepository:
    """
    Provide schema-aware CRUD and shared relationship helpers for one Catalog table.

    Subclasses declare table_name, id_column, and input_aliases; the empty default table is not a
    usable schema selection. The borrowed database provides portable macros and a driver wrapper,
    checked lazily at access. Reads return shallow dictionaries even though the API exposes Mapping,
    and modifying them does not persist changes. Schema names are inspected on each normalization
    call.

    Generic CRUD delegates writes to macros without an encompassing read/write transaction. The
    _link helper explicitly enters a macro transaction for its endpoint reads, priority selection,
    and upsert. Repositories do not open or close the database. Repository-group and matching-policy
    binding are separate assignments used by subclass services.

    Example:
        >>> repository = BaseRepository(None)
        >>> repository.list(limit=0)
        ()


    :ivar db: Borrowed database handle; no eager capability validation or lifetime ownership.
    :ivar table_name: Subclass-selected storage table name.
    :ivar id_column: Storage ID column, protected by input normalization.
    :ivar input_aliases: Mapping of public input keys to storage columns; retained as class configuration.
    :ivar _repositories: Bound repository group, initially None.
    :ivar _matching_policy: Bound matching policy, initially None to use the shared default.
    """

    table_name: ClassVar[str] = ""
    id_column: ClassVar[str] = "id"
    input_aliases: ClassVar[Mapping[str, str]] = {}

    def __init__(self, db: DatabaseHandle) -> None:
        """
        Retain the database and initialize unbound composition dependencies.

        Assign the handle by reference without opening it or checking its capabilities.
        Repository-group and policy access remain independent of this assignment.

        Example:
            >>> repository = BaseRepository(None)
            >>> repository.db is None
            True


        :param db: Borrowed database exposing driver_wrapper and portable macros when operations need them.
        :return: None after initializing dependency references.
        """

        self.db = db
        self._repositories: Any = None
        self._matching_policy: MatchingPolicy | None = None

    def bind_repositories(self, repositories: Any) -> None:
        """
        Replace the repository-group reference used by cross-repository services.

        Do not validate members, copy the group, or bind a matching policy. Rebinding affects later
        lookups; None restores the unbound state and makes repositories raise on access.

        Example:
            >>> repository = BaseRepository(None)
            >>> group = object()
            >>> repository.bind_repositories(group)
            >>> repository.repositories is group
            True


        :param repositories: Group object retained by reference; member capabilities are checked only by consumers.
        :return: None after replacing the group reference.
        """

        self._repositories = repositories

    def bind_matching_policy(self, policy: MatchingPolicy) -> None:
        """
        Replace the policy reference read by later matching operations.

        This assignment performs no validation and does not update matchers already constructed with
        an earlier policy. A None value restores the default-policy fallback at runtime, though the
        annotation requests MatchingPolicy.

        Example:
            >>> from LiuXin_alpha.catalog.matching.policy import MatchingPolicy
            >>> repository = BaseRepository(None)
            >>> policy = MatchingPolicy(acceptance_threshold=0.9)
            >>> repository.bind_matching_policy(policy)
            >>> repository.matching_policy is policy
            True


        :param policy: Matching policy retained by reference for future access.
        :return: None after replacing the policy reference.
        """

        self._matching_policy = policy

    @property
    def repositories(self) -> Any:
        """
        Return the bound repository group without further validation.

        Only None denotes an unbound group; other false-valued objects are returned as assigned.
        Composition can be performed directly and need not come from Catalog.

        Example:
            >>> repository = BaseRepository(None)
            >>> repository.bind_repositories({})
            >>> repository.repositories
            {}


        :return: The exact object passed to bind_repositories.
        :raises RuntimeError: If no non-None group is bound.
        """

        if self._repositories is None:
            raise RuntimeError("repository group has not been bound")
        return self._repositories

    @property
    def matching_policy(self) -> MatchingPolicy:
        """
        Return the bound matching policy or lazily import the shared default.

        Do not cache the default in the instance or construct a new policy. Only None triggers
        fallback; an invalid non-None value assigned at runtime passes through.

        Example:
            >>> from LiuXin_alpha.catalog.matching.policy import DEFAULT_MATCHING_POLICY
            >>> BaseRepository(None).matching_policy is DEFAULT_MATCHING_POLICY
            True


        :return: The bound policy reference or DEFAULT_MATCHING_POLICY.
        """

        if self._matching_policy is None:
            from ..matching.policy import DEFAULT_MATCHING_POLICY

            return DEFAULT_MATCHING_POLICY
        return self._matching_policy

    @property
    def _wrapper(self) -> Any:
        """
        Read the current driver wrapper from the borrowed database.

        Missing and None-valued attributes raise TypeError. Other objects are returned without
        checking individual methods, and the value is not cached.

        Example:
            >>> from types import SimpleNamespace
            >>> wrapper = object()
            >>> BaseRepository(SimpleNamespace(driver_wrapper=wrapper))._wrapper is wrapper
            True


        :return: The current non-None db.driver_wrapper object.
        :raises TypeError: If the database has no non-None driver_wrapper.
        """
        wrapper = getattr(self.db, "driver_wrapper", None)
        if wrapper is None:
            raise TypeError("catalog database must provide driver_wrapper")
        return wrapper

    @property
    def _macros(self) -> Any:
        """
        Read the current portable macro service from the borrowed database.

        Missing and None-valued attributes raise TypeError. Method availability is left to the
        caller, and replacing db.macros affects the next property access.

        Example:
            >>> from types import SimpleNamespace
            >>> macros = object()
            >>> BaseRepository(SimpleNamespace(macros=macros))._macros is macros
            True


        :return: The current non-None db.macros object.
        :raises TypeError: If the database has no non-None macros service.
        """
        macros = getattr(self.db, "macros", None)
        if macros is None:
            raise TypeError("catalog database must provide portable macros")
        return macros

    @staticmethod
    def _validate_entity_id(entity_id: EntityId) -> None:
        """
        Accept nonnegative integer IDs while rejecting booleans and coercible values.

        Zero and int subclasses other than bool are accepted. This checks representation and sign
        only, without checking table membership or row existence.

        Example:
            >>> BaseRepository._validate_entity_id(0)
            >>> BaseRepository._validate_entity_id(True)
            Traceback (most recent call last):
            ...
            TypeError: entity_id must be an integer


        :param entity_id: Database ID to validate without coercion.
        :return: None for an accepted ID.
        :raises TypeError: If the value is not an integer or is a boolean.
        :raises ValueError: If the integer is negative.
        """
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            raise TypeError("entity_id must be an integer")
        if entity_id < 0:
            raise ValueError("entity_id cannot be negative")

    @property
    def columns(self) -> tuple[str, ...]:
        """
        Fetch current column headings for the configured storage table.

        Preserve the wrapper's declared order in a tuple. This property does not cache schema
        information or remove the protected ID column from the returned names.

        Example:
            >>> columns = catalog.works.columns  # doctest: +SKIP


        :return: Tuple of storage-column names in wrapper order.
        :raises Exception: Missing wrapper capabilities and schema lookup failures propagate.
        """

        return tuple(self._wrapper.get_column_headings(self.table_name))

    def normalise_input(
        self,
        data: RowInput,
        *,
        allow_id: bool = False,
        ignore_unknown: bool = False,
    ) -> dict[str, Any]:
        """
        Translate input aliases and validate keys against the current table schema.

        Require a Mapping before reading columns, then inspect string keys in input order. Resolve
        aliases once; unknown resolved columns are rejected or skipped according to ignore_unknown.
        The known ID column is rejected unless allow_id, even when unknown keys are ignored. Other
        declared columns are accepted without a separate writable-column or value-type check in this
        base method.

        Several keys may address one column if their values compare equal. A conflicting value
        raises before a payload is returned. Return a new dict while preserving value references,
        including None; do not mutate input, normalize text values, enforce required fields, or
        validate an allowed ID value. Empty mappings still trigger schema inspection. Subclass
        overrides may impose additional rules.

        Example:
            >>> from types import SimpleNamespace
            >>> wrapper = SimpleNamespace(get_column_headings=lambda table: ("id", "name"))
            >>> repository = BaseRepository(SimpleNamespace(driver_wrapper=wrapper))
            >>> repository.input_aliases = {"label": "name"}
            >>> repository.normalise_input({"label": "A", "name": "A", "extra": 9}, ignore_unknown=True)
            {'name': 'A'}
            >>> repository.normalise_input({"id": "unchecked"}, allow_id=True)
            {'id': 'unchecked'}


        :param data: String-keyed mapping of aliases or storage-column values.
        :param allow_id: Permit the known ID column without validating its value when truthy.
        :param ignore_unknown: Skip keys resolving to absent columns when truthy; known ID and collision rules still apply.
        :return: New storage-column dictionary retaining supplied value objects.
        :raises TypeError: If data is not a Mapping or a key is not a string.
        :raises CatalogMutationError: If an unknown/protected column or unequal alias collision is encountered.
        :raises Exception: Schema lookup and custom mapping/equality failures propagate.
        """

        if not isinstance(data, Mapping):
            raise TypeError("repository data must be a mapping")
        available = set(self.columns)
        result: dict[str, Any] = {}
        for raw_key, value in data.items():
            if not isinstance(raw_key, str):
                raise TypeError("repository data keys must be strings")
            key = self.input_aliases.get(raw_key, raw_key)
            if key not in available:
                if ignore_unknown:
                    continue
                raise CatalogMutationError(
                    f"{raw_key!r} is not writable through {type(self).__name__}"
                )
            if key == self.id_column and not allow_id:
                raise CatalogMutationError(
                    f"{self.id_column!r} cannot be changed through repository data"
                )
            if key in result and result[key] != value:
                raise CatalogMutationError(
                    f"multiple inputs specify conflicting values for {key!r}"
                )
            result[key] = value
        return result

    @staticmethod
    def _as_mapping(row: object) -> dict[str, Any]:
        """
        Copy a database row into a plain dictionary without deep-copying its values.

        Mapping instances use dict(row). Other objects must expose callable keys and indexed access
        for each reported key; iterator-of-pair input alone is insufficient. Backend failures while
        reading keys/values propagate.

        Example:
            >>> values = []
            >>> row = {"values": values}
            >>> copy = BaseRepository._as_mapping(row)
            >>> copy is row, copy["values"] is values
            (False, True)


        :param row: Mapping or keys-plus-indexing database row object.
        :return: A shallow plain-dict copy.
        :raises TypeError: If no supported mapping interface exists.
        """
        if isinstance(row, Mapping):
            return dict(row)
        keys = getattr(row, "keys", None)
        if callable(keys):
            return {key: row[key] for key in keys()}  # type: ignore[index]
        raise TypeError("database rows must provide a mapping interface")

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """
        Read one entity by validated ID, treating a false-valued database row as absent.

        The base implementation validates the ID before accessing portable macros and requests the
        repository's explicit ID column. A returned row becomes a shallow dictionary copy; mutating
        that dictionary does not write back, though nested values are not deep-copied. Concrete
        repositories may impose additional read rules.

        Example:
            >>> row = catalog.works.get(work_id)  # doctest: +SKIP


        :param entity_id: Nonnegative integer database ID; zero is accepted and booleans are rejected.
        :return: A storage-column row mapping, or None for an absent/false-valued row.
        :raises TypeError: If the ID is not an integer, is a boolean, or required database/row capabilities are missing.
        :raises ValueError: If the ID is negative.
        :raises Exception: Database read failures propagate.
        """

        self._validate_entity_id(entity_id)
        row = self._macros.get_row(
            self.table_name,
            entity_id,
            id_column=self.id_column,
        )
        return None if not row else self._as_mapping(row)

    def require(self, entity_id: EntityId) -> RowMapping:
        """
        Read an entity and raise an explicit Catalog error when get finds no row.

        Delegate ID validation and row conversion to get. This is an existence check, not a lock or
        a guarantee that a later operation sees the same row.

        Example:
            >>> row = catalog.works.require(work_id)  # doctest: +SKIP


        :param entity_id: Nonnegative non-boolean integer ID of the required entity.
        :return: The existing storage-column mapping returned by get.
        :raises CatalogNotFoundError: If get returns None.
        :raises Exception: ID validation and database/row conversion errors propagate.
        """

        row = self.get(entity_id)
        if row is None:
            raise CatalogNotFoundError(f"{self.table_name} row not found: {entity_id}")
        return row

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """
        Return an ID-ordered page after materializing the table's rows.

        The base implementation validates both paging arguments even for limit zero, then returns an
        empty tuple without database access for zero. A positive limit fetches and copies all rows
        in ID order before slicing in Python; it does not bound database work. Row dictionaries are
        shallow copies with no write-through behavior.

        Example:
            >>> page = catalog.works.list(limit=10, offset=20)  # doctest: +SKIP


        :param limit: Nonnegative integer page size; booleans are rejected.
        :param offset: Nonnegative integer count of rows to skip; booleans are rejected.
        :return: An ID-ordered sequence, implemented as a tuple by BaseRepository.
        :raises TypeError: If either paging argument is not an integer or is a boolean.
        :raises ValueError: If either paging argument is negative.
        :raises Exception: Database reads and row conversion failures propagate.
        """

        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if not isinstance(offset, int) or isinstance(offset, bool):
            raise TypeError("offset must be an integer")
        if limit < 0 or offset < 0:
            raise ValueError("limit and offset cannot be negative")
        if limit == 0:
            return ()
        materialised = self._all_rows()
        return materialised[offset : offset + limit]

    def _all_rows(self) -> tuple[RowMapping, ...]:
        """
        Read and shallow-copy every row, requesting ascending repository-ID order.

        Delegate ordering to get_rows and materialize the whole iterable before returning. There is
        no paging, caching, filtering, or second Python sort.

        Example:
            >>> rows = catalog.works._all_rows()  # doctest: +SKIP


        :return: Tuple of plain row dictionaries in macro-provided ID order.
        :raises Exception: Database access and row-conversion failures propagate.
        """
        rows = self._macros.get_rows(
            self.table_name,
            order_by=(self.id_column,),
        )
        return tuple(self._as_mapping(row) for row in rows)

    def create(self, data: RowInput) -> EntityId:
        """
        Normalize writable fields, insert one entity, and check the returned ID.

        BaseRepository rejects empty normalized payloads and caller-supplied ID columns. Other field
        validation follows the live schema, aliases, subclass rules, and backend constraints; it
        does not independently validate every value's type. The result must be an integer other than
        bool, but no sign check is performed.

        Insertion and transaction handling belong to portable macros. The returned-ID check occurs
        after insertion and does not undo a write if that check fails.

        Example:
            >>> work_id = catalog.works.create({"title": "Frankenstein"})  # doctest: +SKIP


        :param data: Nonempty mapping of public aliases or storage columns; values are passed through normalization.
        :return: The integer ID returned by insertion.
        :raises TypeError: If data or its keys have unsuitable types.
        :raises CatalogMutationError: If normalization rejects fields, the payload is empty, or insertion returns a non-integer/boolean ID.
        :raises Exception: Schema, backend constraint, and insertion failures propagate.
        """

        payload = self.normalise_input(data)
        if not payload:
            raise CatalogMutationError(
                f"{type(self).__name__}.create requires at least one value"
            )
        new_id = self._macros.insert_row(
            self.table_name,
            payload,
            id_column=self.id_column,
        )
        if not isinstance(new_id, int) or isinstance(new_id, bool):
            raise CatalogMutationError(
                f"database did not return an ID for new {self.table_name} row"
            )
        return new_id

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """
        Require an existing entity, then normalize and replace the supplied fields.

        Existence is checked before payload validation, including for an empty update.
        BaseRepository skips the macro write for no changes; otherwise it passes only supplied
        fields to update_row. It does not compare them to stored values first. The existence check
        and write have no enclosing repository transaction; driver behavior determines concurrency
        and transaction semantics.

        Example:
            >>> catalog.works.update(work_id, {"canonical_title": "Frankenstein"})  # doctest: +SKIP


        :param entity_id: ID of the entity whose existence is checked first.
        :param data: Mapping of fields to replace; omitted fields remain unchanged and an empty mapping is permitted.
        :return: None after an empty update or successful delegated write.
        :raises CatalogNotFoundError: If the entity is absent before normalization.
        :raises Exception: ID/input validation, schema inspection, and update errors propagate.
        """

        self.require(entity_id)
        changes = self.normalise_input(data)
        if not changes:
            return
        self._macros.update_row(
            self.table_name,
            entity_id,
            changes,
            id_column=self.id_column,
        )

    def delete(self, entity_id: EntityId) -> None:
        """
        Require the entity and delegate its deletion to the database macros.

        BaseRepository performs no additional ownership scan or lifecycle policy check. Subclass
        policies and backend constraints may reject deletion. The preliminary read and deletion are
        separate operations without an enclosing repository transaction, so the read does not lock
        the entity against concurrent changes.

        Example:
            >>> catalog.works.delete(work_id)  # doctest: +SKIP


        :param entity_id: ID of the entity to require and then delete.
        :return: None after the delegated deletion returns.
        :raises CatalogNotFoundError: If the initial read finds no entity.
        :raises CatalogMutationError: If a concrete repository policy rejects deletion.
        :raises Exception: ID validation, database reads, and deletion/constraint errors propagate.
        """

        self.require(entity_id)
        self._macros.delete_row(
            self.table_name,
            entity_id,
            id_column=self.id_column,
        )

    def _require_table_row(self, table: str, entity_id: EntityId) -> RowMapping:
        """
        Require a row in an arbitrary table using that table's default ID column.

        Validate the ID before reading. Unlike get, do not pass this repository's id_column: the
        macros resolve the requested table's ID. False-valued rows count as absent; found rows are
        shallow dictionary copies.

        Example:
            >>> row = catalog.items._require_table_row("manifestations", manifestation_id)  # doctest: +SKIP


        :param table: Storage table whose default ID column the macros resolve.
        :param entity_id: Nonnegative non-boolean integer ID to require.
        :return: The existing row as a plain dictionary.
        :raises CatalogNotFoundError: If the row is absent or false-valued.
        :raises Exception: ID validation, table lookup, and row-conversion failures propagate.
        """
        self._validate_entity_id(entity_id)
        row = self._macros.get_row(table, entity_id)
        if not row:
            raise CatalogNotFoundError(f"{table} row not found: {entity_id}")
        return self._as_mapping(row)

    def _link_spec(self, primary_table: str, secondary_table: str) -> StorageLinkSpec:
        """
        Resolve a directional table relationship and require a StorageLinkSpec.

        Ask the wrapper for the primary/secondary orientation supplied by the caller. This validates
        the returned object's type only; endpoint existence and other schema capabilities are
        handled elsewhere.

        Example:
            >>> spec = catalog.expressions._link_spec("works", "expressions")  # doctest: +SKIP


        :param primary_table: Table anchoring the requested link orientation.
        :param secondary_table: Table reached from the primary endpoint.
        :return: The wrapper-provided StorageLinkSpec object.
        :raises CatalogMutationError: If the wrapper returns an object of another type.
        :raises Exception: Wrapper capability and relationship lookup failures propagate.
        """
        spec = self._wrapper.get_link_spec(primary_table, secondary_table)
        if not isinstance(spec, StorageLinkSpec):
            raise CatalogMutationError(
                f"no catalog link exists from {primary_table!r} to {secondary_table!r}"
            )
        return spec

    def _link(
        self,
        primary_table: str,
        primary_id: EntityId,
        secondary_table: str,
        secondary_id: EntityId,
        *,
        link_type: str | None = None,
        priority: int | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> LinkRow:
        """
        Validate endpoints and upsert a relationship inside a portable macro transaction.

        Require primary and secondary rows before resolving the directional link spec. For an
        ordered link with omitted priority, find the first existing link with the secondary ID and,
        when type is part of identity, the same type. Preserve its priority, including None.
        Otherwise assign int(max(existing numeric priorities, default=0)) + 1 across all types,
        ignoring booleans and other values. This is not a count-based rank and has no finite-number
        check.

        Forward explicit priorities unchanged. Package the secondary ID, type, priority, and a
        shallow dict of extra values into LinkValue for upsert. The transaction covers these reads
        and this link write only; callers must supply an outer transaction to include an entity
        created earlier. Backend validation and rollback semantics remain with the macro service.

        Example:
            >>> link = catalog.expressions._link("works", work_id, "expressions", expression_id)  # doctest: +SKIP


        :param primary_table: Storage table for the source endpoint.
        :param primary_id: Source ID validated and required inside the transaction.
        :param secondary_table: Storage table for the destination endpoint.
        :param secondary_id: Destination ID validated and required inside the transaction.
        :param link_type: Desired type, including None; identity participation follows the link spec.
        :param priority: Explicit priority, or None to retain/derive one for ordered links.
        :param extra: Additional link-column values copied into a dict; a false-valued mapping becomes empty.
        :return: The LinkRow returned by the macro upsert.
        :raises CatalogNotFoundError: If either endpoint is missing.
        :raises CatalogMutationError: If no StorageLinkSpec is returned.
        :raises Exception: Validation, priority arithmetic, transaction, and upsert failures propagate.
        """
        with self._macros.transaction():
            self._require_table_row(primary_table, primary_id)
            self._require_table_row(secondary_table, secondary_id)
            spec = self._link_spec(primary_table, secondary_table)
            if spec.ordered and priority is None:
                rows = self._macros.get_link_rows(spec, primary_id)
                existing = next(
                    (
                        row
                        for row in rows
                        if row.secondary_id == secondary_id
                        and (
                            not spec.type_part_of_identity
                            or row.link_type == link_type
                        )
                    ),
                    None,
                )
                if existing is not None:
                    priority = existing.priority
                else:
                    assigned = tuple(
                        row.priority
                        for row in rows
                        if isinstance(row.priority, (int, float))
                        and not isinstance(row.priority, bool)
                    )
                    priority = int(max(assigned, default=0)) + 1
            return self._macros.upsert_link(
                spec,
                primary_id,
                LinkValue(
                    secondary_id,
                    link_type=link_type,
                    priority=priority,
                    extra=dict(extra or {}),
                ),
            )

    @staticmethod
    def _link_metadata(row: LinkRow) -> dict[str, Any]:
        """
        Render one link's endpoints, type, priority, and copied extra fields.

        The result is a new dictionary using public metadata keys. Copy the extra mapping once
        without recursively copying its values or normalizing link fields.

        Example:
            >>> BaseRepository._link_metadata(LinkRow(1, 2, "aut", 7, {"source": "manual"}))
            {'primary_id': 1, 'secondary_id': 2, 'type': 'aut', 'priority': 7, 'extra': {'source': 'manual'}}


        :param row: LinkRow whose fields supply the metadata.
        :return: Plain dictionary with primary_id, secondary_id, type, priority, and extra keys.
        """
        return {
            "primary_id": row.primary_id,
            "secondary_id": row.secondary_id,
            "type": row.link_type,
            "priority": row.priority,
            "extra": dict(row.extra),
        }

    def _linked_rows(
        self,
        primary_table: str,
        primary_id: EntityId,
        secondary_table: str,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[RowMapping, ...]:
        """
        Fetch related rows and attach each traversed link's metadata.

        Require the primary endpoint, resolve the directional spec, then fetch each linked secondary
        row separately in macro link order. Do not deduplicate rows reached by multiple links.
        Replace any existing _catalog_link key in each shallow row copy. A missing secondary raises
        rather than yielding a partial sequence or silently dropping the relationship.

        The default sentinel leaves type unfiltered; explicit None requests a null type and a
        supplied value filters that type. Macro rules may reject filters on untyped links. Portable
        SQL macros order by descending priority where a priority column exists, then secondary ID;
        this helper does not sort itself. Reads are not enclosed in a repository transaction or
        snapshot.

        Example:
            >>> rows = catalog.expressions._linked_rows("works", work_id, "expressions")  # doctest: +SKIP


        :param primary_table: Storage table of the required source row.
        :param primary_id: Nonnegative non-boolean integer source ID.
        :param secondary_table: Related table whose rows should be read.
        :param link_type: LINK_TYPE_UNSET for all types, None for null type, or a specific macro-supported type.
        :return: Tuple of shallow row dictionaries with _catalog_link metadata in link order.
        :raises CatalogNotFoundError: If the source or a linked destination is absent.
        :raises Exception: ID/spec/filter validation, macro reads, and row-conversion failures propagate.
        """
        self._require_table_row(primary_table, primary_id)
        spec = self._link_spec(primary_table, secondary_table)
        result: list[RowMapping] = []
        for link in self._macros.get_link_rows(
            spec,
            primary_id,
            link_type=link_type,
        ):
            row = self._macros.get_row(secondary_table, link.secondary_id)
            if not row:
                raise CatalogNotFoundError(
                    f"{secondary_table} row linked from {primary_table}:"
                    f"{primary_id} is missing: {link.secondary_id}"
                )
            rendered = self._as_mapping(row)
            rendered["_catalog_link"] = self._link_metadata(link)
            result.append(rendered)
        return tuple(result)

__all__ = ["BaseRepository", "WEMI_TABLES", "normalise_text"]
