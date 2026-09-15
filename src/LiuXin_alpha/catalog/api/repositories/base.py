"""
Declare the common CRUD protocol inherited by Catalog repositories.

Inputs use public aliases or storage columns, subject to the concrete repository's
schema and policies. Mapping return annotations do not enforce immutable objects:
the base implementation supplies shallow dictionaries with no write-through behavior.
Portable macros own persistence; this protocol adds no transaction or locking layer.
"""

# Todo: We want a means to get metadata objects back out of these sorts of calls...

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from LiuXin_alpha.catalog.api.common import EntityId, RowInput, RowMapping


@runtime_checkable
class BaseRepositoryAPI(Protocol):
    """
    Declare the shared CRUD shape for Catalog entity repositories.

    Runtime protocol checks establish attribute/method presence, not signatures, database
    compatibility, or transaction behavior. Concrete repositories may add matching, relationship,
    immutability, and ownership rules. The base implementation validates IDs and input aliases
    against live storage columns, exposes row data through Mapping, and returns shallow dictionaries
    rather than enforced immutable objects. Use get for optional absence and require for an explicit
    error.

    Creation excludes caller-assigned IDs. Read-before-write methods and higher-level match/create
    workflows do not gain an enclosing transaction from this protocol.

    Example:
        >>> repository: BaseRepositoryAPI = catalog.works  # doctest: +SKIP
        >>> row = repository.require(work_id)  # doctest: +SKIP


    :ivar table_name: Storage table served by the repository.
    :ivar id_column: Storage ID column used by CRUD operations.
    """

    table_name: str
    id_column: str

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
