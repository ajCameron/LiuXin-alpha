"""
Declare Note repository persistence, identity, and contextual read/write contracts.

The protocol extends ExactEntityRepositoryAPI; entity-specific mutation/reuse rules
and relationship behavior belong to the concrete repository. Runtime protocol checks
establish member presence rather than signatures, schema support, or atomicity.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class NoteRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """
    Store reusable Notes and manage their WEMI attachment sets.

    NOTE_SPEC configures notes/note_id with text/value aliases for note. Body identity is
    case-sensitive and punctuation-preserving after NFKC/whitespace normalization, with no
    approximate policy field. Inherited match_or_create permits intentional reuse, while
    add_for_wemi and replace_for_wemi always create fresh rows.

    add_for_wemi does not wrap creation and linking in a common transaction. replace_for_wemi does
    wrap all creations and link replacement together, leaving detached destination rows intact at
    this repository layer. Listing requires an existing owner but returns empty when the wrapper
    reports no Note relationship.

    This structural protocol describes the concrete repository behavior and adds no runtime
    implementation, validation, or transaction handling of its own.

    Example:
        >>> repository: NoteRepositoryAPI = catalog.notes  # doctest: +SKIP
    """

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """
        Create a fresh Note and attach it to one existing WEMI entity.

        This operation calls create directly and does not search for or reuse equal content. It does
        not clear earlier attachments itself. Validate the owner and link spec before inserting.
        Creation then happens before the _link helper enters its own transaction and repeats the
        endpoint/spec checks. No outer transaction joins insertion and linking here: a later link
        failure can leave a new unattached Note unless the caller has supplied an encompassing
        transaction. Omitted link priority follows the base helper's existing/default ordered-link
        rules.

        Example:
            >>> entity_note_id = catalog.notes.add_for_wemi(level="work", entity_id=work_id, data={"text": "First published anonymously."})  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Writable Note aliases or storage columns; text/value map to its body column.
        :return: Newly created Note ID after linking succeeds.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Read Note attachments and include their traversed relationship metadata.

        Validate the WEMI level and require the owner. The Note implementation requires the owner
        and asks the wrapper for a link spec first. Only an exact None spec returns an empty tuple;
        another invalid spec proceeds to the base helper and fails validation. A supported
        relationship repeats owner/spec lookup through _linked_rows.

        Return shallow row dictionaries with _catalog_link endpoint/type/priority/extra metadata.
        Preserve macro link order and duplicate destinations from distinct links. Portable SQL
        macros use descending priority where present, then destination ID. A dangling destination
        raises instead of being silently omitted. There is no page limit or enclosing snapshot
        transaction.

        Example:
            >>> rows = catalog.notes.list_for_wemi(level="work", entity_id=work_id)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :return: Tuple of linked Note row mappings; empty for no links or a None link specification.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        notes: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """
        Create fresh notes and replace one WEMI entity's attachment set transactionally.

        Validate level, require a non-string Sequence, and convert each string to a note-column
        payload or each Mapping to a shallow dictionary. Reject other members before owner/schema
        access; individual payload keys and values are normalized later during creation. Then
        require the owner and resolve its link spec before opening the macro transaction.

        Inside the transaction, create every row in input order and replace links with LinkValue
        entries referring to those new IDs. No exact reuse or duplicate-content elimination occurs.
        Empty input still validates owner/schema and clears the selected relationship set. Old
        destination rows are not explicitly deleted, and unrelated owners' links are not targeted.
        Backend triggers/constraints remain applicable. Macro replacement assigns default priorities
        where the link spec is ordered; the returned ID tuple preserves caller order.

        The transaction joins all creations and link replacement; owner/spec preflight reads occur
        before it. There is no independent global identity/uniqueness check.

        Example:
            >>> ids = catalog.notes.replace_for_wemi(level="work", entity_id=work_id, notes=["First", {"text": "Second"}])  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param notes: Sequence of Note body strings or row mappings; an empty sequence clears links.
        :return: Tuple of new Note IDs in input order, empty after clearing.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        :raises TypeError: If the outer value is not a non-string Sequence or a member is neither a string nor a Mapping.
        """
