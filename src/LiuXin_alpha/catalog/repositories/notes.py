"""
Implement reusable Note persistence and WEMI attachment conveniences.

Exact body matching comes from ExactEntityRepository. Attachment methods create
fresh rows: single-add has separate insertion/link transactions unless enclosed by
a caller, while replacement joins all new Notes and link replacement in one macro
transaction. Listing tolerates a schema with no Note relationship for that owner.
"""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from LiuXin_alpha.databases.macro_types import LinkValue

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from ..matching.entity_specs import NOTE_SPEC
from .base import WEMI_TABLES
from .exact import ExactEntityRepository


class NoteRepository(ExactEntityRepository):
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

    Example:
        >>> note_id = catalog.notes.add_for_wemi(level="work", entity_id=work_id, data={"text": "Archive copy"})  # doctest: +SKIP


    :ivar match_spec: Shared NOTE_SPEC configuring body identity, aliases, and permitted reuse.
    """

    table_name = NOTE_SPEC.table_name
    id_column = NOTE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = NOTE_SPEC.input_aliases
    match_spec = NOTE_SPEC

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        self._require_table_row(WEMI_TABLES[level], entity_id)
        self._link_spec(WEMI_TABLES[level], self.table_name)
        note_id = self.create(data)
        self._link(WEMI_TABLES[level], entity_id, self.table_name, note_id)
        return note_id

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        self._require_table_row(WEMI_TABLES[level], entity_id)
        if self._wrapper.get_link_spec(WEMI_TABLES[level], self.table_name) is None:
            return ()
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(notes, Sequence) or isinstance(notes, (str, bytes)):
            raise TypeError("notes must be a sequence of strings or mappings")
        payloads: list[RowInput] = []
        for note in notes:
            if isinstance(note, str):
                payloads.append({"note": note})
            elif isinstance(note, Mapping):
                payloads.append(dict(note))
            else:
                raise TypeError("notes must contain only strings or mappings")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            note_ids = tuple(self.create(payload) for payload in payloads)
            self._macros.replace_links(
                spec,
                entity_id,
                (LinkValue(note_id) for note_id in note_ids),
            )
        return note_ids


__all__ = ["NoteRepository"]
