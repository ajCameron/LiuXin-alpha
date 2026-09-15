"""
Declare Synopsis repository persistence, identity, and contextual read/write contracts.

The protocol extends ExactEntityRepositoryAPI; entity-specific mutation/reuse rules
and relationship behavior belong to the concrete repository. Runtime protocol checks
establish member presence rather than signatures, schema support, or atomicity.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class SynopsisRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """
    Store reusable Synopses and create fresh WEMI attachments when requested.

    SYNOPSIS_SPEC configures synopses/synopsis_id and text/value aliases for synopsis. Body identity
    preserves case and punctuation after NFKC/whitespace normalization. Explicit exact
    match_or_create can reuse content; add_for_wemi and replace_for_wemi always create fresh rows.
    No approximate policy field is configured. Attachment creation/replacement uses macro
    transactions, and replacing links does not explicitly delete detached Synopsis rows or alter
    unrelated owners' links.

    This structural protocol describes the concrete repository behavior and adds no runtime
    implementation, validation, or transaction handling of its own.

    Example:
        >>> repository: SynopsisRepositoryAPI = catalog.synopses  # doctest: +SKIP
    """

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """
        Create a fresh Synopsis and attach it to one existing WEMI entity.

        This operation calls create directly and does not search for or reuse equal content. It does
        not clear earlier attachments itself. Require the owner first, then enter one macro
        transaction around Synopsis insertion and the nested link helper. The link is requested with
        priority zero; cardinality and allowed-priority rules remain with the schema and macro
        backend. Failures inside that transaction are subject to its rollback semantics, including
        failure to resolve a link after insertion.

        Example:
            >>> entity_note_id = catalog.synopses.add_for_wemi(level="work", entity_id=work_id, data={"text": "First published anonymously."})  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Writable Synopsis aliases or storage columns; text/value map to its body column.
        :return: Newly created Synopsis ID after linking succeeds.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        synopses: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """
        Create fresh synopses and replace one WEMI entity's attachment set transactionally.

        Validate level, require a non-string Sequence, and convert each string to a synopsis-column
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
            >>> ids = catalog.synopses.replace_for_wemi(level="work", entity_id=work_id, synopses=["First", {"text": "Second"}])  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param synopses: Sequence of Synopsis body strings or row mappings; an empty sequence clears links.
        :return: Tuple of new Synopsis IDs in input order, empty after clearing.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        :raises TypeError: If the outer value is not a non-string Sequence or a member is neither a string nor a Mapping.
        """

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """
        Read Synopsis attachments and include their traversed relationship metadata.

        Validate the WEMI level and require the owner. A missing relationship specification is an
        error, even when the owner has no attachments; this method does not provide the Note
        repository's absent-schema fallback.

        Return shallow row dictionaries with _catalog_link endpoint/type/priority/extra metadata.
        Preserve macro link order and duplicate destinations from distinct links. Portable SQL
        macros use descending priority where present, then destination ID. A dangling destination
        raises instead of being silently omitted. There is no page limit or enclosing snapshot
        transaction.

        Example:
            >>> rows = catalog.synopses.list_for_wemi(level="work", entity_id=work_id)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :return: Tuple of linked Synopsis row mappings; empty for no links.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """


__all__ = ["SynopsisRepositoryAPI"]
