"""
Declare Comment repository persistence, identity, and contextual read/write contracts.

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
class CommentRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """
    Create contextual Comment attachments while keeping exact inspection available.

    COMMENT_SPEC configures comments/comment_id with text/value aliases for comment. Body comparison
    preserves case and punctuation after NFKC/whitespace normalization. The spec is mutable but
    non-reusable: inherited match_or_create always rejects before lookup, while direct create and
    exact/match remain available. No approximate policy field is configured. Attachment conveniences
    create fresh rows and use macro transactions for create/link or clear/replace; they do not
    delete detached historical Comment rows themselves.

    This structural protocol describes the concrete repository behavior and adds no runtime
    implementation, validation, or transaction handling of its own.

    Example:
        >>> repository: CommentRepositoryAPI = catalog.comments  # doctest: +SKIP
    """

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """
        Create a fresh Comment and attach it to one existing WEMI entity.

        This operation calls create directly and does not search for or reuse equal content. It does
        not clear earlier attachments itself. Require the owner first, then enter one macro
        transaction around Comment insertion and the nested link helper. The link is requested with
        priority zero; cardinality and allowed-priority rules remain with the schema and macro
        backend. Failures inside that transaction are subject to its rollback semantics, including
        failure to resolve a link after insertion.

        Example:
            >>> entity_note_id = catalog.comments.add_for_wemi(level="work", entity_id=work_id, data={"text": "First published anonymously."})  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Writable Comment aliases or storage columns; text/value map to its body column.
        :return: Newly created Comment ID after linking succeeds.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | None,
    ) -> EntityId | None:
        """
        Clear a WEMI entity's Comment links and optionally attach one fresh Comment.

        Validate level and owner, then resolve the link spec before entering a macro transaction.
        First replace the owner's links with an empty set. A None payload returns None from inside
        that transaction; any other payload, including an empty mapping, goes through create and may
        fail validation. For valid data, create and link a new Comment at priority zero before
        returning its ID.

        The transaction covers clearing, creation, and linking, so macro rollback can restore old
        links after a later failure. This method does not delete the old Comment rows or perform
        global matching/reuse; constraints and triggers remain backend responsibilities. Only the
        selected owner's relationship set is targeted.

        Example:
            >>> comment_id = catalog.comments.replace_for_wemi(level="work", entity_id=work_id, data={"text": "Revised comment"})  # doctest: +SKIP
            >>> catalog.comments.replace_for_wemi(level="work", entity_id=work_id, data=None)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :param data: Fresh Comment payload; exactly None clears without inserting a replacement.
        :return: New Comment ID after replacement, or None after clearing.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """
        Read Comment attachments and include their traversed relationship metadata.

        Validate the WEMI level and require the owner. A missing relationship specification is an
        error, even when the owner has no attachments; this method does not provide the Note
        repository's absent-schema fallback.

        Return shallow row dictionaries with _catalog_link endpoint/type/priority/extra metadata.
        Preserve macro link order and duplicate destinations from distinct links. Portable SQL
        macros use descending priority where present, then destination ID. A dangling destination
        raises instead of being silently omitted. There is no page limit or enclosing snapshot
        transaction.

        Example:
            >>> rows = catalog.comments.list_for_wemi(level="work", entity_id=work_id)  # doctest: +SKIP


        :param level: Exact lowercase WEMI level: work, expression, manifestation, or item.
        :param entity_id: Nonnegative non-boolean integer ID of the required entity at that level.
        :return: Tuple of linked Comment row mappings; empty for no links.
        :raises ValueError: If level is not a supported WEMI key.
        :raises CatalogNotFoundError: If a required endpoint row is missing.
        :raises Exception: Input/ID validation, schema-link resolution, and database failures propagate.
        """


__all__ = ["CommentRepositoryAPI"]
