"""
Declare Annotation repository persistence, identity, and contextual read/write contracts.

The protocol extends ExactEntityRepositoryAPI; entity-specific mutation/reuse rules
and relationship behavior belong to the concrete repository. Runtime protocol checks
establish member presence rather than signatures, schema support, or atomicity.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowMapping
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class AnnotationRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """
    Store and inspect Annotations with required Item scope for identity matching.

    ANNOTATION_SPEC configures annotations/annotation_id and public aliases for Item, user, kind,
    anchor type/start/end, source, selected/note text, device, and extra JSON. Candidate matching
    requires non-None item_id, kind, anchor_type, and anchor_start; supplied user/end/source values
    also constrain identity. Kind, anchor type, and source comparisons case-fold. Scalar exact uses
    only anchor start plus required Item scope, without the full candidate-identity check.

    Presence checks in matching do not validate Item existence; list_for_item does. Mutable direct
    CRUD is available, but inherited match_or_create rejects this non-reusable spec before lookup.
    No approximate policy field is configured.

    This structural protocol describes the concrete repository behavior and adds no runtime
    implementation, validation, or transaction handling of its own.

    Example:
        >>> repository: AnnotationRepositoryAPI = catalog.annotations  # doctest: +SKIP
    """

    def list_for_item(
        self,
        item_id: EntityId,
        *,
        user_id: EntityId | None = None,
        kind: str | None = None,
    ) -> Sequence[RowMapping]:
        """
        Read one Item's Annotations with optional direct user and kind filters.

        Require the Item before validating optional filters. A supplied user_id must pass the strict
        nonnegative/non-boolean integer check, but no user row is fetched. A supplied kind must be a
        string with nonblank stripped content; strip its outer whitespace and pass it to the
        database without case-folding or matching-policy normalization. None means no constraint for
        either filter, not a request to find null-valued columns.

        Ask macros for ascending annotation_id order and shallow-copy all returned rows. No
        _catalog_link metadata, pagination, global reuse, or persistence is added. The owner read
        and annotation query are not enclosed in one snapshot.

        Example:
            >>> rows = catalog.annotations.list_for_item(item_id, user_id=11, kind="highlight")  # doctest: +SKIP


        :param item_id: Nonnegative non-boolean integer ID of the Item required first.
        :param user_id: Optional validated integer user ID used as a direct column filter, without existence checking.
        :param kind: Optional nonblank string stripped before applying a direct annotation_kind filter.
        :return: Tuple of Annotation row dictionaries in macro-provided ID order.
        :raises CatalogNotFoundError: If the Item is missing.
        :raises ValueError: If kind is supplied but is not a nonblank string, or a validated ID is negative.
        :raises TypeError: If a supplied ID is not an integer or is a boolean.
        :raises Exception: Database/schema access and row-conversion failures propagate.
        """


__all__ = ["AnnotationRepositoryAPI"]
