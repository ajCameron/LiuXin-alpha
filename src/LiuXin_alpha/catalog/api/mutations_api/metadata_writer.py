"""
Define coordinated WEMI and semantic metadata writer contracts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from ..common import CreatedWemiStack, EntityId, RowInput, WemiLevel


@runtime_checkable
class MetadataWriterAPI(Protocol):
    """
    Coordinate WEMI creation, links and selected metadata attachment/merge operations.

    Explicit graph/replacement methods require macro transactions. Attachment
    and merge methods also support legacy handles without a callable transaction,
    where atomicity is not guaranteed. Policy checks are preliminary rather than
    a complete validation of all repository payloads.

    Example:
        Attach a title and role-bearing Agent mapping through
        ``catalog.mutations.writer.attach_metadata`` for coordinated repository writes.
    """

    def create_wemi_stack(
        self,
        *,
        work: RowInput,
        expression: RowInput,
        manifestation: RowInput,
        items: Sequence[RowInput] = (),
        origin: str | None = None,
        work_id: EntityId | None = None,
    ) -> CreatedWemiStack:
        """
        Create a linked WEMI path inside a macro transaction.

        Validate work_id, look up its current row, check payloads/items and origin
        before the transaction. Existing-row lookup is not protected by that later
        transaction. Create/update the Work, then preferred priority-zero graph
        links and Items. Item manifestation_id takes precedence over
        item_manifestation_id; the selected value must be None or the new ID, and
        output always uses the new Manifestation. Late failures roll back through
        the macro transaction. Replaced links do not delete former descendant rows.

        Example:
            Supplying work_id replaces that Work's Expression links with the new preferred Expression.


        :param work: Work creation/update payload; empty permitted only for an existing work_id.
        :param expression: Nonempty Expression mapping to create.
        :param manifestation: Nonempty Manifestation mapping to create.
        :param items: Sequence of nonempty Item mappings, shallow-copied before mutation.
        :param origin: Optional string provenance required to fit both graph-link schemas.
        :param work_id: Optional nonnegative integer, excluding bool; reuse/update or explicitly insert this ID.
        :return: CreatedWemiStack containing Work, Expression, Manifestation and Item IDs.
        :raises TypeError: work_id, items container or origin has an invalid type/value.
        :raises CatalogMutationError: Required data is empty, an Item targets another Manifestation,
            an explicit Work ID is not preserved, or link metadata cannot be represented.
        """

    def attach_metadata(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> None:
        """
        Apply direct fields and attachment groups using an available macro transaction.

        Reserve fields, title/titles, agents, identifiers and notes. Apply direct
        fields first, then titles, Agents, identifiers and Notes; title precedes
        entries in titles. If macros.transaction is callable, all writes use it;
        otherwise the null context permits partial effects. Preflight checks shapes
        and selected existing IDs, but deeper normalization may fail after writes.

        Example:
            An explicit fields mapping overrides same-named unreserved top-level fields.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing entity ID.
        :param data: Nonempty mapping of direct fields and semantic attachment groups.
        :return: None; updates the entity and selected attachments.
        :raises CatalogMutationError: Policy or an attachment shape/value is rejected.
        """

    def replace_metadata(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> None:
        """
        Replace explicitly supplied groups inside a macro transaction.

        Unlike attachment, identifiers must be a scheme/value mapping (None means
        empty). Title accepts at most one string/mapping or None to clear. Replace
        title before direct fields so explicit fields win. Agents are deduplicated
        by ID within stripped roles; existing roles absent from input are cleared,
        and supplied priority values are ignored in favor of replacement order.
        Notes, comments and synopses use their repository replacement contracts.
        Policy and some validation happen before the transaction; deeper errors
        inside it roll back earlier group writes.

        Example:
            ``{"agents": [], "identifiers": {}}`` clears those groups while leaving Notes unchanged.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Existing entity ID.
        :param data: Nonempty mapping of direct fields and semantic attachment groups.
        :return: None; omitted groups remain unchanged.
        :raises CatalogMutationError: Policy, fields or group shape is rejected.
        """

    def link_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
        primary: bool | None = None,
        priority: int | None = None,
        origin: str | None = None,
    ) -> Mapping[str, object]:
        """
        Link existing adjacent entities and reconcile primary status transactionally.

        Validate level pair and optional metadata before opening a transaction;
        require both endpoints inside it. For link tables, a new primary demotes
        other primary children of this parent. For Items, primary=False is invalid;
        the foreign key may reassign ownership from another Manifestation.

        Example:
            Linking an Item changes its sole Manifestation foreign key; priority/origin are unsupported there.


        :param parent_level: Parent WEMI level.
        :param parent_id: Existing parent ID, validated by its repository inside the transaction.
        :param child_level: Immediate child WEMI level.
        :param child_id: Existing child ID, validated by its repository inside the transaction.
        :param primary: True demotes sibling links; None preserves an existing flag or selects a first primary.
        :param priority: Optional integer priority, excluding bool; negative values are accepted locally.
        :param origin: Optional string origin; None omits an explicit origin update.
        :return: Receipt with parent/child levels and IDs plus authoritative link metadata.
        :raises TypeError: Optional primary, priority or origin has an invalid type.
        :raises CatalogMutationError: Levels, Item link metadata or required marker columns are unsupported.
        """

    def unlink_wemi(
        self,
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
    ) -> bool:
        """
        Remove an adjacent relationship while retaining both entity rows.

        Require both endpoints inside the macro transaction, even when unrelated.
        Item ownership is cleared only if its foreign key matches the parent. Other
        relations are replaced with the remaining links and writable extra fields.
        Failure to discover the link ID column is ignored.

        Example:
            Removing a primary Expression link does not promote a remaining sibling.


        :param parent_level: Parent WEMI level.
        :param parent_id: Existing parent ID, validated by its repository inside the transaction.
        :param child_level: Immediate child WEMI level.
        :param child_id: Existing child ID, validated by its repository inside the transaction.
        :return: True when the relation was removed; False when absent.
        :raises CatalogMutationError: The pair is not an adjacent downward WEMI relationship.
        """

    def merge_entities(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> None:
        """
        Fill missing target fields and transfer selected relationships before deleting the source.

        Use macros.transaction when callable, otherwise a null context. Only
        supported WEMI adjacency, Agent/Note links and curated identifiers are
        explicitly transferred; this is not a complete merge of every metadata
        family. Target nonempty fields and existing link identities win. Unsupported
        link specifications are skipped. Deletion/cascades follow repository policy.

        Example:
            Merging Manifestations reassigns source Items to the target.


        :param level: WEMI level selecting the semantic repository.
        :param source_id: Existing entity to absorb and delete.
        :param target_id: Distinct existing entity to retain.
        :return: None; retains target identity and deletes the source on success.
        :raises CatalogMutationError: Policy rejects level, IDs or entity existence.
        """
