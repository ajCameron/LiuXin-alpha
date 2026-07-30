"""Mutation policy API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, WemiLevel


@runtime_checkable
class MutationPolicyAPI(Protocol):
    """Side-effect-free preflight checks for semantic Catalog writes.

    A ``False`` result means the proposed operation is not currently supported
    or violates policy. It does not mutate or reserve anything; the writer
    validates again inside its transaction.
    """

    def can_create(self, *, level: WemiLevel, data: RowInput) -> bool:
        """Return whether ``data`` is sufficient and allowed for creation.

        :param level: WEMI level to create.
        :param data: Proposed public values.
        :return: ``True`` when the semantic writer may attempt creation.
        """

    def can_update(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> bool:
        """Return whether an existing entity may receive ``data``.

        :param level: WEMI level containing ``entity_id``.
        :param entity_id: Proposed update target.
        :param data: Proposed scalar/relationship changes.
        :return: ``True`` when the semantic writer may attempt the update.
        """

    def can_merge(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> bool:
        """Return whether source and target are eligible for a merge.

        :param level: Shared WEMI level.
        :param source_id: Entity proposed for absorption/deletion.
        :param target_id: Entity proposed as canonical target.
        :return: ``True`` when a merge may be attempted.
        """
