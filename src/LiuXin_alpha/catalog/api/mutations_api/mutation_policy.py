"""
Describe preliminary mutation policy without promising final write validity.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, WemiLevel


@runtime_checkable
class MutationPolicyAPI(Protocol):
    """
    Provide read-only preliminary shape and existence checks for WEMI mutations.

    These checks may query repositories and raise database/ID errors. They do
    not normalize payloads, reserve entities or guarantee that a writer succeeds.

    Example:
        Call can_update before displaying an operation, then handle validation or
        storage errors from the actual write independently.
    """

    def can_create(self, *, level: WemiLevel, data: RowInput) -> bool:
        """
        Check only that a WEMI level and nonempty mapping are supplied.

        Example:
            A mapping containing only an unknown column can pass this preliminary check.


        :param level: WEMI level selecting the semantic repository.
        :param data: Proposed payload; contents are not normalized.
        :return: True for a known level with a nonempty Mapping.
        """

    def can_update(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> bool:
        """
        Check a nonempty mapping and look up an integer-ID target.

        Lookup failures propagate. A positive result neither reserves the Row nor
        predicts success of later field/relationship validation.

        Example:
            An empty mapping is rejected before any repository lookup.


        :param level: WEMI level selecting the semantic repository.
        :param entity_id: Integer ID excluding bool; sign/existence checks are delegated.
        :param data: Proposed mapping; individual fields are not validated.
        :return: False for rejected shape/type or absent target; True when its Row exists.
        """

    def can_merge(self, *, level: WemiLevel, source_id: EntityId, target_id: EntityId) -> bool:
        """
        Check distinct integer IDs and read both same-level entities.

        No semantic equivalence, relationship support or value-conflict checks are
        performed. Sign validation and lookup errors belong to the repositories;
        results do not lock or reserve either Row.

        Example:
            If source lookup returns None, target lookup is short-circuited.


        :param level: WEMI level selecting the semantic repository.
        :param source_id: Source ID, excluding bool.
        :param target_id: Target ID, excluding bool.
        :return: True when both Rows exist; otherwise False for rejected inputs or missing Rows.
        """
