"""Repository base for exact-default catalog entities."""

from __future__ import annotations

from typing import ClassVar

from ..api.common import (
    CatalogMutationError,
    EntityId,
    MatchResult,
    MetadataCandidate,
    RowInput,
    RowMapping,
)
from ..matching.exact_matcher import ExactEntityMatcher, ExactEntitySpec
from ..matching.policy import normalise_match_text, raise_for_unresolved
from .base import BaseRepository


class ExactEntityRepository(BaseRepository):
    """Provide CRUD and exact-default matching for a configured entity."""

    match_spec: ClassVar[ExactEntitySpec]

    def _normalise_storage_input(self, data: RowInput) -> dict[str, object]:
        """Map aliases and derive values required only for persistence.

        :param data: Caller-supplied entity values.
        :return: Plain storage-column mapping with derived normalized values.
        """

        result = super().normalise_input(data)
        for destination, source in self.match_spec.normalized_storage_fields:
            source_value = result.get(source)
            if source_value is not None and destination not in result:
                result[destination] = normalise_match_text(source_value)
        return result

    def _require_mutable(self) -> None:
        if not self.match_spec.mutable:
            raise CatalogMutationError(
                f"{self.match_spec.entity_name} rows are read-only catalog constants"
            )

    def create(self, data: RowInput) -> EntityId:
        """Create a mutable entity row.

        :param data: Public aliases or storage-column values.
        :return: Newly created entity ID.
        :raises CatalogMutationError: If the entity table is read-only.
        """

        self._require_mutable()
        return super().create(self._normalise_storage_input(data))

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """Update a mutable entity row.

        :param entity_id: Entity to update.
        :param data: Public aliases or storage-column values.
        :return: None.
        :raises CatalogMutationError: If the entity table is read-only.
        """

        self._require_mutable()
        super().update(entity_id, self._normalise_storage_input(data))

    def delete(self, entity_id: EntityId) -> None:
        """Delete a mutable entity row.

        :param entity_id: Entity to delete.
        :return: None.
        :raises CatalogMutationError: If the entity table is read-only.
        """

        self._require_mutable()
        super().delete(entity_id)

    def matcher(self) -> ExactEntityMatcher:
        """Return a matcher sharing this repository's configured policy.

        :return: Exact-default entity matcher.
        """

        return ExactEntityMatcher(self, self.match_spec, self.matching_policy)

    def match(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """Return an exact-default match decision.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit approximate matching after exact matching fails.
        :return: Match, no-match, ambiguity, or conflict decision.
        """

        return self.matcher().best(candidate, use_policy=use_policy)

    def exact(self, value: object, **scope: object) -> MatchResult:
        """Return the exact decision for a scalar identity value.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases.
        :return: Exact match, no-match, or ambiguity decision.
        """

        return self.matcher().exact(value, **scope)

    def resolve(self, value: object, **scope: object) -> RowMapping | None:
        """Return one uniquely exact row for a scalar identity value.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases.
        :return: Matching row, or ``None`` when not uniquely matched.
        """

        result = self.exact(value, **scope)
        return result.candidate if result.is_match else None

    def match_or_create(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> EntityId:
        """Reuse a permitted match or create only on a genuine non-match.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit explicitly requested approximate reuse.
        :return: Existing or newly created entity ID.
        :raises CatalogMutationError: If this entity is read-only or unsafe to
            reuse globally.
        :raises CatalogAmbiguousMatchError: If several entities match.
        :raises CatalogMatchConflictError: If exact identity fields conflict.
        """

        self._require_mutable()
        if not self.match_spec.reusable:
            raise CatalogMutationError(
                f"{self.match_spec.entity_name} rows require contextual creation"
            )
        result = self.match(candidate, use_policy=use_policy)
        if result.is_match:
            assert result.entity_id is not None
            return result.entity_id
        raise_for_unresolved(result)
        return self.create(candidate.data)


__all__ = ["ExactEntityRepository"]
