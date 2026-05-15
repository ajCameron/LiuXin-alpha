"""Shared WEMI metadata relation-helper API."""
from __future__ import annotations

import abc
from typing import Generic, TypeVar


RelationKeyT = TypeVar("RelationKeyT", bound=str)
RelationTargetT = TypeVar("RelationTargetT")


class WemiMetadataRelationsAPI(Generic[RelationKeyT, RelationTargetT], abc.ABC):
    """Shared read helpers for relation-keyed WEMI metadata bundles."""

    @classmethod
    @abc.abstractmethod
    def relation_names(cls) -> tuple[RelationKeyT, ...]:
        """Relation keys this metadata bundle can expose."""

    @abc.abstractmethod
    def get_related(self, relation_key: RelationKeyT) -> list[RelationTargetT]:
        """Get related targets for one relation key."""

    def get_all_related(self) -> dict[RelationKeyT, list[RelationTargetT]]:
        """Return related targets grouped by relation key."""

        return {
            relation_key: list(self.get_related(relation_key))
            for relation_key in self.relation_names()
        }


__all__ = ["WemiMetadataRelationsAPI"]
