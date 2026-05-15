"""Shared WEMI metadata relation-helper API."""
from __future__ import annotations

import abc
from collections.abc import Iterable
from typing import Generic, TypeVar

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    RelationCardinality,
    RelationLink,
    RelationLinkID,
    select_primary_relation_link,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    RelationTarget,
)

RelationKeyT = TypeVar("RelationKeyT", bound=str)
RelationTargetT = TypeVar("RelationTargetT", bound=RelationTarget)
RelationLinkT = TypeVar("RelationLinkT", bound=RelationLink[RelationTargetT])


class WemiMetadataRelationsAPI(Generic[RelationKeyT, RelationTargetT, RelationLinkT], abc.ABC):
    """Shared helpers for relation-keyed WEMI metadata bundles."""

    RELATION_LINK_CLASS: type[RelationLinkT]

    @classmethod
    @abc.abstractmethod
    def relation_names(cls) -> tuple[RelationKeyT, ...]:
        """Relation keys this metadata bundle can expose."""

    @classmethod
    @abc.abstractmethod
    def validate_relation_name(cls, relation_key: str) -> RelationKeyT:
        """Normalize and validate one relation key."""

    @classmethod
    @abc.abstractmethod
    def relation_cardinality(cls, relation_key: RelationKeyT) -> RelationCardinality:
        """Return the cardinality policy for one relation key."""

    @classmethod
    @abc.abstractmethod
    def validate_relation_links(
        cls,
        relation_key: RelationKeyT,
        links: Iterable[RelationLinkT],
    ) -> list[RelationLinkT]:
        """Validate relation links for one relation key."""

    @abc.abstractmethod
    def get_relation_links(self, relation_key: RelationKeyT) -> list[RelationLinkT]:
        """Get relation links for one relation key."""

    @abc.abstractmethod
    def set_relation_links(self, relation_key: RelationKeyT, links: Iterable[RelationLinkT]) -> None:
        """Replace relation links for one relation key."""

    def add_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """Add one relation link for a relation key."""

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(
            relation_key,
            self.validate_relation_links(relation_key, links),
        )

    def remove_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> bool:
        """Remove one relation link for a relation key, if it exists."""

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation_key: RelationKeyT) -> list[RelationTargetT]:
        """Get related targets for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        return [link.target for link in self.get_relation_links(relation_key)]

    def get_all_related(self) -> dict[RelationKeyT, list[RelationTargetT]]:
        """Return related targets grouped by relation key."""

        return {
            relation_key: list(self.get_related(relation_key))
            for relation_key in self.relation_names()
        }

    def primary_relation_link(self, relation_key: RelationKeyT) -> RelationLinkT | None:
        """Return the preferred relation link for one relation key, if any."""

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: RelationKeyT) -> RelationTargetT | None:
        """Return the preferred relation target for one relation key, if any."""

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """Mark one relation link as the preferred link for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        selected_index: int | None = None
        for index, existing_link in enumerate(links):
            same_link_id = link.link_id is not None and existing_link.link_id == link.link_id
            same_target = link.link_id is None and existing_link.target == link.target
            if existing_link is link or same_link_id or same_target:
                selected_index = index
                links[index] = link
                break

        if selected_index is None:
            selected_index = len(links)
            links.append(link)

        for index, existing_link in enumerate(links):
            existing_link.primary = index == selected_index
        self.set_relation_links(relation_key, links)

    def set_related(self, relation_key: RelationKeyT, values: Iterable[RelationTargetT]) -> None:
        """Replace related targets for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(
            relation_key,
            [
                self.RELATION_LINK_CLASS(
                    target=value,
                    cardinality=self.relation_cardinality(relation_key),
                )
                for value in values
            ],
        )

    def add_related(self, relation_key: RelationKeyT, value: RelationTargetT) -> None:
        """Add one related target for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        self.add_relation_link(
            relation_key,
            self.RELATION_LINK_CLASS(
                target=value,
                cardinality=self.relation_cardinality(relation_key),
            ),
        )

    def get_relation_link_by_id(
        self,
        relation_key: RelationKeyT,
        link_id: RelationLinkID,
    ) -> RelationLinkT | None:
        """Return a relation link by ID, if present."""

        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    def upsert_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """Upsert a relation link by ID when it already exists."""

        relation_key = self.validate_relation_name(relation_key)
        if link.link_id is None:
            self.add_relation_link(relation_key, link)
            return

        links = list(self.get_relation_links(relation_key))
        for index, existing_link in enumerate(links):
            if existing_link.link_id == link.link_id:
                links[index] = link
                self.set_relation_links(relation_key, links)
                return
        self.add_relation_link(relation_key, link)

    def remove_relation_link_by_id(
        self,
        relation_key: RelationKeyT,
        link_id: RelationLinkID,
    ) -> bool:
        """Remove a relation link by ID, if present."""

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        for index, link in enumerate(links):
            if link.link_id == link_id:
                del links[index]
                self.set_relation_links(relation_key, links)
                return True
        return False

    def clear_related(self, relation_key: RelationKeyT) -> None:
        """Clear all relation links for one relation key."""

        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])


__all__ = ["WemiMetadataRelationsAPI"]
