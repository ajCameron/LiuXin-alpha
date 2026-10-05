"""
Define common helpers for editable relation-keyed metadata bundles.

Concrete bundles supply supported keys, cardinality validation and bucket storage.
Helpers copy lists while retaining shared link and target objects.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
    >>> metadata = WorkMetadata()
    >>> link = RelationLink(target='History', link_id=1)
    >>> metadata.add_relation_link('tags', link)
    >>> metadata.get_related('tags')
    ['History']
"""
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
    """
    Combine abstract bucket policy with link and target editing helpers.

    RELATION_LINK_CLASS supplies wrappers for bare targets. Link-object metadata remains
    shared; primary selection mutates flags before setter validation and has no
    rollback.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
        >>> metadata = WorkMetadata()
        >>> isinstance(metadata, WemiMetadataRelationsAPI)
        True
    """

    RELATION_LINK_CLASS: type[RelationLinkT]

    @classmethod
    @abc.abstractmethod
    def relation_names(cls) -> tuple[RelationKeyT, ...]:
        """
        Require the supported canonical relation keys in the bundle's declared order.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> 'tags' in metadata.relation_names()
            True


        :return: Tuple of supported relation names.
        """

    @classmethod
    @abc.abstractmethod
    def validate_relation_name(cls, relation_key: str) -> RelationKeyT:
        """
        Require normalization and validation of a relation name.

        Alias support and rejection behavior belong to the concrete bundle.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.validate_relation_name('tag')
            'tags'


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: Canonical relation key.
        """

    @classmethod
    @abc.abstractmethod
    def relation_cardinality(cls, relation_key: RelationKeyT) -> RelationCardinality:
        """
        Require the cardinality policy for one relation bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.relation_cardinality('tags').allows_many_targets
            True


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: RelationCardinality describing the local target policy.
        """

    @classmethod
    @abc.abstractmethod
    def validate_relation_links(
        cls,
        relation_key: RelationKeyT,
        links: Iterable[RelationLinkT],
    ) -> list[RelationLinkT]:
        """
        Require validation and materialization of a relation-link iterable.

        Concrete policy determines multiplicity and any additional checks.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.validate_relation_links('tags', [])
            []


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param links: Iterable of links to validate.
        :return: Validated list of shared link objects.
        """

    @abc.abstractmethod
    def get_relation_links(self, relation_key: RelationKeyT) -> list[RelationLinkT]:
        """
        Require access to the links stored for one relation.

        Whether the returned list is live or copied depends on the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.get_relation_links('tags')[0] is link
            True


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: List of relation-link objects.
        """

    @abc.abstractmethod
    def set_relation_links(self, relation_key: RelationKeyT, links: Iterable[RelationLinkT]) -> None:
        """
        Require replacement of one relation bucket under its validation policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.set_relation_links('tags', [])
            >>> metadata.get_related('tags')
            []


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param links: Iterable of replacement links.
        :return: None.
        """

    def add_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """
        Append a shared link through explicit validation and the concrete setter.

        Normalize the key, copy the existing list, append and validate before assignment. No
        id or target deduplication is performed.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.add_relation_link('tags', link)
            >>> len(metadata.get_relation_links('tags'))
            2


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link: Relation-link object retained by reference.
        :return: None.
        """

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        links.append(link)
        self.set_relation_links(
            relation_key,
            self.validate_relation_links(relation_key, links),
        )

    def remove_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> bool:
        """
        Remove the first equal link from a copied bucket list.

        List removal uses equality, not only object identity. ValueError from removal or
        from the setter is caught and reported as False; other failures propagate. Setter
        side effects, if any, are not rolled back.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.remove_relation_link('tags', RelationLink(target='History', link_id=1))
            True
            >>> metadata.remove_relation_link('tags', link)
            False


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link: Link compared by equality against stored entries.
        :return: True after successful replacement, or False for a caught ValueError.
        """

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        try:
            links.remove(link)
            self.set_relation_links(relation_key, links)
            return True
        except ValueError:
            return False

    def get_related(self, relation_key: RelationKeyT) -> list[RelationTargetT]:
        """
        Collect target references from a validated relation bucket.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.get_related('tags')
            ['History']


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: New list containing shared targets in bucket order.
        """

        relation_key = self.validate_relation_name(relation_key)
        return [link.target for link in self.get_relation_links(relation_key)]

    def get_all_related(self) -> dict[RelationKeyT, list[RelationTargetT]]:
        """
        Collect targets for every declared relation, including empty buckets.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.get_all_related()['tags']
            ['History']


        :return: New dictionary of new lists containing shared targets.
        """

        return {
            relation_key: list(self.get_related(relation_key))
            for relation_key in self.relation_names()
        }

    def primary_relation_link(self, relation_key: RelationKeyT) -> RelationLinkT | None:
        """
        Select the preferred shared link using primary, priority, index and input order.

        Selection delegates to select_primary_relation_link without changing flags or
        enforcing singleton cardinality.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.primary_relation_link('tags') is link
            True
            >>> link.primary is None
            True


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: Selected link, or None for an empty bucket.
        """

        relation_key = self.validate_relation_name(relation_key)
        return select_primary_relation_link(self.get_relation_links(relation_key))

    def primary_related(self, relation_key: RelationKeyT) -> RelationTargetT | None:
        """
        Return the target of the preferred relation link.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.primary_related('tags')
            'History'


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: Shared target, or None when no link exists.
        """

        link = self.primary_relation_link(relation_key)
        if link is None:
            return None
        return link.target

    def set_primary_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """
        Replace the first matching link or append it, then mark only that position primary.

        Match by object identity, by a non-None incoming link id, or by equal target when
        the incoming id is None. Matching replaces the link without merging metadata. All
        primary flags are mutated on shared objects before the setter runs, so a validation
        failure can leave those flag changes visible.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> replacement = RelationLink(target='Revised', link_id=1)
            >>> metadata.set_primary_relation_link('tags', replacement)
            >>> metadata.get_related('tags'), replacement.primary
            (['Revised'], True)


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link: Relation-link object retained by reference.
        :return: None.
        """

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
        """
        Replace a bucket with new link wrappers around the supplied targets.

        Each wrapper uses RELATION_LINK_CLASS and the relation's cardinality. Targets remain
        shared and duplicates are retained; setter validation governs assignment.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.set_related('tags', ['History', 'History'])
            >>> metadata.get_related('tags')
            ['History', 'History']


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param values: Iterable of target values in replacement order.
        :return: None.
        """

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
        """
        Wrap a target with the relation cardinality and append it through add_relation_link.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> metadata.add_related('tags', 'History')
            >>> metadata.get_related('tags')
            ['History']


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param value: Target retained by reference in a new link wrapper.
        :return: None.
        """

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
        """
        Find the first stored link whose id compares equal to the supplied id.

        Key handling is delegated to get_relation_links; no id conversion occurs.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.get_relation_link_by_id('tags', 1) is link
            True


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link_id: Id compared directly against each link_id.
        :return: First matching shared link, or None.
        """

        for link in self.get_relation_links(relation_key):
            if link.link_id == link_id:
                return link
        return None

    def upsert_relation_link(self, relation_key: RelationKeyT, link: RelationLinkT) -> None:
        """
        Replace the first equal non-None link id in place, otherwise append.

        An incoming link without an id always appends, even when its target matches.
        Replacement preserves the list position and uses the setter; metadata is not merged.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.upsert_relation_link('tags', RelationLink(target='Revised', link_id=1))
            >>> metadata.get_related('tags')
            ['Revised']


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link: Relation-link object retained by reference.
        :return: None.
        """

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
        """
        Remove the first link with an equal id and replace the bucket.

        No id conversion occurs. Setter failures, including ValueError, propagate.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.remove_relation_link_by_id('tags', 1)
            True
            >>> metadata.remove_relation_link_by_id('tags', 1)
            False


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :param link_id: Id compared directly against each link_id.
        :return: True after removal, or False when no matching id exists.
        """

        relation_key = self.validate_relation_name(relation_key)
        links = list(self.get_relation_links(relation_key))
        for index, link in enumerate(links):
            if link.link_id == link_id:
                del links[index]
                self.set_relation_links(relation_key, links)
                return True
        return False

    def clear_related(self, relation_key: RelationKeyT) -> None:
        """
        Normalize the relation key and replace its bucket with an empty list.

        The concrete setter still controls validation and errors.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import WorkMetadata
            >>> metadata = WorkMetadata()
            >>> link = RelationLink(target='History', link_id=1)
            >>> metadata.add_relation_link('tags', link)
            >>> metadata.clear_related('tags')
            >>> metadata.get_related('tags')
            []


        :param relation_key: Relation key accepted by the concrete bundle; aliases follow
            its validation policy.
        :return: None.
        """

        relation_key = self.validate_relation_name(relation_key)
        self.set_relation_links(relation_key, [])


__all__ = ["WemiMetadataRelationsAPI"]
