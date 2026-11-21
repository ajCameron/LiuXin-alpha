"""
Coordinate shared metadata reads and writes with the bound metadata owner.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.caches.api import CacheQuery, CacheSort
from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.api.metadata_shared import (
    SharedMetadataKind,
    SharedMetadataRecord,
)
from LiuXin_alpha.catalog.repositories.agents import AgentRepository
from LiuXin_alpha.catalog.repositories.base import BaseRepository
from LiuXin_alpha.catalog.repositories.work_memberships import WorkMembershipRepository

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI
    from LiuXin_alpha.catalog.api.metadata_access import MetadataWriteReceipt


class SharedMetadata:
    """
    Expose public aliases without leaking repository objects or live rows.

    Example:
        ``metadata.vocabulary("tags").require(tag_id)`` returns detached public aliases.
    """

    def __init__(
        self, owner: MetadataAPI, repository: BaseRepository, kind: SharedMetadataKind
    ) -> None:
        """
        Bind shared-record operations to one Catalog repository.

        Example:
            The tags service binds the Tag repository and labels returned records as tags.


        :param owner: Metadata service that owns attachment checks, transaction
            coordination and cache freshness.
        :param repository: Repository providing validation, aliases and deletion policy.
        :param kind: Public shared-metadata family label for returned records.
        :return: None; retains the bindings without reading rows.
        """
        self._owner = owner
        self._repository = repository
        self._kind: SharedMetadataKind = kind

    def _record(self, row: Mapping[str, object]) -> SharedMetadataRecord:
        """
        Copy public aliases from a complete shared-record row.

        Example:
            Changing record.fields after a read does not persist shared metadata.


        :param row: Physical row containing the repository's integer identity column.
        :return: Detached SharedMetadataRecord; a malformed identity raises TypeError.
        """
        repository = self._repository
        entity_id = row[repository.id_column]
        if type(entity_id) is not int:
            raise TypeError("Shared metadata identity must be an integer")
        fields = {
            alias: deepcopy(row[column])
            for alias, column in repository.input_aliases.items()
            if column != repository.id_column and column in row
        }
        return SharedMetadataRecord(self._kind, entity_id, fields)

    def get(self, entity_id: int) -> SharedMetadataRecord | None:
        """
        Read a complete shared record, preserving known-miss semantics.

        Example:
            A missing tag does not trigger database fallback.


        :param entity_id: Shared-record identity; booleans and negative IDs are invalid.
        :return: Detached shared record, or None for a confirmed miss.
        """
        BaseRepository._validate_entity_id(entity_id)
        with self._owner._reading():
            row = self._owner._reads().get_row_from_id(
                self._repository.table_name, entity_id
            )
            return (
                None
                if row is None
                else self._record(cast(Mapping[str, object], row.row_dict))
            )

    def require(self, entity_id: int) -> SharedMetadataRecord:
        """
        Require an existing shared identity.

        Example:
            ``metadata.agents.require(agent_id)`` requires an existing Agent.


        :param entity_id: Shared-record identity; booleans and negative IDs are invalid.
        :return: Detached shared record; a confirmed miss raises CatalogNotFoundError.
        """
        value = self.get(entity_id)
        if value is None:
            raise CatalogNotFoundError(f"Missing {self._kind} identity: {entity_id}")
        return value

    def list(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """
        Read one complete ID-ordered page under the owner's cache lock.

        Example:
            ``metadata.vocabulary("tags").list(limit=20)`` reads a complete first page.


        :param limit: Maximum shared records; zero returns an empty page.
        :param offset: Number of ascending identities to skip.
        :return: Tuple of detached shared records in identity order.
        """
        BaseRepository._validate_page(limit, offset)
        with self._owner._reading():
            rows = self._owner._reads().rows(
                CacheQuery(
                    table=self._repository.table_name,
                    sort=(CacheSort(self._repository.id_column),),
                    limit=limit,
                    offset=offset,
                ),
                lambda: self._repository.list(limit=limit, offset=offset),
            )
            return tuple(self._record(row) for row in rows)

    def create(self, data: Mapping[str, object]) -> MetadataWriteReceipt[int]:
        """
        Create through the owning Catalog repository and reconcile once.

        Example:
            ``metadata.vocabulary("tags").create({"name": "Science"})`` creates one value.


        :param data: Repository public aliases; omission preserves values on update.
        :return: Receipt containing the new shared identity.
        """
        return self._owner._write(lambda: (self._repository.create(data), True))

    def update(
        self, entity_id: int, data: Mapping[str, object]
    ) -> MetadataWriteReceipt[int]:
        """
        Apply supplied public aliases and refresh affected projections.

        Example:
            A renamed tag is reconciled into dependent cached metadata.


        :param entity_id: Existing shared identity.
        :param data: Repository public aliases; omission preserves values on update.
        :return: Receipt containing the edited shared identity.
        """

        def operation() -> tuple[int, bool]:
            """
            Apply the shared-record payload through its repository.

            Example:
                An empty payload reports changed=False after repository validation.


            :return: Edited identity and bool(data); equal supplied values may still
                report a change.
            """
            self._repository.update(entity_id, data)
            return entity_id, bool(data)

        return self._owner._write(operation)

    def delete(self, entity_id: int) -> MetadataWriteReceipt[int]:
        """
        Delegate deletion policy and reconcile dependent cached metadata.

        Example:
            Deletion constraints on a referenced shared value still propagate.


        :param entity_id: Existing shared identity to delete under repository policy.
        :return: Receipt containing the deleted shared identity.
        """

        def operation() -> tuple[int, bool]:
            """
            Delegate shared-record deletion before cache publication.

            Example:
                A database constraint failure escapes without reporting a successful receipt.


            :return: Deleted identity and True after successful repository deletion.
            """
            self._repository.delete(entity_id)
            return entity_id, True

        return self._owner._write(operation)


class AgentMetadata(SharedMetadata):
    """
    Retain Agent-specific aggregate creation and name policy in Catalog.

    Example:
        ``metadata.agents.rename(agent_id, "New name")`` preserves the current sort name.
    """

    def __init__(self, owner: MetadataAPI, repository: AgentRepository) -> None:
        """
        Bind core Agent reads and aggregate-aware mutation methods.

        Example:
            Person and organisation creation reuse the same cache coordinator.


        :param owner: Metadata service that owns attachment checks, transaction
            coordination and cache freshness.
        :param repository: Agent repository with person/organisation and rename
            operations.
        :return: None; retains the Agent repository under the shared-record binding.
        """
        super().__init__(owner, repository, "agents")
        self._agents = repository

    def create_person(self, data: Mapping[str, object]) -> MetadataWriteReceipt[int]:
        """
        Create the person aggregate, then reconcile the metadata cache.

        Example:
            ``metadata.agents.create_person({"name": "Example name"})`` creates the aggregate.


        :param data: Agent aggregate payload validated by the Agent repository.
        :return: Receipt containing the new person's Agent identity.
        """
        return self._owner._write(lambda: (self._agents.create_person(data), True))

    def create_organisation(
        self, data: Mapping[str, object]
    ) -> MetadataWriteReceipt[int]:
        """
        Create the organisation aggregate, then reconcile the metadata cache.

        Example:
            ``metadata.agents.create_organisation({"name": "Example name"})`` creates the aggregate.


        :param data: Agent aggregate payload validated by the Agent repository.
        :return: Receipt containing the new organisation's Agent identity.
        """
        return self._owner._write(
            lambda: (self._agents.create_organisation(data), True)
        )

    def rename(
        self, entity_id: int, name: str, *, derive_sort: bool = False
    ) -> MetadataWriteReceipt[int]:
        """
        Apply Catalog's name/sort policy without dropping contributions.

        Nested use requires the serialized outer transaction supplied by metadata.batch
        or an explicitly serialized caller-owned transaction.

        Example:
            ``metadata.agents.rename(agent_id, "Le Guin, Ursula", derive_sort=True)`` updates name policy.


        :param entity_id: Existing Agent identity.
        :param name: Replacement display name validated by Catalog.
        :param derive_sort: Derive a new sort name when true; preserve the existing one
            otherwise.
        :return: Receipt containing the Agent identity and Catalog's known change flag.
        """
        if type(derive_sort) is not bool:
            raise TypeError("derive_sort must be a boolean")

        def operation() -> tuple[int, bool]:
            """
            Rename through Catalog with the selected sort-name policy.

            Example:
                Preserving the same name and sort policy can produce a known no-op.


            :return: Agent identity and Catalog's explicit changed flag.
            """
            result = self._agents.rename(
                entity_id, name, sort_mode="derive" if derive_sort else "preserve"
            )
            return entity_id, bool(result["changed"])

        return self._owner._write(operation)


class VocabularyMetadata(SharedMetadata):
    """
    Coordinate exact-value CRUD and Tag/Series membership changes.

    Example:
        ``metadata.vocabulary("series").link_to_work(...)`` retains membership metadata.
    """

    def _membership(
        self, entity_id: int, work_id: int, *, unlink: bool
    ) -> MetadataWriteReceipt[int]:
        """
        Coordinate a Tag/Series membership change and detect an existing no-op.

        Example:
            Linking an already-linked tag returns changed=False.


        :param entity_id: Tag or Series identity.
        :param work_id: Work identity receiving or losing the membership.
        :param unlink: Remove the association when true; otherwise ensure it exists.
        :return: Receipt containing the vocabulary identity; unsupported vocabularies
            raise ValueError.
        """
        repository = self._repository
        if not isinstance(repository, WorkMembershipRepository):
            raise ValueError("Work membership is supported for tags and series")

        def operation() -> tuple[int, bool]:
            """
            Inspect and update the membership within the same owned transaction.

            Example:
                An existing association keeps its metadata when link_to_work is repeated.


            :return: Vocabulary identity paired with whether the requested membership
                changed.
            """
            linked = repository.is_linked_to_work(entity_id=entity_id, work_id=work_id)
            method = repository.unlink_from_work if unlink else repository.link_to_work
            method(entity_id=entity_id, work_id=work_id)
            return entity_id, linked if unlink else not linked

        return self._owner._write(operation)

    def link_to_work(
        self, *, entity_id: int, work_id: int
    ) -> MetadataWriteReceipt[int]:
        """
        Attach a Tag or Series without replacing retained link metadata.

        Example:
            Repeated attachment preserves the existing association metadata.


        :param entity_id: Existing Tag or Series identity.
        :param work_id: Existing Work identity.
        :return: Receipt containing the vocabulary identity and membership change flag.
        """
        return self._membership(entity_id, work_id, unlink=False)

    def unlink_from_work(
        self, *, entity_id: int, work_id: int
    ) -> MetadataWriteReceipt[int]:
        """
        Remove a Tag/Series membership while retaining both entities.

        Example:
            Detaching a tag retains both the tag and the Work.


        :param entity_id: Existing Tag or Series identity.
        :param work_id: Existing Work identity.
        :return: Receipt containing the vocabulary identity and membership change flag.
        """
        return self._membership(entity_id, work_id, unlink=True)
