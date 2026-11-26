"""
Coordinate shared metadata reads and writes with the bound metadata owner.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from copy import deepcopy
from typing import TYPE_CHECKING, cast

from LiuXin_alpha.caches.metadata.reads import MetadataReadSource
from LiuXin_alpha.catalog.api.common import CatalogInUseError
from LiuXin_alpha.catalog.api.metadata_revisions import (
    SharedMetadataReference,
    SharedMetadataRevision,
    SharedMetadataSnapshot,
)
from LiuXin_alpha.catalog.api.metadata_shared import (
    SharedMetadataKind,
)
from LiuXin_alpha.catalog.metadata_revisions import shared_revision
from LiuXin_alpha.catalog.repositories.agents import AgentRepository
from LiuXin_alpha.catalog.repositories.base import BaseRepository
from LiuXin_alpha.catalog.repositories.work_memberships import WorkMembershipRepository

if TYPE_CHECKING:
    from LiuXin_alpha.caches.metadata.service import MetadataAPI
    from LiuXin_alpha.catalog.api.metadata_access import MetadataWriteReceipt


from LiuXin_alpha.caches.metadata.record_reads import (
    SharedMetadataReader,
    SuggestibleMetadataReader,
)


class SharedMetadata(SharedMetadataReader):
    """Coordinate mutable shared-record operations through Catalog."""

    def __init__(
        self, owner: MetadataAPI, repository: BaseRepository, kind: SharedMetadataKind
    ) -> None:
        """Register authoritative revision reads alongside mutable record operations."""
        super().__init__(owner, repository, kind)
        owner._shared_revision_readers[kind] = self._snapshot
        owner._shared_tables[kind] = repository.table_name

    def _snapshot(self, entity_id: int) -> SharedMetadataSnapshot:
        """Read a complete authoritative row and copy all public scalar aliases."""
        BaseRepository._validate_entity_id(entity_id)
        source = MetadataReadSource(
            self._owner._database,
            None,
            allow_database_fallback=False,
            require_rows=True,
        )
        row = source.get_row_from_id(self._repository.table_name, entity_id)
        assert row is not None
        root = deepcopy(dict(cast(Mapping[str, object], row.row_dict)))
        value = self._record(root)
        return SharedMetadataSnapshot(
            value,
            shared_revision(SharedMetadataReference(self._kind, entity_id), root),
            deepcopy(value.fields),
        )

    def read_for_edit(self, entity_id: int) -> SharedMetadataSnapshot:
        """Read authoritative content without requiring the bound cache to be current."""
        with self._owner._lock():
            self._owner._assert_attached()
            macros = self._owner._database.macros
            with (
                nullcontext()
                if macros.current_transaction_connection() is not None
                else macros.read_transaction()
            ):
                with self._owner._schema_scope():
                    return self._snapshot(entity_id)

    def _expected(
        self, entity_id: int, token: SharedMetadataRevision | None
    ) -> Sequence[SharedMetadataRevision]:
        """Reject a precondition for another record before entering mutation."""
        if token is None:
            return ()
        if not isinstance(
            token, SharedMetadataRevision
        ) or token.reference != SharedMetadataReference(self._kind, entity_id):
            raise ValueError("Shared edit precondition belongs to a different record")
        return (token,)

    def create(self, data: Mapping[str, object]) -> MetadataWriteReceipt[int]:
        """
        Create through the owning Catalog repository and reconcile once.

        Example:
            ``metadata.vocabulary("tags").create({"name": "Science"})`` creates one value.


        :param data: Public aliases used to create the shared record.
        :return: Receipt containing the new shared identity.
        """
        return self._owner._write(lambda: (self._repository.create(data), True))

    def update(
        self,
        entity_id: int,
        data: Mapping[str, object],
        *,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Apply supplied public aliases and refresh affected projections.

        Example:
            A renamed tag is reconciled into dependent cached metadata.


        :param entity_id: Existing shared identity.
        :param data: Repository public aliases; omission preserves values on update.
        :param expected_revision: Optional authoritative full-row precondition.
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

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )

    def delete(
        self, entity_id: int, *, expected_revision: SharedMetadataRevision | None = None
    ) -> MetadataWriteReceipt[int]:
        """
        Delegate deletion policy and reconcile dependent cached metadata.

        Example:
            Deletion constraints on a referenced shared value still propagate.


        :param entity_id: Existing shared identity to delete under repository policy.
        :param expected_revision: Optional authoritative full-row precondition.
        :return: Receipt containing the deleted shared identity.
        """
        return self._delete(
            entity_id, expected_revision=expected_revision, refuse_if_linked=False
        )

    def _delete(
        self,
        entity_id: int,
        *,
        expected_revision: SharedMetadataRevision | None,
        refuse_if_linked: bool,
    ) -> MetadataWriteReceipt[int]:
        """
        Delete under repository policy, optionally refusing a linked entity first.

        The link check runs inside the same serialized write as the deletion, so a
        link committed concurrently cannot slip between them.

        Example:
            ``self._delete(7, expected_revision=None, refuse_if_linked=True)`` raises
            for a Tag still on a Work.


        :param entity_id: Existing shared identity to delete.
        :param expected_revision: Optional authoritative full-row precondition.
        :param refuse_if_linked: Raise instead of deleting when link rows attach the
            entity to other entities.
        :return: Receipt containing the deleted shared identity.
        :raises CatalogInUseError: The entity is linked and ``refuse_if_linked`` is true.
        """

        def operation() -> tuple[int, bool]:
            """
            Delegate shared-record deletion before cache publication.

            Example:
                A database constraint failure escapes without reporting a successful receipt.


            :return: Deleted identity and True after successful repository deletion.
            """
            if refuse_if_linked:
                self._repository.require(entity_id)
                references = self._repository.link_references(entity_id)
                if references:
                    raise CatalogInUseError(
                        f"{self._kind} row {entity_id} is linked to other entities; "
                        "pass detach=True to remove those links with it",
                        references,
                    )
            self._repository.delete(entity_id)
            return entity_id, True

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )


def _require_detach_flag(detach: object) -> bool:
    """
    Accept only a real boolean for the explicit detach choice.

    Example:
        >>> _require_detach_flag(True)
        True


    :param detach: Caller's detach argument.
    :return: The same boolean.
    :raises TypeError: The argument is not a bool.
    """
    if type(detach) is not bool:
        raise TypeError("detach must be boolean")
    return detach


class AgentMetadata(SharedMetadata, SuggestibleMetadataReader):
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
        self,
        entity_id: int,
        name: str,
        *,
        derive_sort: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
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
        :param expected_revision: Optional Agent-row precondition; credits are excluded.
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

        return self._owner._write(
            operation, expected=self._expected(entity_id, expected_revision)
        )

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Delete an Agent, refusing while credits or Agent links remain unless detached.

        Deleting a credited Agent would remove its contributions from every Work,
        Expression, Manifestation and Item at once, so that needs ``detach=True``.
        Person or organisation details belong to the Agent and go with it.

        Example:
            ``metadata.agents.delete(agent_id, detach=True)`` removes the Agent and
            its credits together.


        :param entity_id: Existing Agent identity.
        :param detach: Also remove link rows attaching the Agent to other entities.
        :param expected_revision: Optional Agent-row precondition.
        :return: Receipt containing the deleted Agent identity.
        :raises CatalogInUseError: Links remain and ``detach`` is false.
        """
        return self._delete(
            entity_id,
            expected_revision=expected_revision,
            refuse_if_linked=not _require_detach_flag(detach),
        )


class VocabularyMetadata(SharedMetadata):
    """Administer mutable values without advertising unsupported capabilities."""

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Delete a value, refusing while it is linked to Works or others unless detached.

        Tree vocabularies override this with their own leaf and subtree rules.

        Example:
            ``metadata.tags.delete(tag_id)`` raises while the Tag is on any Work;
            ``metadata.tags.delete(tag_id, detach=True)`` removes those memberships too.


        :param entity_id: Existing vocabulary identity.
        :param detach: Also remove link rows attaching the value to other entities.
        :param expected_revision: Optional full-row precondition.
        :return: Receipt containing the deleted identity.
        :raises CatalogInUseError: Links remain and ``detach`` is false.
        """
        return self._delete(
            entity_id,
            expected_revision=expected_revision,
            refuse_if_linked=not _require_detach_flag(detach),
        )


class TextVocabularyMetadata(VocabularyMetadata, SuggestibleMetadataReader):
    """Combine mutable values with literal text suggestions."""


class WorkMembershipMetadata(TextVocabularyMetadata):
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
