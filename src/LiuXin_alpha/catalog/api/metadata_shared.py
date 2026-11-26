"""Native shared metadata administration without exposing raw repository writes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict

from LiuXin_alpha.catalog.api.metadata_shared_values import (
    SharedMetadataKind as SharedMetadataKind,
)
from LiuXin_alpha.catalog.api.metadata_shared_values import (
    SharedMetadataPage as SharedMetadataPage,
)
from LiuXin_alpha.catalog.api.metadata_shared_values import (
    SharedMetadataRecord as SharedMetadataRecord,
)
from LiuXin_alpha.catalog.api.metadata_shared_values import (
    VocabularyKind as VocabularyKind,
)

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import WemiLevel
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt
    from LiuXin_alpha.catalog.api.metadata_revisions import (
        SharedMetadataRevision,
        SharedMetadataSnapshot,
    )


class AgentInput(TypedDict, total=False):
    """Public core Agent attributes; omitted keys retain existing values."""

    name: str
    canonical_name: str
    sort_name: str | None
    type: str
    aliases: str | tuple[str, ...] | list[str] | None
    note: str | None


class MetadataRecordsReader(Protocol):
    """Read detached public aliases through the bound source, ordered by identity."""

    def get(self, entity_id: int) -> SharedMetadataRecord | None:
        """Return a detached value or None for a confirmed miss."""
        ...

    def require(self, entity_id: int) -> SharedMetadataRecord:
        """Require an existing shared identity."""
        ...

    def list(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return an ID-ordered page from the bound read source."""
        ...

    def get_many(
        self, entity_ids: Sequence[int], *, missing: Literal["none", "raise"] = "none"
    ) -> tuple[SharedMetadataRecord | None, ...]:
        """Read once, retaining input order, duplicate slots and confirmed misses."""
        ...

    def lookup(
        self, fields: Mapping[str, object], *, limit: int = 100, offset: int = 0
    ) -> SharedMetadataPage:
        """Filter public aliases exactly, returning all candidates without resolving identity."""
        ...


class SuggestibleMetadataReader(MetadataRecordsReader, Protocol):
    """Read literal text candidates without creating or choosing identities."""

    def suggest(
        self,
        text: str,
        *,
        mode: Literal["exact", "prefix", "contains"] = "prefix",
        limit: int = 20,
        offset: int = 0,
        filters: Mapping[str, object] | None = None,
    ) -> SharedMetadataPage:
        """Return a deterministic literal page; empty normalized text has no matches."""
        ...


class SharedMetadataInterface[InputT](MetadataRecordsReader, Protocol):
    """Coordinate record mutations using Catalog validation and public aliases.

    Context-owned records require their owner in the payload. Languages expose
    MetadataRecordsReader instead and have no mutation methods.
    """

    def read_for_edit(self, entity_id: int) -> SharedMetadataSnapshot:
        """Read the authoritative full row and a record-scoped edit precondition."""
        ...

    def create(self, data: InputT) -> MetadataWriteReceipt[int]:
        """Create through Catalog without implicit reuse or merging."""
        ...

    def update(
        self,
        entity_id: int,
        data: InputT,
        *,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Edit supplied aliases and refresh all affected metadata projections."""
        ...

    def delete(
        self, entity_id: int, *, expected_revision: SharedMetadataRevision | None = None
    ) -> MetadataWriteReceipt[int]:
        """Delete under Catalog/schema constraints, reconciling linked metadata."""
        ...


class AgentMetadataInterface(
    SharedMetadataInterface[AgentInput], SuggestibleMetadataReader, Protocol
):
    """Administer core Agents and create typed person/organisation identities."""

    def create_person(self, data: AgentInput) -> MetadataWriteReceipt[int]:
        """Create an Agent with its person subtype atomically."""
        ...

    def create_organisation(self, data: AgentInput) -> MetadataWriteReceipt[int]:
        """Create an Agent with its organisation subtype atomically."""
        ...

    def rename(
        self,
        entity_id: int,
        name: str,
        *,
        derive_sort: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Rename with Catalog's explicit preserve/derive sort-name policy.

        Nested use requires a serialized outer macro transaction, including the
        one owned by metadata.batch(). The Agent's credits remain attached.
        """
        ...

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Delete an Agent; credits and Agent links require ``detach=True``.

        Raises CatalogInUseError while links remain and ``detach`` is false.

        Example:
            ``metadata.agents.delete(agent_id, detach=True)`` also removes credits.


        :param entity_id: Existing Agent identity.
        :param detach: Also remove link rows attaching the Agent to other entities.
        :param expected_revision: Optional Agent-row precondition.
        :return: Receipt containing the deleted Agent identity.
        """
        ...


class VocabularyMetadataInterface(
    SharedMetadataInterface[Mapping[str, object]], Protocol
):
    """Manage mutable native values without assuming hierarchy or membership."""

    def delete(
        self,
        entity_id: int,
        *,
        detach: bool = False,
        expected_revision: SharedMetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """
        Delete a value; links to Works or other entities require ``detach=True``.

        Raises CatalogInUseError while links remain and ``detach`` is false.

        Example:
            ``metadata.tags.delete(tag_id, detach=True)`` also removes memberships.


        :param entity_id: Existing vocabulary identity.
        :param detach: Also remove link rows attaching the value to other entities.
        :param expected_revision: Optional full-row precondition.
        :return: Receipt containing the deleted identity.
        """
        ...


class TextVocabularyMetadataInterface(
    VocabularyMetadataInterface, SuggestibleMetadataReader, Protocol
):
    """Read literal candidates and administer mutable text vocabulary values."""


class WorkMembershipMetadataInterface(TextVocabularyMetadataInterface, Protocol):
    """Coordinate the Work memberships supported by Tags and Series."""

    def link_to_work(
        self, *, entity_id: int, work_id: int
    ) -> MetadataWriteReceipt[int]:
        """Attach a Tag or Series under Catalog's membership policy."""
        ...

    def unlink_from_work(
        self, *, entity_id: int, work_id: int
    ) -> MetadataWriteReceipt[int]:
        """Detach a Tag or Series while retaining both endpoint identities."""
        ...


class TextMetadataInput(TypedDict, total=False):
    """Note, Comment or Synopsis text; omitted fields remain unchanged on update."""

    text: str


class IdentifierInput(TypedDict, total=False):
    """Curated scheme/value and optional WEMI or Agent ownership."""

    scheme: str
    value: str
    source: str | None
    is_primary: bool | int | None
    level: WemiLevel | Literal["agent"] | None
    entity_id: int | None


class ItemIdentifierInput(TypedDict, total=False):
    """Raw identifier observation; creation requires item_id, scheme and value."""

    item_id: int
    scheme: str
    value: str
    source: str | None


class AnnotationInput(TypedDict, total=False):
    """Item-scoped annotation fields, preserving independent equal-text records."""

    item_id: int
    user_id: int | None
    kind: str
    anchor_type: str
    anchor_start: str
    anchor_end: str | None
    selected_text: str | None
    note_text: str | None
    source: str | None
    device_id: int | None
    extra_json: str | None


class TextMetadataInterface(SharedMetadataInterface[TextMetadataInput], Protocol):
    """Administer text records and create fresh attachments to WEMI owners."""

    def add_for_wemi(
        self, *, level: WemiLevel, entity_id: int, text: str
    ) -> MetadataWriteReceipt[int]:
        """Atomically create and attach fresh text, retaining Catalog cardinality rules."""
        ...

    def list_for_wemi(
        self, *, level: WemiLevel, entity_id: int
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return distinct attached records in ID order; require the owner.

        These are record projections. WEMI hydration supplies relationship
        attributes and ordering when those are needed.
        """
        ...


class IdentifierMetadataInterface(SharedMetadataInterface[IdentifierInput], Protocol):
    """Administer curated identifiers, preserving owner-specific copy semantics."""

    def list_for_wemi(
        self, *, level: WemiLevel, entity_id: int
    ) -> tuple[SharedMetadataRecord, ...]:
        """Return ID-ordered identifiers directly owned by an existing WEMI entity."""
        ...

    def list_for_agent(self, agent_id: int) -> tuple[SharedMetadataRecord, ...]:
        """Return ID-ordered identifiers owned by an existing Agent."""
        ...

    def link_to_wemi(
        self,
        *,
        identifier_id: int,
        level: WemiLevel,
        entity_id: int,
        priority: int | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Assign or copy to the owner; the receipt contains the assigned row ID."""
        ...

    def link_to_agent(
        self,
        *,
        identifier_id: int,
        agent_id: int,
        priority: int | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Assign or copy to an Agent; preserve the original owner when copying."""
        ...


class ItemIdentifierMetadataInterface(
    SharedMetadataInterface[ItemIdentifierInput], Protocol
):
    """Administer raw observations independently of curated entity identifiers."""

    def list_for_item(self, item_id: int) -> tuple[SharedMetadataRecord, ...]:
        """Require the Item and return its observations in ID order."""
        ...


class AnnotationMetadataInterface(SharedMetadataInterface[AnnotationInput], Protocol):
    """Administer annotations by row identity, without global text reuse."""

    def list_for_item(
        self,
        item_id: int,
        *,
        user_id: int | None = None,
        kind: str | None = None,
    ) -> tuple[SharedMetadataRecord, ...]:
        """Require the Item and filter annotations by optional user and exact kind."""
        ...
