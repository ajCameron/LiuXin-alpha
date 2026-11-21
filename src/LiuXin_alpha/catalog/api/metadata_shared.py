"""Native shared metadata administration without exposing raw repository writes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt

type VocabularyKind = Literal[
    "tags", "labels", "genres", "subjects", "series", "languages", "ratings"
]
type SharedMetadataKind = Literal[
    "agents", "tags", "labels", "genres", "subjects", "series", "languages", "ratings"
]


class AgentInput(TypedDict, total=False):
    """Public core Agent attributes; omitted keys retain existing values."""

    name: str
    canonical_name: str
    sort_name: str | None
    type: str
    aliases: str | tuple[str, ...] | list[str] | None
    note: str | None


@dataclass(frozen=True, slots=True)
class SharedMetadataRecord:
    """Detached shared identity and public field aliases, without live SQL Rows."""

    kind: SharedMetadataKind
    entity_id: int
    fields: Mapping[str, object]


class SharedMetadataInterface[InputT](Protocol):
    """Read shared values through the bound source; coordinate every mutation.

    Languages retain Catalog's read-only constant policy. Vocabulary payload keys
    follow that repository's public aliases. list() is ordered by identity.
    """

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

    def create(self, data: InputT) -> MetadataWriteReceipt[int]:
        """Create through Catalog without implicit reuse or merging."""
        ...

    def update(self, entity_id: int, data: InputT) -> MetadataWriteReceipt[int]:
        """Edit supplied aliases and refresh all affected metadata projections."""
        ...

    def delete(self, entity_id: int) -> MetadataWriteReceipt[int]:
        """Delete under Catalog/schema constraints, reconciling linked metadata."""
        ...


class AgentMetadataInterface(SharedMetadataInterface[AgentInput], Protocol):
    """Administer core Agents and create typed person/organisation identities."""

    def create_person(self, data: AgentInput) -> MetadataWriteReceipt[int]:
        """Create an Agent with its person subtype atomically."""
        ...

    def create_organisation(self, data: AgentInput) -> MetadataWriteReceipt[int]:
        """Create an Agent with its organisation subtype atomically."""
        ...

    def rename(
        self, entity_id: int, name: str, *, derive_sort: bool = False
    ) -> MetadataWriteReceipt[int]:
        """Rename with Catalog's explicit preserve/derive sort-name policy.

        Nested use requires a serialized outer macro transaction, including the
        one owned by metadata.batch(). The Agent's credits remain attached.
        """
        ...


class VocabularyMetadataInterface(
    SharedMetadataInterface[Mapping[str, object]], Protocol
):
    """Manage native vocabularies, with explicit Work membership for Tags/Series."""

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
