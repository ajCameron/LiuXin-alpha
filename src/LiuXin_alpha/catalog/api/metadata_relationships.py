"""Typed relationship views and explicit owner-oriented metadata editing."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, overload

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_results import MetadataWriteReceipt
    from LiuXin_alpha.catalog.api.metadata_revisions import MetadataRevision
    from LiuXin_alpha.catalog.api.metadata_search import MetadataReference
    from LiuXin_alpha.catalog.api.metadata_shared import SharedMetadataRecord


@dataclass(frozen=True, slots=True)
class MetadataMembership:
    """A detached member and one association's type and ordering priority."""

    member: SharedMetadataRecord
    link_type: str | None
    priority: int | float | None


@dataclass(frozen=True, slots=True)
class MetadataCredit:
    """A detached Agent credited under one explicit role on a WEMI owner."""

    agent: SharedMetadataRecord
    role: str
    priority: int | float | None


class MetadataCreditsInterface(Protocol):
    """Read and edit credits without guessing Agent identities from names."""

    def list_credits(self, entity_id: int) -> tuple[MetadataCredit, ...]:
        """Read all roles in stored priority order from one complete source."""
        ...

    def add_credit(
        self,
        entity_id: int,
        *,
        agent_id: int,
        role: str,
        priority: int | None = None,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Upsert one existing Agent/role and preserve omitted ordering metadata."""
        ...

    def remove_credit(
        self,
        entity_id: int,
        *,
        agent_id: int,
        role: str,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Remove only this Agent/role, retaining its other credits and identity."""
        ...

    def set_credits(
        self,
        entity_id: int,
        *,
        role: str,
        agent_ids: Sequence[int],
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[MetadataReference]:
        """Replace one role in supplied display order; empty input clears that role."""
        ...


class MetadataWorkMembershipsInterface(Protocol):
    """Edit Work Tag/Series membership sets while retaining existing link metadata."""

    def list_tags(self, work_id: int) -> tuple[MetadataMembership, ...]:
        """Read detached Tag memberships, requiring a complete existing Work."""
        ...

    def list_series(self, work_id: int) -> tuple[MetadataMembership, ...]:
        """Read every Series membership type without interpreting volume numbers."""
        ...

    @overload
    def add_tag(
        self,
        work_id: int,
        *,
        tag_id: int,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]: ...
    @overload
    def add_tag(
        self,
        work_id: int,
        *,
        text: str,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]: ...
    def add_tag(
        self,
        work_id: int,
        *,
        tag_id: int | None = None,
        text: str | None = None,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Attach an existing ID or atomically ensure and attach literal Tag text."""
        ...

    def remove_tag(
        self,
        work_id: int,
        tag_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Remove the pair's links while keeping both endpoint identities."""
        ...

    def set_tags(
        self,
        work_id: int,
        tag_ids: Sequence[int],
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace membership with unique existing IDs, preserving retained links."""
        ...

    def add_series(
        self,
        work_id: int,
        series_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Attach an existing Series without creating or guessing its identity."""
        ...

    def remove_series(
        self,
        work_id: int,
        series_id: int,
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[int]:
        """Remove every type for one Work/Series pair, preserving both endpoints."""
        ...

    def set_series(
        self,
        work_id: int,
        series_ids: Sequence[int],
        *,
        expected_revision: MetadataRevision | None = None,
    ) -> MetadataWriteReceipt[tuple[int, ...]]:
        """Replace the membership set without assigning preferred-Series order."""
        ...
