"""Intrinsic profile API contract for canonical agents.

Category: agent-profile exception.
This module defines metadata about the agent itself. It is deliberately not a
WEMI `XMetadataAPI` object and not a graph-spanning participation view.
"""
from __future__ import annotations

import abc
from typing import Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.metadata_types import AgentID


class AgentProfileAPI(abc.ABC):
    """
    Intrinsic metadata bundle for an agent.

    This is deliberately *not* the same kind of object as a WEMI `XMetadataAPI`.
    It represents metadata about the agent itself rather than a metadata bundle
    around a WEMI node. Graph-spanning query results belong in dedicated read-side
    snapshot/view containers.
    """

    @property
    @abc.abstractmethod
    def agent(self) -> Optional[AgentIdentityAPI]:
        """Primary agent identity for this profile."""

    @agent.setter
    @abc.abstractmethod
    def agent(self, value: Optional[AgentIdentityAPI]) -> None:
        """Set primary agent identity."""

    @property
    def agent_id(self) -> AgentID | None:
        """Convenience pass-through to the attached identity, if any."""
        return self.agent.agent_id if self.agent is not None else None

    @property
    def display_name(self) -> str | None:
        """Convenience pass-through to the attached identity, if any."""
        return self.agent.display_name if self.agent is not None else None

    @property
    @abc.abstractmethod
    def legal_name(self) -> str | None:
        """Formal / legal name, when distinct from the display name."""

    @legal_name.setter
    @abc.abstractmethod
    def legal_name(self, value: str | None) -> None:
        """Set legal name."""

    @property
    @abc.abstractmethod
    def birth_name(self) -> str | None:
        """Birth name or earlier personal name, when relevant."""

    @birth_name.setter
    @abc.abstractmethod
    def birth_name(self, value: str | None) -> None:
        """Set birth name."""

    @property
    @abc.abstractmethod
    def biography(self) -> str | None:
        """Free-text biographical or organisational description."""

    @biography.setter
    @abc.abstractmethod
    def biography(self, value: str | None) -> None:
        """Set biography text."""

    @property
    @abc.abstractmethod
    def notes(self) -> str | None:
        """Loose editorial notes about the agent itself."""

    @notes.setter
    @abc.abstractmethod
    def notes(self, value: str | None) -> None:
        """Set notes."""

    @property
    @abc.abstractmethod
    def canonical_name_ep_k(self) -> int | None:
        """Timestamp for when the current canonical name took effect, if tracked."""

    @canonical_name_ep_k.setter
    @abc.abstractmethod
    def canonical_name_ep_k(self, value: int | None) -> None:
        """Set canonical-name timestamp."""

    @property
    @abc.abstractmethod
    def aliases(self) -> tuple[str, ...]:
        """Alternative display names / aliases for the agent."""

    @aliases.setter
    @abc.abstractmethod
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        """Replace aliases."""

    @property
    @abc.abstractmethod
    def identifiers(self) -> tuple[str, ...]:
        """Intrinsic identifiers for the agent, pending richer typed containers later."""

    @identifiers.setter
    @abc.abstractmethod
    def identifiers(self, value: tuple[str, ...] | list[str]) -> None:
        """Replace identifiers."""

    @property
    @abc.abstractmethod
    def labels(self) -> tuple[str, ...]:
        """Loose intrinsic labels / groupings for the agent itself."""

    @labels.setter
    @abc.abstractmethod
    def labels(self, value: tuple[str, ...] | list[str]) -> None:
        """Replace labels."""

    @property
    @abc.abstractmethod
    def extra(self) -> MetadataRecord:
        """Extension fields for profile metadata not yet promoted to first-class properties."""

    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """Convert to a plain mapping for storage / serialisation."""

__all__ = ["AgentProfileAPI"]
