"""Agent identity API contract for canonical agents.

Category: identity object.
This module defines the smallest stable API for the agent itself. Graph-spanning
participation results and intrinsic profile metadata live elsewhere.
"""
from __future__ import annotations

import abc

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.metadata_types import AgentID, AgentTypes


class AgentIdentityAPI(abc.ABC):
    """
    Lightweight identity surface for a canonical agent.

    This should stay narrow. Querying "what is this agent credited for?"
    belongs on a repository / database API, not on the identity object itself.
    """

    @property
    @abc.abstractmethod
    def agent_id(self) -> AgentID | None:
        """Canonical agent ID."""

    @agent_id.setter
    @abc.abstractmethod
    def agent_id(self, value: AgentID | None) -> None:
        """Set canonical agent ID."""

    @property
    @abc.abstractmethod
    def agent_type(self) -> AgentTypes | None:
        """Human / organisation / etc."""

    @agent_type.setter
    @abc.abstractmethod
    def agent_type(self, value: AgentTypes | None) -> None:
        """Set agent type."""

    @property
    @abc.abstractmethod
    def display_name(self) -> str | None:
        """Best display name for the agent."""

    @display_name.setter
    @abc.abstractmethod
    def display_name(self, value: str | None) -> None:
        """Set display name."""

    @property
    def sort_name(self) -> str | None:
        """Optional canonical sort form."""
        return None

    @sort_name.setter
    def sort_name(self, value: str | None) -> None:
        raise AttributeError("sort_name is read-only on this implementation")

    def to_mapping(self) -> MutableMetadataRecord:
        """Convert to a plain mapping."""
        return {
            'agent_id': self.agent_id,
            'agent_type': self.agent_type,
            'agent_display_name': self.display_name,
            'agent_sort_name': self.sort_name,
        }

    def __str__(self) -> str:
        return self.display_name or "<unnamed agent>"

__all__ = ["AgentIdentityAPI"]
