"""Agent identity implementation containers for canonical agents.

Category: identity object.
This module implements the smallest stable agent surface. Intrinsic profile data
and graph-spanning participation views live elsewhere.
"""
from __future__ import annotations

from typing import Any, Mapping

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.metadata_types import AgentTypes


class AgentIdentity(AgentIdentityAPI):
    """Container for a single agent row."""

    def __init__(
        self,
        *,
        agent_id: int | None = None,
        agent_type: AgentTypes | None = None,
        agent_display_name: str | None = None,
        agent_sort_name: str | None = None,
    ) -> None:
        self._agent_id = agent_id
        self._agent_type = agent_type
        self._display_name = agent_display_name
        self._sort_name = agent_sort_name

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> 'AgentIdentity':
        return cls(
            agent_id=row.get('agent_id'),
            agent_type=row.get('agent_type'),
            agent_display_name=row.get('agent_display_name') or row.get('display_name'),
            agent_sort_name=row.get('agent_sort_name') or row.get('sort_name'),
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            'agent_id': self.agent_id,
            'agent_type': self.agent_type,
            'agent_display_name': self.display_name,
            'agent_sort_name': self.sort_name,
        }

    @property
    def agent_id(self) -> int | None:
        return self._agent_id

    @agent_id.setter
    def agent_id(self, value: int | None) -> None:
        if self._agent_id is None:
            self._agent_id = value
        else:
            raise AttributeError('Agent id is already set.')

    @property
    def agent_type(self) -> AgentTypes | None:
        return self._agent_type

    @agent_type.setter
    def agent_type(self, value: AgentTypes | None) -> None:
        self._agent_type = value

    @property
    def display_name(self) -> str | None:
        return self._display_name

    @display_name.setter
    def display_name(self, value: str | None) -> None:
        self._display_name = value

    @property
    def sort_name(self) -> str | None:
        return self._sort_name

    @sort_name.setter
    def sort_name(self, value: str | None) -> None:
        self._sort_name = value


__all__ = ['AgentIdentity']
