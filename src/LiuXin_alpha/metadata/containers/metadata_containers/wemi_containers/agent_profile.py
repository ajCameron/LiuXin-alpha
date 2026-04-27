"""Intrinsic profile implementation containers for canonical agents.

Category: agent-profile exception.
This module implements metadata about the agent itself. It is deliberately not a
WEMI `XMetadata` object and not a graph-spanning participation view.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_profile_api import AgentProfileAPI
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity


class AgentProfile(AgentProfileAPI):
    """
    Concrete implementation of :class:`AgentProfileAPI`.

    This is a deliberately light intrinsic profile bundle. Rich graph traversal
    remains the job of participation snapshots/views.
    """

    def __init__(
        self,
        *,
        agent: Optional[AgentIdentityAPI] = None,
        legal_name: str | None = None,
        birth_name: str | None = None,
        biography: str | None = None,
        notes: str | None = None,
        canonical_name_ep_k: int | None = None,
        aliases: Iterable[str] = (),
        identifiers: Iterable[str] = (),
        labels: Iterable[str] = (),
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        self._agent = agent
        self._legal_name = legal_name
        self._birth_name = birth_name
        self._biography = biography
        self._notes = notes
        self._canonical_name_ep_k = canonical_name_ep_k
        self._aliases = tuple(aliases)
        self._identifiers = tuple(identifiers)
        self._labels = tuple(labels)
        self._extra = dict(extra or {})

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> 'AgentProfile':
        agent_mapping = row.get('agent')
        agent = None
        if isinstance(agent_mapping, Mapping):
            agent = AgentIdentity.from_mapping(agent_mapping)
        elif any(key in row for key in ('agent_id', 'agent_type', 'agent_display_name', 'agent_sort_name', 'display_name', 'sort_name')):
            agent = AgentIdentity.from_mapping(row)
        return cls(
            agent=agent,
            legal_name=row.get('legal_name'),
            birth_name=row.get('birth_name'),
            biography=row.get('biography'),
            notes=row.get('notes'),
            canonical_name_ep_k=row.get('canonical_name_ep_k') or row.get('canonical_name_timestamp_ep_k'),
            aliases=tuple(row.get('aliases') or ()),
            identifiers=tuple(row.get('identifiers') or ()),
            labels=tuple(row.get('labels') or ()),
            extra=row.get('extra') or {},
        )

    @property
    def agent(self) -> Optional[AgentIdentityAPI]:
        return self._agent

    @agent.setter
    def agent(self, value: Optional[AgentIdentityAPI]) -> None:
        self._agent = value

    @property
    def legal_name(self) -> str | None:
        return self._legal_name

    @legal_name.setter
    def legal_name(self, value: str | None) -> None:
        self._legal_name = value

    @property
    def birth_name(self) -> str | None:
        return self._birth_name

    @birth_name.setter
    def birth_name(self, value: str | None) -> None:
        self._birth_name = value

    @property
    def biography(self) -> str | None:
        return self._biography

    @biography.setter
    def biography(self, value: str | None) -> None:
        self._biography = value

    @property
    def notes(self) -> str | None:
        return self._notes

    @notes.setter
    def notes(self, value: str | None) -> None:
        self._notes = value

    @property
    def canonical_name_ep_k(self) -> int | None:
        return self._canonical_name_ep_k

    @canonical_name_ep_k.setter
    def canonical_name_ep_k(self, value: int | None) -> None:
        self._canonical_name_ep_k = value

    @property
    def aliases(self) -> tuple[str, ...]:
        return self._aliases

    @aliases.setter
    def aliases(self, value: tuple[str, ...] | list[str]) -> None:
        self._aliases = tuple(value)

    @property
    def identifiers(self) -> tuple[str, ...]:
        return self._identifiers

    @identifiers.setter
    def identifiers(self, value: tuple[str, ...] | list[str]) -> None:
        self._identifiers = tuple(value)

    @property
    def labels(self) -> tuple[str, ...]:
        return self._labels

    @labels.setter
    def labels(self, value: tuple[str, ...] | list[str]) -> None:
        self._labels = tuple(value)

    @property
    def extra(self) -> Mapping[str, Any]:
        return self._extra

    def add_alias(self, value: str) -> None:
        self._aliases = tuple([*self._aliases, value])

    def add_identifier(self, value: str) -> None:
        self._identifiers = tuple([*self._identifiers, value])

    def add_label(self, value: str) -> None:
        self._labels = tuple([*self._labels, value])

    def to_mapping(self) -> dict[str, Any]:
        return {
            'agent': self.agent.to_mapping() if self.agent is not None else None,
            'legal_name': self.legal_name,
            'birth_name': self.birth_name,
            'biography': self.biography,
            'notes': self.notes,
            'canonical_name_ep_k': self.canonical_name_ep_k,
            'aliases': list(self.aliases),
            'identifiers': list(self.identifiers),
            'labels': list(self.labels),
            'extra': dict(self.extra),
        }


__all__ = ['AgentProfile']
