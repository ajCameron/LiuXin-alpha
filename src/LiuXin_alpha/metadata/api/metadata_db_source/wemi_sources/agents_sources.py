"""Agent-facing metadata source contracts."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers.agent_profile_api import AgentProfileAPI
    from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
        AgentParticipationSnapshot,
        WorkAgentCredit,
    )
    from LiuXin_alpha.metadata.metadata_types import AgentID, WorkID, AgentTypes


class AgentProfileGetterAPI(abc.ABC):
    """Read agent identities, intrinsic profiles, and participation views from the database."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        self.db = db

    @abc.abstractmethod
    def get_agent_identity(self, agent_id: 'AgentID') -> 'AgentIdentityAPI':
        """Get the narrow identity container for one agent."""

    @abc.abstractmethod
    def get_agent_profile(self, agent_id: 'AgentID') -> 'AgentProfileAPI':
        """Get the intrinsic profile container for one agent."""

    @abc.abstractmethod
    def get_agent_participation_snapshot(self, agent_id: 'AgentID') -> 'AgentParticipationSnapshot':
        """Get the read-side 'everything this agent is involved in' snapshot."""

    @abc.abstractmethod
    def get_work_credit_for_typed_agent(
        self,
        work_id: 'WorkID',
        agent_id: 'AgentID',
        type_filter: 'AgentTypes',
    ) -> Optional['WorkAgentCredit']:
        """Get the single work credit for an agent/type combination, if one exists."""

    @abc.abstractmethod
    def get_work_credits_for_agent(
        self,
        work_id: 'WorkID',
        agent_id: 'AgentID',
    ) -> Iterable['WorkAgentCredit']:
        """Get all work credits for a single agent on a single work."""

    @abc.abstractmethod
    def get_work_agent_credits(self, work_id: 'WorkID') -> Iterable['WorkAgentCredit']:
        """Get all work-agent credits for a single work."""
