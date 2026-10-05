"""
Define database-backed getters for agent identities, profiles, participation snapshots and work credits.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise agents sources with the owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_profile_api import AgentProfileAPI
    from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers import (
        AgentParticipationSnapshot,
        WorkAgentCredit,
    )
    from LiuXin_alpha.metadata.metadata_types import AgentID, WorkID, AgentTypes


class AgentProfileGetterAPI(abc.ABC):
    """
    Contract agent identities, profiles, participation snapshots and work-credit reads.

    Example:
        Exercise AgentProfileGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind an agent metadata getter to its database dependency.

        Example:
            Exercise AgentProfileGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_agent_identity(self, agent_id: 'AgentID') -> 'AgentIdentityAPI':
        """
        Return the narrow identity container for one agent.

        Example:
            Exercise AgentProfileGetterAPI.get agent identity with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param agent_id: Agent identifier used to load identity, profile or credits.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_agent_profile(self, agent_id: 'AgentID') -> 'AgentProfileAPI':
        """
        Return intrinsic names and profile metadata for one agent.

        Example:
            Exercise AgentProfileGetterAPI.get agent profile with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param agent_id: Agent identifier used to load identity, profile or credits.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_agent_participation_snapshot(self, agent_id: 'AgentID') -> 'AgentParticipationSnapshot':
        """
        Return the read-side snapshot of works and relations involving one agent.

        Example:
            Exercise AgentProfileGetterAPI.get agent participation snapshot with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param agent_id: Agent identifier used to load identity, profile or credits.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_work_credit_for_typed_agent(
        self,
        work_id: 'WorkID',
        agent_id: 'AgentID',
        type_filter: 'AgentTypes',
    ) -> Optional['WorkAgentCredit']:
        """
        Return one work credit for an agent and role type, or None when absent.

        Example:
            Exercise AgentProfileGetterAPI.get work credit for typed agent with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param work_id: Work identifier used to load identity or metadata.
        :param agent_id: Agent identifier used to load identity, profile or credits.
        :param type_filter: Agent role type required for the selected credit.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_work_credits_for_agent(
        self,
        work_id: 'WorkID',
        agent_id: 'AgentID',
    ) -> Iterable['WorkAgentCredit']:
        """
        Return every role credit held by one agent on one work.

        Example:
            Exercise AgentProfileGetterAPI.get work credits for agent with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param work_id: Work identifier used to load identity or metadata.
        :param agent_id: Agent identifier used to load identity, profile or credits.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_work_agent_credits(self, work_id: 'WorkID') -> Iterable['WorkAgentCredit']:
        """
        Return all agent credits attached to one work.

        Example:
            Exercise AgentProfileGetterAPI.get work agent credits with the owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param work_id: Work identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """
