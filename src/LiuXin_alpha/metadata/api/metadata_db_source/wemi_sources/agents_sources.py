
"""
Responsible for getting agents metadata from the system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import abc

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI

    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.agent_containers import (
        AgentCreditsContainerBaseAPI,
        WorkAgentCredit)

    from LiuXin_alpha.metadata.metadata_types import AgentID, WorkID, AgentTypes


class AgentMetadataGetterAPI(abc.ABC):
    """
    Get metadata concerning agents from the database.
    """

    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the agent metadata getter.

        :param db:
        """
        self.db = db

    @abc.abstractmethod
    def get_agent_metadata(self, agent_id: "AgentID") -> "AgentCreditsContainerBaseAPI":
        """
        Get a metadata container for metadata concerning the individual agent.

        :param agent_id:
        :return:
        """

    # - AGENT CREDIT GETTERS

    @abc.abstractmethod
    def get_work_agent_type_credit(
            self,
            work_id: "WorkID",
            agent_id: "AgentID",
            type_filter: "AgentTypes") -> Optional["WorkAgentCredit"]:
        """
        Get the work agent credit of the given type - if one exists.

        We're looking for a single Work Agent credit - if one exists.
        If there is no one which satisfies the link requirements, returns None.
        :param work_id:
        :param agent_id:
        :param type_filter:
        :return:
        """

