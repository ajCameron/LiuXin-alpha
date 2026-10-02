"""
Define the canonical agent identity contract and its minimal default helpers.

The identity exposes an id, type and display name with optional sort text. Intrinsic
profile metadata and graph participation queries are separate APIs.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
    >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
    >>> isinstance(agent, AgentIdentityAPI)
    True
"""
from __future__ import annotations

import abc

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MutableMetadataRecord,
)
from LiuXin_alpha.metadata.metadata_types import AgentID, AgentTypes


class AgentIdentityAPI(abc.ABC):
    """
    Require an agent id, type and display name with implementation-specific setters.

    The base sort name is absent and read-only. Concrete implementations may override
    it; mapping and minimal string helpers read the public identity properties.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
        >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
        >>> AgentIdentityAPI.to_mapping(agent)['agent_display_name']
        'Ada'
    """

    @property
    @abc.abstractmethod
    def agent_id(self) -> AgentID | None:
        """
        Require the canonical agent row id of this identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> agent.agent_id
            3


        :return: Stored canonical agent row id, or None.
        """

    @agent_id.setter
    @abc.abstractmethod
    def agent_id(self, value: AgentID | None) -> None:
        """
        Require assignment of the canonical agent row id under concrete validation policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> fresh = AgentIdentity()
            >>> fresh.agent_id = 7
            >>> fresh.agent_id
            7


        :param value: New canonical agent row id, or None where permitted.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def agent_type(self) -> AgentTypes | None:
        """
        Require the agent classification of this identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> agent.agent_type is None
            True


        :return: Stored agent classification, or None.
        """

    @agent_type.setter
    @abc.abstractmethod
    def agent_type(self, value: AgentTypes | None) -> None:
        """
        Require assignment of the agent classification under concrete validation policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> agent.agent_type = None
            >>> agent.agent_type is None
            True


        :param value: New agent classification, or None where permitted.
        :return: None.
        """

    @property
    @abc.abstractmethod
    def display_name(self) -> str | None:
        """
        Require the preferred display name of this identity.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> agent.display_name
            'Ada'


        :return: Stored preferred display name, or None.
        """

    @display_name.setter
    @abc.abstractmethod
    def display_name(self, value: str | None) -> None:
        """
        Require assignment of the preferred display name under concrete validation policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> agent.display_name = 'Ada Lovelace'
            >>> agent.display_name
            'Ada Lovelace'


        :param value: New preferred display name, or None where permitted.
        :return: None.
        """

    @property
    def sort_name(self) -> str | None:
        """
        Provide an absent sort name unless the implementation overrides it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> AgentIdentityAPI.sort_name.fget(agent) is None
            True


        :return: None in this base implementation.
        """
        return None

    @sort_name.setter
    def sort_name(self, value: str | None) -> None:
        """
        Reject sort-name assignment in the default implementation.

        Concrete identities can override this setter to support editing.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> AgentIdentityAPI.sort_name.fset(agent, 'Lovelace, Ada')
            Traceback (most recent call last):
            ...
            AttributeError: sort_name is read-only on this implementation


        :param value: Requested sort name, unused by this rejecting default.
        :return: Never returns normally; raises AttributeError.
        """
        raise AttributeError("sort_name is read-only on this implementation")

    def to_mapping(self) -> MutableMetadataRecord:
        """
        Collect the four public identity values under canonical agent column names.

        The new dictionary includes None values and retains property values without
        normalization or conversion.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> AgentIdentityAPI.to_mapping(agent)
            {'agent_id': 3, 'agent_type': None, 'agent_display_name': 'Ada', 'agent_sort_name': None}


        :return: New mapping with agent_id, agent_type, agent_display_name and
            agent_sort_name.
        """
        return {
            'agent_id': self.agent_id,
            'agent_type': self.agent_type,
            'agent_display_name': self.display_name,
            'agent_sort_name': self.sort_name,
        }

    def __str__(self) -> str:
        """
        Return a truthy display name or the unnamed-agent placeholder.

        Whitespace-only names remain unchanged; concrete implementations may override this
        diagnostic.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.agent_identity import AgentIdentity
            >>> agent = AgentIdentity(agent_id=3, agent_display_name='Ada')
            >>> AgentIdentityAPI.__str__(agent)
            'Ada'
            >>> agent.display_name = ''
            >>> AgentIdentityAPI.__str__(agent)
            '<unnamed agent>'


        :return: Display name, or the literal <unnamed agent>.
        """
        return self.display_name or "<unnamed agent>"

__all__ = ["AgentIdentityAPI"]
