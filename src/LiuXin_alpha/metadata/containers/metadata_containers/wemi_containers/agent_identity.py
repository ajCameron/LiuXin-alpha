"""
Implement the minimal canonical agent identity separately from profiles and participation views.

Example:
    >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
    >>> identity.display_name
    'Ada'
"""
from __future__ import annotations

from typing import Any, Mapping

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.agent_containers.agent_identity_api import AgentIdentityAPI
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)
from LiuXin_alpha.metadata.metadata_types import AgentTypes


class AgentIdentity(AgentIdentityAPI):
    """
    Store an agent row id, type and display/sort names.

    Names and type remain editable. The public id setter accepts assignments only while
    the stored id is None.

    Example:
        >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
        >>> identity.agent_id
        7
    """

    def __init__(
        self,
        *,
        agent_id: int | None = None,
        agent_type: AgentTypes | None = None,
        agent_display_name: str | None = None,
        agent_sort_name: str | None = None,
    ) -> None:
        """
        Store the supplied identity fields without coercion or lookup.

        Example:
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> identity.sort_name is None
            True


        :param agent_id: Optional agent row id; a non-None value locks the public id setter.
        :param agent_type: Optional agent type value retained as supplied.
        :param agent_display_name: Optional human-readable name.
        :param agent_sort_name: Optional name used for sorting.
        :return: None.
        """
        self._agent_id = agent_id
        self._agent_type = agent_type
        self._display_name = agent_display_name
        self._sort_name = agent_sort_name

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> 'AgentIdentity':
        """
        Build an identity using canonical and short-name fallbacks.

        Display text takes the first truthy value among agent_display_name,
        agent_canonical_name and display_name. Sorting similarly prefers agent_sort_name to
        sort_name. Values are not coerced.

        Example:
            >>> identity = AgentIdentity.from_mapping({'agent_canonical_name': 'Ada', 'sort_name': 'Lovelace, Ada'})
            >>> identity.display_name, identity.sort_name
            ('Ada', 'Lovelace, Ada')


        :param row: Mapping with optional agent identity columns and name aliases.
        :return: New identity instance of the requested class.
        """
        return cls(
            agent_id=row.get('agent_id'),
            agent_type=row.get('agent_type'),
            agent_display_name=(
                row.get('agent_display_name')
                or row.get('agent_canonical_name')
                or row.get('display_name')
            ),
            agent_sort_name=row.get('agent_sort_name') or row.get('sort_name'),
        )

    def to_mapping(self) -> dict[str, object]:
        """
        Serialize the four identity fields using the agent-prefixed display-name keys.

        Example:
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> identity.to_mapping()['agent_display_name']
            'Ada'


        :return: New dictionary including None values.
        """
        return {
            'agent_id': self.agent_id,
            'agent_type': self.agent_type,
            'agent_display_name': self.display_name,
            'agent_sort_name': self.sort_name,
        }

    def __str__(self) -> str:
        """
        Format a compact diagnostic string containing populated identity fields.

        Example:
            >>> identity = AgentIdentity(agent_id=7, agent_display_name='Ada')
            >>> 'Ada' in str(identity)
            True


        :return: Human-readable identity summary.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("agent_id",),
            display_keys=("agent_display_name", "agent_type", "agent_sort_name"),
        )

    @property
    def agent_id(self) -> int | None:
        """
        Return the stored agent row id.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.agent_id is None
            True


        :return: Agent row id, or None when unset.
        """
        return self._agent_id

    @agent_id.setter
    def agent_id(self, value: int | None) -> None:
        """
        Assign an agent id only while the stored id is None.

        Raise AttributeError once a non-None id is stored, even if the new value is
        identical. Assigning None to an unset id leaves it assignable.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.agent_id = 7
            >>> identity.agent_id = 7
            Traceback (most recent call last):
            ...
            AttributeError: Agent id is already set.


        :param value: New agent row id, or None to leave or mark it unset.
        :return: None.
        """
        if self._agent_id is None:
            self._agent_id = value
        else:
            raise AttributeError('Agent id is already set.')

    @property
    def agent_type(self) -> AgentTypes | None:
        """
        Return the stored agent type.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.agent_type is None
            True


        :return: Agent type, or None when unset.
        """
        return self._agent_type

    @agent_type.setter
    def agent_type(self, value: AgentTypes | None) -> None:
        """
        Replace the agent type without coercion or validation.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.agent_type = 'person'
            >>> identity.agent_type
            'person'


        :param value: New agent type, or None to leave or mark it unset.
        :return: None.
        """
        self._agent_type = value

    @property
    def display_name(self) -> str | None:
        """
        Return the stored display name.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.display_name is None
            True


        :return: Display name, or None when unset.
        """
        return self._display_name

    @display_name.setter
    def display_name(self, value: str | None) -> None:
        """
        Replace the display name without coercion or validation.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.display_name = 'Ada'
            >>> identity.display_name
            'Ada'


        :param value: New display name, or None to leave or mark it unset.
        :return: None.
        """
        self._display_name = value

    @property
    def sort_name(self) -> str | None:
        """
        Return the stored sort name.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.sort_name is None
            True


        :return: Sort name, or None when unset.
        """
        return self._sort_name

    @sort_name.setter
    def sort_name(self, value: str | None) -> None:
        """
        Replace the sort name without coercion or validation.

        Example:
            >>> identity = AgentIdentity()
            >>> identity.sort_name = 'Lovelace, Ada'
            >>> identity.sort_name
            'Lovelace, Ada'


        :param value: New sort name, or None to leave or mark it unset.
        :return: None.
        """
        self._sort_name = value


__all__ = ['AgentIdentity']
