"""
Provide the concrete human_agents row value used by metadata callers.

The HumanAgentRow dataclass stores database-shaped fields in memory and inherits
column mapping and diagnostic-string helpers. Creating or editing it performs no
database write.

Example:
    >>> row = HumanAgentRow(human_agent_preferred_name='Ada Lovelace')
    >>> row.human_agent_preferred_name
    'Ada Lovelace'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class HumanAgentRow(MetadataTableRow):
    """
    Store a human profile associated with a generic agent id.

    Name components, preferred name, biography, nationality and date strings are
    retained without parsing or creating the linked agent.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = HumanAgentRow.from_mapping({'human_agent_id': 7, 'human_agent_preferred_name': 'Ada Lovelace'})
        >>> row.primary_id, row.to_mapping()['human_agent_preferred_name']
        (7, 'Ada Lovelace')
    """
    TABLE_NAME: ClassVar[str] = "human_agents"
    ID_COLUMN: ClassVar[str] = "human_agent_id"

    human_agent_id: int | None = None
    human_agent_agent_id: int | None = None
    human_agent_given_name: str | None = None
    human_agent_middle_name: str | None = None
    human_agent_family_name: str | None = None
    human_agent_prefix: str | None = None
    human_agent_suffix: str | None = None
    human_agent_preferred_name: str | None = None
    human_agent_birth_date: str | None = None
    human_agent_death_date: str | None = None
    human_agent_nationality: str | None = None
    human_agent_biography: str | None = None
    human_agent_created_timestamp_ep_k: int | None = None
    human_agent_modified_timestamp_ep_k: int | None = None
    human_agent_scratch: str | None = None


__all__ = ["HumanAgentRow"]
