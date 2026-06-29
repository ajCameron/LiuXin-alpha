"""Concrete row container for the ``human_agents`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class HumanAgentRow(MetadataTableRow):
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
