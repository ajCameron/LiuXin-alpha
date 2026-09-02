"""Concrete row container for the ``org_agent_relations`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class OrgAgentRelationRow(MetadataTableRow):
    """
    Represent a dated parent-child relationship between organisational Agents.
    """
    TABLE_NAME: ClassVar[str] = "org_agent_relations"
    ID_COLUMN: ClassVar[str] = "org_agent_relation_id"

    org_agent_relation_id: int | None = None
    org_agent_relation_child_agent_id: int | None = None
    org_agent_relation_parent_agent_id: int | None = None
    org_agent_relation_type: str | None = None
    org_agent_relation_start_date: str | None = None
    org_agent_relation_end_date: str | None = None
    org_agent_relation_note: str | None = None
    org_agent_relation_created_timestamp_ep_k: int | None = None
    org_agent_relation_modified_timestamp_ep_k: int | None = None
    org_agent_relation_source_created_datestamp_ep_k: int | None = None
    org_agent_relation_source_modified_datestamp_ep_k: int | None = None
    org_agent_relation_scratch: str | None = None


__all__ = ["OrgAgentRelationRow"]
