"""Concrete row container for the ``org_agents`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class OrgAgentRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "org_agents"
    ID_COLUMN: ClassVar[str] = "org_agent_id"

    org_agent_id: int | None = None
    org_agent_agent_id: int | None = None
    org_agent_legal_name: str | None = None
    org_agent_trading_name: str | None = None
    org_agent_registration_id: str | None = None
    org_agent_jurisdiction: str | None = None
    org_agent_founded_date: str | None = None
    org_agent_dissolved_date: str | None = None
    org_agent_website: str | None = None
    org_agent_contact_email: str | None = None
    org_agent_description: str | None = None
    org_agent_created_timestamp_ep_k: int | None = None
    org_agent_modified_timestamp_ep_k: int | None = None
    org_agent_scratch: str | None = None


__all__ = ["OrgAgentRow"]
