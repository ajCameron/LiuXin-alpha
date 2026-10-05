"""
Provide the concrete org_agents row value used by metadata callers.

The OrgAgentRow dataclass stores database-shaped fields in memory and inherits
column mapping and diagnostic-string helpers. Creating or editing it performs no
database write.

Example:
    >>> row = OrgAgentRow(org_agent_legal_name='Example Press')
    >>> row.org_agent_legal_name
    'Example Press'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class OrgAgentRow(MetadataTableRow):
    """
    Store an organization profile associated with a generic agent id.

    Legal/trading names, registration, jurisdiction, dates and contact fields are
    retained without creating the linked agent or validating external addresses.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = OrgAgentRow.from_mapping({'org_agent_id': 7, 'org_agent_legal_name': 'Example Press'})
        >>> row.primary_id, row.to_mapping()['org_agent_legal_name']
        (7, 'Example Press')
    """
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
