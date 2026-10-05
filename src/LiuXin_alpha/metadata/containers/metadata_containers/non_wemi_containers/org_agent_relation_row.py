"""
Provide the concrete org_agent_relations row value used by metadata callers.

The OrgAgentRelationRow dataclass stores database-shaped fields in memory and
inherits column mapping and diagnostic-string helpers. Creating or editing it
performs no database write.

Example:
    >>> row = OrgAgentRelationRow(org_agent_relation_type='subsidiary')
    >>> row.org_agent_relation_type
    'subsidiary'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class OrgAgentRelationRow(MetadataTableRow):
    """
    Store a dated parent-child relation between organizational agents.

    Child/parent agent ids, relation type, date strings and note remain independent
    fields. Construction does not enforce chronology or detect cycles.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = OrgAgentRelationRow.from_mapping({'org_agent_relation_id': 7, 'org_agent_relation_type': 'subsidiary'})
        >>> row.primary_id, row.to_mapping()['org_agent_relation_type']
        (7, 'subsidiary')
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
