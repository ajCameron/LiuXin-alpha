"""
Provide the concrete entity_identifiers row value used by metadata callers.

The EntityIdentifierRow dataclass stores database-shaped fields in memory and
inherits column mapping and diagnostic-string helpers. Creating or editing it
performs no database write.

Example:
    >>> row = EntityIdentifierRow(entity_identifier_value='10/example')
    >>> row.entity_identifier_value
    '10/example'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from LiuXin_alpha.databases.db_types import IdentifierEntityType, IdentifierScheme

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class EntityIdentifierRow(MetadataTableRow):
    """
    Store a scheme-qualified identifier for a typed entity and its database id.

    The record keeps scheme/value, primary flag and provenance. Enum or string
    scheme/entity types are retained without resolution or normalization.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = EntityIdentifierRow.from_mapping({'entity_identifier_id': 7, 'entity_identifier_value': '10/example'})
        >>> row.primary_id, row.to_mapping()['entity_identifier_value']
        (7, '10/example')
    """
    TABLE_NAME: ClassVar[str] = "entity_identifiers"
    ID_COLUMN: ClassVar[str] = "entity_identifier_id"

    entity_identifier_id: int | None = None
    entity_identifier_entity_type: IdentifierEntityType | str | None = None
    entity_identifier_entity_id: int | None = None
    entity_identifier_scheme: IdentifierScheme | str | None = None
    entity_identifier_value: str | None = None
    entity_identifier_is_primary: int | None = None
    entity_identifier_provenance: str | None = None
    entity_identifier_created_timestamp_ep_k: int | None = None
    entity_identifier_modified_timestamp_ep_k: int | None = None
    entity_identifier_source_created_datestamp_ep_k: int | None = None
    entity_identifier_source_modified_datestamp_ep_k: int | None = None
    entity_identifier_scratch: str | None = None


__all__ = ["EntityIdentifierRow"]
