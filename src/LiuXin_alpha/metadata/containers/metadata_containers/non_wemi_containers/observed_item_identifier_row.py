"""
Provide the concrete item_identifiers row value used by metadata callers.

The ObservedItemIdentifierRow dataclass stores database-shaped fields in memory and
inherits column mapping and diagnostic-string helpers. Creating or editing it
performs no database write.

Example:
    >>> row = ObservedItemIdentifierRow(item_identifier_value='observed-7')
    >>> row.item_identifier_value
    'observed-7'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from LiuXin_alpha.databases.db_types import IdentifierScheme

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class ObservedItemIdentifierRow(MetadataTableRow):
    """
    Store an identifier observed for one item by an external source.

    The row records the item id, scheme/value and source separately from
    entity_identifiers; construction does not promote or reconcile identifiers.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = ObservedItemIdentifierRow.from_mapping({'item_identifier_id': 7, 'item_identifier_value': 'observed-7'})
        >>> row.primary_id, row.to_mapping()['item_identifier_value']
        (7, 'observed-7')
    """
    TABLE_NAME: ClassVar[str] = "item_identifiers"
    ID_COLUMN: ClassVar[str] = "item_identifier_id"

    item_identifier_id: int | None = None
    item_identifier_item_id: int | None = None
    item_identifier_scheme: IdentifierScheme | str | None = None
    item_identifier_value: str | None = None
    item_identifier_source: str | None = None
    item_identifier_created_timestamp_ep_k: int | None = None
    item_identifier_modified_timestamp_ep_k: int | None = None
    item_identifier_source_created_datestamp_ep_k: int | None = None
    item_identifier_source_modified_datestamp_ep_k: int | None = None
    item_identifier_scratch: str | None = None


__all__ = ["ObservedItemIdentifierRow"]
