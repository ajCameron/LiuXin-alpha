"""Concrete row container for the ``item_identifiers`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from LiuXin_alpha.databases.db_types import IdentifierScheme

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class ObservedItemIdentifierRow(MetadataTableRow):
    """
    Represent a source-observed identifier attached to one Item.
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
