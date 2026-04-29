"""Concrete row container for the ``entity_identifiers`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from LiuXin_alpha.databases.db_types import IdentifierEntityType, IdentifierScheme

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class EntityIdentifierRow(MetadataTableRow):
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
