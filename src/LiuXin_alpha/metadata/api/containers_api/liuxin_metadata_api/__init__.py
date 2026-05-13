"""
API contracts for legacy LiuXin extended metadata objects.

Category: high-level metadata compatibility API.
This module defines the pre-WEMI LiuXin metadata object surface used by legacy
importers, Calibre adapters, and title-row hydration paths.
"""

from __future__ import annotations

from typing import Protocol, TypeAlias, Mapping, Sequence

from LiuXin_alpha.metadata.api import LiuXinMetadataAPI
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_metadata_types import (
    LiuXinValueToID,
    LiuXinPayloadToID,
    LiuXinRatingMapping,
    LiuXinCreatorMapping,
    LiuXinCreatorDump,
    LiuXinFieldValue,
    LiuXinFieldMapping,
    LiuXinFieldKeys,
    LiuXinValueToID,
    LiuXinPayloadToID,
    LiuXinCreatorDump,
    LiuXinFieldValue,
    LiuXinFieldMapping,
    LiuXinFieldKeys,
    LiuXinRowID,
    LiuXinScalar,
    LiuXinScalarSequence,
    LiuXinStringSet,
    LiuXinValueToID,
    LiuXinPayloadKey,
    LiuXinPayloadToID,
    LiuXinRatingValue,
    LiuXinRatingMapping,
    LiuXinCreatorMapping,
    LiuXinCreatorDump,
    LiuXinFieldValue,
    LiuXinFieldMapping,
    LiuXinFieldKeys)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_title_metadata_api import \
    LiuXinMetadataAPI


class LiuXinMetadataDatabaseAPI(Protocol):
    """Database methods used by legacy ``MetaData.from_title_row``."""

    def get_categorized_tables(self) -> Mapping[str, Sequence[str]]: ...

    def get_display_column(self, table: str) -> str: ...


class LiuXinTitleRowAPI(Protocol):
    """Database row shape accepted by legacy ``MetaData.from_title_row``."""

    db: LiuXinMetadataDatabaseAPI

    def __getitem__(self, item: str) -> LiuXinFieldValue: ...


LiuXinMetaInformationAPI: TypeAlias = LiuXinMetadataAPI


__all__ = [
    "LiuXinCreatorDump",
    "LiuXinCreatorMapping",
    "LiuXinFieldKeys",
    "LiuXinFieldMapping",
    "LiuXinFieldValue",
    "LiuXinMetadataAPI",
    "LiuXinMetadataDatabaseAPI",
    "LiuXinMetaInformationAPI",
    "LiuXinPayloadKey",
    "LiuXinPayloadToID",
    "LiuXinRatingMapping",
    "LiuXinRatingValue",
    "LiuXinRowID",
    "LiuXinScalar",
    "LiuXinScalarSequence",
    "LiuXinStringSet",
    "LiuXinTitleRowAPI",
    "LiuXinValueToID",
]
