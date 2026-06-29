"""
API contracts for legacy LiuXin extended metadata objects.

Category: high-level metadata compatibility API.
This module defines the pre-WEMI LiuXin metadata object surface used by legacy
importers, Calibre adapters, and title-row hydration paths.
"""

from __future__ import annotations

from typing import TypeAlias

from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_metadata_types import (
    LiuXinRowID,
    LiuXinScalar,
    LiuXinScalarSequence,
    LiuXinStringSet,
    LiuXinValueToID,
    LiuXinPayloadKey,
    LiuXinPayloadToID,
    LiuXinRatingMapping,
    LiuXinRatingValue,
    LiuXinCreatorMapping,
    LiuXinCreatorDump,
    LiuXinFieldValue,
    LiuXinFieldMapping,
    LiuXinFieldKeys,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_title_metadata_api import (
    LiuXinMetadataAPI,
    LiuXinMetadataDatabaseAPI,
    LiuXinTitleRowAPI,
)


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
