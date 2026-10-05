"""
Expose contracts and bounded value types for legacy title-oriented LiuXin metadata.

The surface covers creator/identifier collections, optional row ids, files/covers,
and title-row hydration. LiuXinMetaInformationAPI aliases LiuXinMetadataAPI; WEMI
projection contracts have separate owning modules.

Example:
    >>> LiuXinMetaInformationAPI is LiuXinMetadataAPI
    True
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
