from __future__ import annotations

from datetime import datetime
from typing import TypeAlias, Sequence, Mapping, AbstractSet

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreFilePayload,
    CalibreIdentifierSnapshot,
    CalibreUserMetadata,
)

LiuXinRowID: TypeAlias = int | None
LiuXinScalar: TypeAlias = str | int | float | bool | bytes | datetime | None
LiuXinScalarSequence: TypeAlias = Sequence[LiuXinScalar]
LiuXinStringSet: TypeAlias = AbstractSet[str]
LiuXinValueToID: TypeAlias = Mapping[str, LiuXinRowID]
LiuXinPayloadKey: TypeAlias = tuple[str, CalibreFilePayload]
LiuXinPayloadToID: TypeAlias = Mapping[LiuXinPayloadKey, LiuXinRowID]
LiuXinRatingValue: TypeAlias = int | float
LiuXinRatingMapping: TypeAlias = Mapping[str, LiuXinRatingValue]
LiuXinCreatorMapping: TypeAlias = Mapping[str, Sequence[str]]
LiuXinCreatorDump: TypeAlias = Mapping[str, LiuXinValueToID]
LiuXinFieldValue: TypeAlias = (
    LiuXinScalar
    | LiuXinScalarSequence
    | LiuXinStringSet
    | LiuXinValueToID
    | LiuXinPayloadToID
    | LiuXinRatingMapping
    | CalibreIdentifierSnapshot
    | CalibreUserMetadata
)
LiuXinFieldMapping: TypeAlias = Mapping[str, LiuXinFieldValue]
LiuXinFieldKeys: TypeAlias = AbstractSet[str]
