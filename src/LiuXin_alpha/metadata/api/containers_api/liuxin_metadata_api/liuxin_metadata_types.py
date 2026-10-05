"""
Define legacy LiuXin metadata value shapes, including optional row ids and relation mappings.

Creator views map roles to name sequences, while creator dumps retain name-to-row-id
mappings. Payload keys pair a type marker with a Calibre file payload. Ratings are
numeric, field keys are abstract sets, and these aliases perform no runtime
validation.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py
"""
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
