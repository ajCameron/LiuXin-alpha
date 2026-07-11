"""Metadata tool API contracts for the catalog layer."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api.add_api import AddAPI
from LiuXin_alpha.catalog.api.metadata_tools_api.apply_api import ApplyAPI
from LiuXin_alpha.catalog.api.metadata_tools_api.common import (
    DateLike,
    IsoDateLike,
    LinkPriority,
    RowMapping,
    RowOrMapping,
    RowValue,
    TextOrRow,
)
from LiuXin_alpha.catalog.api.metadata_tools_api.ensure_api import EnsureAPI
from LiuXin_alpha.catalog.api.metadata_tools_api.fingerprints_api import (
    FingerprintSubject,
    FingerprintToolsAPI,
    GenerateBookFingerprintAPI,
    GenerateOneTitleFingerprintAPI,
    GenerateTitleFingerprintAPI,
)
from LiuXin_alpha.catalog.api.metadata_tools_api.get_api import BackendGetterAPI
from LiuXin_alpha.catalog.api.metadata_tools_api.intralinker_api import IntralinkerAPI


@runtime_checkable
class CatalogMetadataToolsAPI(Protocol):
    """Grouped metadata tool API exposed by legacy database/library backends."""

    add: AddAPI
    ensure: EnsureAPI
    apply: ApplyAPI
    intralink: IntralinkerAPI


__all__ = [
    "AddAPI",
    "ApplyAPI",
    "BackendGetterAPI",
    "CatalogMetadataToolsAPI",
    "DateLike",
    "EnsureAPI",
    "FingerprintSubject",
    "FingerprintToolsAPI",
    "GenerateBookFingerprintAPI",
    "GenerateOneTitleFingerprintAPI",
    "GenerateTitleFingerprintAPI",
    "IntralinkerAPI",
    "IsoDateLike",
    "LinkPriority",
    "RowMapping",
    "RowOrMapping",
    "RowValue",
    "TextOrRow",
]
