"""
Export structural contracts and shared types for row-oriented metadata helpers.

Add constructs Rows, Ensure resolves or creates conventional values, Apply
links metadata to resource Rows, and Intralinker relates metadata Rows.
These compatibility interfaces describe composed helpers, not a database
connection or a transaction boundary.
"""

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
    """
    Describe the four legacy metadata helpers grouped on a Catalog.

    Runtime protocol checks inspect attribute availability; they do not execute
    helper methods or validate their signatures.

    Example:
        A row-oriented workflow can create a Work with catalog.add.work, ensure a
        Creator, then link that Creator with catalog.apply.creator.
    """

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
