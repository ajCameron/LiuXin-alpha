"""Compatibility contracts for Catalog helpers that operate on database Rows.

These helpers predate the mapping/ID-oriented repository API and remain useful
when a workflow already holds ``RowAPI`` objects:

``add``
    Create concrete metadata or WEMI rows.
``ensure``
    Resolve conventional value rows, creating when absent.
``apply``
    Link metadata rows/values to an existing resource row.
``intralink``
    Relate two rows from the same metadata family.

New code which does not need Row objects should generally prefer
``catalog.repositories`` and ``catalog.mutations``.
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
    """Row-oriented metadata helpers exposed directly by Catalog.

    Example::

        work_row = catalog.add.work(work_title="Frankenstein")
        author_row = catalog.ensure.creator_blind("Mary Shelley")
        catalog.apply.creator(
            resource_row=work_row,
            creator_row=author_row,
            creator_role="author",
        )
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
