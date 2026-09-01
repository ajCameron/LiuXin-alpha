
"""
Minimal metadata shape which can be read from calibre adapters.
"""


from __future__ import annotations

from typing import Protocol, Sequence

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
)


class CalibreMetadataInputAPI(Protocol):
    """Minimum metadata object shape that can be read from Calibre adapters."""

    title: str | None
    authors: Sequence[str] | None

    def get_identifiers(
        self,
    ) -> CalibreIdentifierSnapshot | CalibreIdentifierMapping:
        """
        Returns all the identifiers for the given metadata object.

        :return:
        """
