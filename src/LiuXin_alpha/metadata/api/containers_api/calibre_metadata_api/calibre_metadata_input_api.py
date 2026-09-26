
"""
Define the minimum readable metadata shape accepted by Calibre adapters.

Only title, authors, and an identifier accessor are required; mutation and richer
optional fields belong to other contracts.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""


from __future__ import annotations

from typing import Protocol, Sequence

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
)


class CalibreMetadataInputAPI(Protocol):
    """
    Describe a source with optional title/authors and a readable identifier mapping.

    The protocol imposes no setter requirements or runtime validation.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
    """

    title: str | None
    authors: Sequence[str] | None

    def get_identifiers(
        self,
    ) -> CalibreIdentifierSnapshot | CalibreIdentifierMapping:
        """
        Read identifier schemes and values from the source without requiring mutation access.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Identifier snapshot or input mapping in the shared supported value shapes.
        """
