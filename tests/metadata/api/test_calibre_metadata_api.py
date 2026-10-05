"""
Verify the Calibre-compatible metadata facade and conversion boundary.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test calibre metadata api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""
from __future__ import annotations

from collections.abc import Mapping

from LiuXin_alpha.metadata.api import (
    CalibreLikeBookMetadataAPI,
)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import CalibreMetadataInputAPI, CalibreMetadataAPI
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)


class _ReadOnlyCalibreSource:
    """
    Provide the ReadOnlyCalibreSource test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ReadOnlyCalibreSource through its owning regression module::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
    """
    title = "Source Title"
    authors = ("Source Author",)

    def get_identifiers(self) -> Mapping[str, str]:
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise ReadOnlyCalibreSource.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return {"isbn": "9780306406157"}


def test_calibre_metadata_api_is_exported_from_metadata_api_root() -> None:
    """
    Verify calibre metadata api remains exported from metadata api root.

    Example:
        Exercise test calibre metadata api is exported from metadata api root through its owning regression module::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


    :return: None; the function records state or raises through its assertions.
    """
    assert "CalibreMetadataInputAPI" in metadata_api_all
    assert "CalibreMetadataAPI" in metadata_api_all
    assert "CalibreLikeBookMetadataAPI" in metadata_api_all


def test_calibre_metadata_input_api_accepts_minimal_calibre_source() -> None:
    """
    Verify calibre metadata input api accepts minimal calibre source.

    Example:
        Exercise test calibre metadata input api accepts minimal calibre source through its owning regression module::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


    :return: None; the function records state or raises through its assertions.
    """
    source: CalibreMetadataInputAPI = _ReadOnlyCalibreSource()

    assert source.get_identifiers()["isbn"] == "9780306406157"


def test_liuxin_calibre_metadata_clone_matches_calibre_metadata_api() -> None:
    """
    Verify liuxin calibre metadata clone matches calibre metadata api.

    Example:
        Exercise test liuxin calibre metadata clone matches calibre metadata api through its owning regression module::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata: CalibreMetadataAPI = calibreMetadata("Title", ["Author"])

    metadata.set_identifier("isbn", "9780306406157")
    assert metadata.has_identifier("isbn") is True
    assert metadata.get_identifiers()["isbn"] == "9780306406157"
    assert metadata.get("title") == "Title"
    assert "title" in metadata.standard_field_keys()


def test_calibre_like_book_container_matches_liuxin_calibre_like_api() -> None:
    """
    Verify calibre like book container matches liuxin calibre like api.

    Example:
        Exercise test calibre like book container matches liuxin calibre like api through its owning regression module::

            python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata: CalibreLikeBookMetadataAPI = CalibreLikeLiuXinBookMetaData(
        "Title",
        ["Author"],
    )

    metadata.set_identifier("isbn", "9780306406157")
    metadata.labels = ["new_entry"]
    metadata.tags = ["Space Opera"]
    assert metadata.has_identifier("isbn") is True
    assert "isbn" in metadata.get_identifiers()
    assert "new_entry" in metadata.labels
    assert "Space Opera" in metadata.tags
