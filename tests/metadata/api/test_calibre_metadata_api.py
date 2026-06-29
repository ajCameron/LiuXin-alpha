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
    title = "Source Title"
    authors = ("Source Author",)

    def get_identifiers(self) -> Mapping[str, str]:
        return {"isbn": "9780306406157"}


def test_calibre_metadata_api_is_exported_from_metadata_api_root() -> None:
    assert "CalibreMetadataInputAPI" in metadata_api_all
    assert "CalibreMetadataAPI" in metadata_api_all
    assert "CalibreLikeBookMetadataAPI" in metadata_api_all


def test_calibre_metadata_input_api_accepts_minimal_calibre_source() -> None:
    source: CalibreMetadataInputAPI = _ReadOnlyCalibreSource()

    assert source.get_identifiers()["isbn"] == "9780306406157"


def test_liuxin_calibre_metadata_clone_matches_calibre_metadata_api() -> None:
    metadata: CalibreMetadataAPI = calibreMetadata("Title", ["Author"])

    metadata.set_identifier("isbn", "9780306406157")
    assert metadata.has_identifier("isbn") is True
    assert metadata.get_identifiers()["isbn"] == "9780306406157"
    assert metadata.get("title") == "Title"
    assert "title" in metadata.standard_field_keys()


def test_calibre_like_book_container_matches_liuxin_calibre_like_api() -> None:
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
