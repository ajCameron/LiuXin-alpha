from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    LiuXinMetaInformationAPI,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import LiuXinMetadataAPI
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)


def test_liuxin_metadata_api_is_exported_from_metadata_api_root() -> None:
    assert "LiuXinMetadataAPI" in metadata_api_all
    assert "LiuXinMetaInformationAPI" in metadata_api_all


def test_calibre_like_container_supports_legacy_liuxin_metadata_contract() -> None:
    metadata: LiuXinMetadataAPI = CalibreLikeLiuXinBookMetaData(
        "Title",
        ["Author"],
    )

    metadata.add_creators({"editors": ["Editor"]})
    metadata.add_identifiers({"isbn": "9780306406157"})
    metadata.add_internal_identifiers({"uuid": "legacy-uuid"})
    metadata.labels = ["new_entry"]
    metadata.direct_add("legacy_extra", "value", key_check=False)

    assert "Author" in metadata.get_authors_copy()
    assert "editors" in metadata.get_creators_dump()
    assert metadata.has_identifier("isbn") is True
    assert "uuid" in metadata.get_internal_identifiers()
    assert "new_entry" in metadata.labels
    assert metadata.direct_get("legacy_extra") == "value"
    assert "title" in metadata.standard_field_keys()


def test_liuxin_meta_information_alias_tracks_metadata_contract() -> None:
    metadata: LiuXinMetaInformationAPI = CalibreLikeLiuXinBookMetaData(
        "Alias Title",
        ["Alias Author"],
    )

    assert metadata.get("title") == "Alias Title"
