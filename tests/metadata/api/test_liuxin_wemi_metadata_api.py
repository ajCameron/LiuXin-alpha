from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    LiuXinWEMIAPI,
    LiuXinWEMIMetadataAPI,
    OPFMetadataSource,
    WemiLevel,
)
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata


def test_liuxin_wemi_metadata_api_is_exported_from_metadata_api_root() -> None:
    assert "LiuXinWEMIMetadataAPI" in metadata_api_all
    assert "LiuXinWEMIAPI" in metadata_api_all
    assert "OPFMetadataSource" in metadata_api_all
    assert LiuXinWEMIAPI is LiuXinWEMIMetadataAPI


def test_wemi_level_alias_allows_item_centered_stack_names() -> None:
    level: WemiLevel = "item"
    assert level == "item"


def test_opf_source_alias_covers_bytes_and_paths() -> None:
    raw_source: OPFMetadataSource = b"<package />"
    path_source: OPFMetadataSource = "metadata.opf"

    assert raw_source.startswith(b"<")
    assert path_source.endswith(".opf")


def test_wemi_metadata_api_exposes_current_operational_methods() -> None:
    expected_methods = (
        "sync_legacy_genres_from_wemi",
        "sync_legacy_subjects_from_wemi",
        "sync_legacy_series_from_wemi",
        "sync_legacy_identifiers_from_wemi",
        "from_database",
        "from_opf",
    )

    for method_name in expected_methods:
        assert hasattr(LiuXinWEMIMetadataAPI, method_name)
        assert hasattr(LiuXinWEMIMetadata, method_name)


def test_concrete_wemi_metadata_supports_extended_legacy_sync_contract() -> None:
    metadata: LiuXinWEMIMetadataAPI = LiuXinWEMIMetadata(
        "Protocol Title",
        ["Protocol Author"],
    )

    assert metadata.sync_legacy_genres_from_wemi() == ()
    assert metadata.sync_legacy_subjects_from_wemi() == ()
    assert metadata.sync_legacy_series_from_wemi() == ()
    assert metadata.sync_legacy_identifiers_from_wemi() == ()
