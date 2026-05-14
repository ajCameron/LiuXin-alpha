from __future__ import annotations

from LiuXin_alpha.metadata.api import (
    LazyLiuXinWEMIAPI,
    LazyLiuXinWEMIMetadataAPI,
    LiuXinWEMIAPI,
    LiuXinWEMIMetadataAPI,
    OPFMetadataSource,
    WemiLevel,
)
from LiuXin_alpha.metadata.api import __all__ as metadata_api_all
from LiuXin_alpha.metadata.containers import LazyLiuXinWEMIMetadata, LiuXinWEMIMetadata


def test_liuxin_wemi_metadata_api_is_exported_from_metadata_api_root() -> None:
    assert "LiuXinWEMIMetadataAPI" in metadata_api_all
    assert "LiuXinWEMIAPI" in metadata_api_all
    assert "LazyLiuXinWEMIMetadataAPI" in metadata_api_all
    assert "LazyLiuXinWEMIAPI" in metadata_api_all
    assert "OPFMetadataSource" in metadata_api_all
    assert LiuXinWEMIAPI is LiuXinWEMIMetadataAPI
    assert LazyLiuXinWEMIAPI is LazyLiuXinWEMIMetadataAPI


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


def test_lazy_wemi_metadata_api_exposes_lazy_hydration_surface() -> None:
    expected_methods = (
        "install_lazy_value_to_id",
        "install_lazy_relation_loader",
        "hydrate_field",
        "force_hydrate",
        "lazy_fields",
        "is_lazy_field_loaded",
        "lazy_legacy_terms_from_relation",
    )

    for method_name in expected_methods:
        assert hasattr(LazyLiuXinWEMIMetadataAPI, method_name)
        assert hasattr(LazyLiuXinWEMIMetadata, method_name)


def test_concrete_lazy_wemi_metadata_supports_lazy_contract() -> None:
    metadata: LazyLiuXinWEMIMetadataAPI = LazyLiuXinWEMIMetadata(
        "Lazy Protocol Title",
        ["Lazy Protocol Author"],
    )
    metadata.install_lazy_value_to_id("tags", lambda: {"deferred-tag": 7})

    assert metadata.is_lazy_field_loaded("tags") is False
    assert "tags" in metadata.lazy_fields()
    hydrated = metadata.hydrate_field("tags")

    assert list(hydrated) == ["deferred-tag"]
    assert metadata.is_lazy_field_loaded("tags") is True
    assert metadata.force_hydrate() is metadata
