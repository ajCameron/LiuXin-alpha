from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("module_name", "expected_names"),
    [
        (
            "LiuXin_alpha.metadata.api",
            [
                "AssetReplicaIdentityAPI",
                "AssetReplicaMetadataAPI",
                "DigitalAssetIdentityAPI",
                "DigitalAssetMetadataAPI",
                "WorkIdentityAPI",
                "WorkMetadataAPI",
                "ExpressionIdentityAPI",
                "ExpressionMetadataAPI",
                "ManifestationIdentityAPI",
                "ManifestationMetadataAPI",
                "ItemIdentityAPI",
                "ItemMetadataAPI",
                "AgentIdentityAPI",
                "AgentProfileAPI",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.metadata_container_api",
            [
                "AssetReplicaIdentityAPI",
                "AssetReplicaMetadataAPI",
                "DigitalAssetIdentityAPI",
                "DigitalAssetMetadataAPI",
                "WorkIdentityAPI",
                "WorkMetadataAPI",
                "ExpressionIdentityAPI",
                "ExpressionMetadataAPI",
                "ManifestationIdentityAPI",
                "ManifestationMetadataAPI",
                "ItemIdentityAPI",
                "ItemMetadataAPI",
                "AgentIdentityAPI",
                "AgentProfileAPI",
            ],
        ),
        (
            "LiuXin_alpha.metadata.containers",
            [
                "WorkIdentity",
                "WorkMetadata",
                "ExpressionIdentity",
                "ExpressionMetadata",
                "ManifestationIdentity",
                "ManifestationMetadata",
                "ItemIdentity",
                "ItemMetadata",
                "AgentIdentity",
                "AgentProfile",
                "WorkTitlesContainer",
                "WorkNotesContainer",
                "WorkLabelsContainer",
                "WorkGenresContainer",
                "WorkSubjectsContainer",
                "WorkIdentifiersContainer",
                "WorkResourcesContainer",
                "WorkSeriesEntriesContainer",
                "WorkRatingsContainer",
                "WorkDatesContainer",
                "WorkLanguagesContainer",
            ],
        ),
        (
            "LiuXin_alpha.metadata.containers.metadata_containers",
            [
                "WorkIdentity",
                "WorkMetadata",
                "ExpressionIdentity",
                "ExpressionMetadata",
                "ManifestationIdentity",
                "ManifestationMetadata",
                "ItemIdentity",
                "ItemMetadata",
                "AgentIdentity",
                "AgentProfile",
                "WorkTitlesContainer",
                "WorkNotesContainer",
                "WorkLabelsContainer",
                "WorkGenresContainer",
                "WorkSubjectsContainer",
                "WorkIdentifiersContainer",
                "WorkResourcesContainer",
                "WorkSeriesEntriesContainer",
                "WorkRatingsContainer",
                "WorkDatesContainer",
                "WorkLanguagesContainer",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.metadata_db_source",
            ["DBMetadataSourceAPI"],
        ),
    ],
)
def test_metadata_package_import_surface(module_name: str, expected_names: list[str]) -> None:
    module = importlib.import_module(module_name)
    for expected_name in expected_names:
        assert hasattr(module, expected_name), f"{module_name} is missing {expected_name}"


def test_metadata_api_root_matches_metadata_container_api_root() -> None:
    api_root = importlib.import_module("LiuXin_alpha.metadata.api")
    container_api_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.metadata_container_api"
    )
    assert api_root.__all__ == container_api_root.__all__


def test_metadata_container_root_matches_metadata_containers_root() -> None:
    container_root = importlib.import_module("LiuXin_alpha.metadata.containers")
    metadata_containers_root = importlib.import_module(
        "LiuXin_alpha.metadata.containers.metadata_containers"
    )
    assert container_root.__all__ == metadata_containers_root.__all__


@pytest.mark.parametrize(
    ("module_name", "concrete_names"),
    [
        (
            "LiuXin_alpha.metadata.api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.metadata_container_api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
        (
            "LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api",
            [
                "WorkMetadata",
                "WorkTitle",
                "WorkLanguagesContainer",
                "WorkAgentCredit",
            ],
        ),
    ],
)
def test_metadata_api_surfaces_do_not_export_concrete_containers(
    module_name: str,
    concrete_names: list[str],
) -> None:
    module = importlib.import_module(module_name)
    for concrete_name in concrete_names:
        assert not hasattr(module, concrete_name), f"{module_name} still exports {concrete_name}"


@pytest.mark.parametrize(
    "module_name",
    [
        "LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.expression_containers.expression_metadata_api",
        "LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api",
    ],
)
def test_metadata_leaf_all_names_exist(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for exported_name in module.__all__:
        assert hasattr(module, exported_name), f"{module_name} exports missing {exported_name}"
