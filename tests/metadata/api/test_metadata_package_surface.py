from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("module_name", "expected_names"),
    [
        (
            "LiuXin_alpha.metadata.api",
            [
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
            "LiuXin_alpha.metadata.api.metadata_container_api",
            [
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
