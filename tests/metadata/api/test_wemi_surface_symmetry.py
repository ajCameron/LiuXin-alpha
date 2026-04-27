from __future__ import annotations

import importlib

import pytest


LEVELS = [
    {
        "level": "work",
        "api_identity_module": "work_identity_api",
        "api_identity_class": "WorkIdentityAPI",
        "api_metadata_module": "work_metadata_api",
        "api_metadata_class": "WorkMetadataAPI",
        "impl_identity_module": "work_container",
        "impl_identity_class": "WorkIdentity",
        "impl_metadata_module": "work_metadata_container",
        "impl_metadata_class": "WorkMetadata",
        "hydrator_module": "work_metadata_hydrator",
        "hydrator_class": "WorkMetadataHydrator",
        "source_class": "WorkMetadataGetterAPI",
    },
    {
        "level": "expression",
        "api_identity_module": "expression_identity_api",
        "api_identity_class": "ExpressionIdentityAPI",
        "api_metadata_module": "expression_metadata_api",
        "api_metadata_class": "ExpressionMetadataAPI",
        "impl_identity_module": "expression_container",
        "impl_identity_class": "ExpressionIdentity",
        "impl_metadata_module": "expression_metadata_container",
        "impl_metadata_class": "ExpressionMetadata",
        "hydrator_module": "expression_metadata_hydrator",
        "hydrator_class": "ExpressionMetadataHydrator",
        "source_class": "ExpressionMetadataGetterAPI",
    },
    {
        "level": "manifestation",
        "api_identity_module": "manifestation_identity_api",
        "api_identity_class": "ManifestationIdentityAPI",
        "api_metadata_module": "manifestation_metadata_api",
        "api_metadata_class": "ManifestationMetadataAPI",
        "impl_identity_module": "manifestation_container",
        "impl_identity_class": "ManifestationIdentity",
        "impl_metadata_module": "manifestation_metadata_container",
        "impl_metadata_class": "ManifestationMetadata",
        "hydrator_module": "manifestation_metadata_hydrator",
        "hydrator_class": "ManifestationMetadataHydrator",
        "source_class": "ManifestationMetadataGetterAPI",
    },
    {
        "level": "item",
        "api_identity_module": "item_identity_api",
        "api_identity_class": "ItemIdentityAPI",
        "api_metadata_module": "item_metadata_api",
        "api_metadata_class": "ItemMetadataAPI",
        "impl_identity_module": "item_container",
        "impl_identity_class": "ItemIdentity",
        "impl_metadata_module": "item_metadata_container",
        "impl_metadata_class": "ItemMetadata",
        "hydrator_module": "item_metadata_hydrator",
        "hydrator_class": "ItemMetadataHydrator",
        "source_class": "ItemMetadataGetterAPI",
    },
]


@pytest.mark.parametrize("entry", LEVELS, ids=[entry["level"] for entry in LEVELS])
def test_core_wemi_surfaces_are_symmetrical(entry: dict[str, str]) -> None:
    api_identity_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.{entry['api_identity_module']}"
    )
    api_metadata_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.{entry['api_metadata_module']}"
    )
    impl_identity_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{entry['impl_identity_module']}"
    )
    impl_metadata_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{entry['impl_metadata_module']}"
    )
    hydrator_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{entry['hydrator_module']}"
    )
    source_root = importlib.import_module(
        "LiuXin_alpha.metadata.api.metadata_db_source.wemi_sources"
    )

    assert hasattr(api_identity_module, entry["api_identity_class"])
    assert hasattr(api_metadata_module, entry["api_metadata_class"])
    assert hasattr(impl_identity_module, entry["impl_identity_class"])
    assert hasattr(impl_metadata_module, entry["impl_metadata_class"])
    assert hasattr(hydrator_module, entry["hydrator_class"])
    assert hasattr(source_root, entry["source_class"])

    metadata_class = getattr(impl_metadata_module, entry["impl_metadata_class"])
    assert hasattr(metadata_class, "from_mapping")
    assert hasattr(metadata_class, "from_database")
    assert hasattr(metadata_class, "to_mapping")
