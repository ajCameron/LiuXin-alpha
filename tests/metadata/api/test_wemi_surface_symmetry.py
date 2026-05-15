from __future__ import annotations

import importlib
import inspect

from typing import get_args

import pytest


LEVELS = [
    {
        "level": "work",
        "api_identity_module": "work_containers.work_identity_api",
        "api_identity_class": "WorkIdentityAPI",
        "api_metadata_module": "work_containers.work_metadata_api",
        "api_metadata_class": "WorkMetadataAPI",
        "api_relation_key": "WorkRelationKey",
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
        "api_identity_module": "expression_containers.expression_identity_api",
        "api_identity_class": "ExpressionIdentityAPI",
        "api_metadata_module": "expression_containers.expression_metadata_api",
        "api_metadata_class": "ExpressionMetadataAPI",
        "api_relation_key": "ExpressionRelationKey",
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
        "api_identity_module": "manifestation_containers.manifestation_identity_api",
        "api_identity_class": "ManifestationIdentityAPI",
        "api_metadata_module": "manifestation_containers.manifestation_metadata_api",
        "api_metadata_class": "ManifestationMetadataAPI",
        "api_relation_key": "ManifestationRelationKey",
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
        "api_identity_module": "item_containers.item_identity_api",
        "api_identity_class": "ItemIdentityAPI",
        "api_metadata_module": "item_containers.item_metadata_api",
        "api_metadata_class": "ItemMetadataAPI",
        "api_relation_key": "ItemRelationKey",
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
        f"LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.{entry['api_identity_module']}"
    )
    api_metadata_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.{entry['api_metadata_module']}"
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
        "LiuXin_alpha.metadata.api.from_database_api.wemi_sources"
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

    api_metadata_class = getattr(api_metadata_module, entry["api_metadata_class"])
    relation_key_alias = getattr(api_metadata_module, entry["api_relation_key"])
    api_doc = inspect.getdoc(api_metadata_class) or ""
    assert "relation_key" in api_doc
    assert "RELATION_KEYS" in api_doc
    assert "physical database table" in api_doc
    assert hasattr(api_metadata_module, entry["api_relation_key"])
    assert get_args(relation_key_alias) == api_metadata_class.relation_names()
    assert entry["api_relation_key"] in str(inspect.signature(api_metadata_class.relation_names).return_annotation)
    validate_signature = inspect.signature(api_metadata_class.validate_relation_name)
    assert validate_signature.parameters["relation_key"].annotation == "str"
    assert validate_signature.return_annotation == entry["api_relation_key"]

    for method_name in (
        "relation_names",
        "validate_relation_name",
        "relation_cardinality",
        "validate_relation_links",
        "get_relation_links",
        "set_relation_links",
        "add_relation_link",
        "remove_relation_link",
        "primary_relation_link",
        "primary_related",
        "set_primary_relation_link",
        "get_relation_link_by_id",
        "upsert_relation_link",
        "remove_relation_link_by_id",
        "get_related",
        "set_related",
        "add_related",
        "clear_related",
    ):
        assert hasattr(api_metadata_class, method_name)

    for relation_name in api_metadata_class.relation_names():
        assert isinstance(getattr(api_metadata_class, relation_name), property)

    for projection_name in ("values", "text"):
        assert isinstance(getattr(api_metadata_class, projection_name), property)
        assert isinstance(getattr(metadata_class, projection_name), property)


@pytest.mark.parametrize("entry", LEVELS, ids=[entry["level"] for entry in LEVELS])
def test_core_wemi_relation_contract_uses_relation_key_parameter(entry: dict[str, str]) -> None:
    api_metadata_module = importlib.import_module(
        f"LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.{entry['api_metadata_module']}"
    )
    impl_metadata_module = importlib.import_module(
        f"LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.{entry['impl_metadata_module']}"
    )
    api_metadata_class = getattr(api_metadata_module, entry["api_metadata_class"])
    impl_metadata_class = getattr(impl_metadata_module, entry["impl_metadata_class"])
    relation_key_alias = getattr(api_metadata_module, entry["api_relation_key"])

    for metadata_class in (api_metadata_class, impl_metadata_class):
        for method_name in (
            "get_relation_links",
            "set_relation_links",
        ):
            parameters = inspect.signature(getattr(metadata_class, method_name)).parameters
            assert "relation_key" in parameters
            assert "relation" not in parameters
            assert parameters["relation_key"].annotation == entry["api_relation_key"]

    for method_name in (
        "relation_cardinality",
        "validate_relation_links",
        "get_relation_links",
        "set_relation_links",
        "add_relation_link",
        "remove_relation_link",
        "primary_relation_link",
        "primary_related",
        "set_primary_relation_link",
        "get_relation_link_by_id",
        "upsert_relation_link",
        "remove_relation_link_by_id",
        "get_related",
        "set_related",
        "add_related",
        "clear_related",
    ):
        parameters = inspect.signature(getattr(api_metadata_class, method_name)).parameters
        assert "relation_key" in parameters
        assert "relation" not in parameters
        assert parameters["relation_key"].annotation == entry["api_relation_key"]
