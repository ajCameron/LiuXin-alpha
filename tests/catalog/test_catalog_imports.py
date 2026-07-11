"""Smoke tests for the catalog API skeleton."""

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.field_metadata import CalibreFieldMetadata, FieldMetadata
from LiuXin_alpha.catalog.api import (
    AddAPI,
    ApplyAPI,
    BackendGetterAPI,
    CalibreFieldMetadataAPI,
    CatalogAPI,
    CatalogMetadataToolsAPI,
    CatalogMutationsAPI,
    EnsureAPI,
    FieldMetadataAPI,
    FingerprintToolsAPI,
    IdentifierCandidate,
    IntralinkerAPI,
    MetadataCandidate,
)


class DummyDatabase:
    pass


def test_catalog_facade_imports_and_instantiates() -> None:
    catalog = Catalog(DummyDatabase())
    assert catalog.works is catalog.repositories.works
    assert catalog.agents is catalog.repositories.agents
    assert catalog.matching.works is not None
    assert catalog.retrieval.bundles is not None
    assert catalog.mutations.policy is not None
    assert catalog.mutations.writer is not None
    assert not hasattr(catalog, "storage")


def test_api_dataclasses_are_lightweight() -> None:
    candidate = MetadataCandidate(data={"title": "Example"})
    identifier = IdentifierCandidate(identifier_type="isbn", value="9780000000000")
    assert candidate.data["title"] == "Example"
    assert identifier.identifier_type == "isbn"


def test_catalog_matches_protocol_shape() -> None:
    catalog = Catalog(DummyDatabase())
    assert isinstance(catalog, CatalogAPI)
    assert isinstance(catalog.mutations, CatalogMutationsAPI)


def test_metadata_tools_api_contracts_import() -> None:
    assert AddAPI is not None
    assert ApplyAPI is not None
    assert BackendGetterAPI is not None
    assert CatalogMetadataToolsAPI is not None
    assert EnsureAPI is not None
    assert FingerprintToolsAPI is not None
    assert IntralinkerAPI is not None


def test_field_metadata_api_contracts_import_and_match() -> None:
    assert isinstance(FieldMetadata(), FieldMetadataAPI)
    assert isinstance(CalibreFieldMetadata(), CalibreFieldMetadataAPI)
