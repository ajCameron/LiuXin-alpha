"""Smoke tests for the catalog API skeleton."""

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import CatalogAPI, IdentifierCandidate, MetadataCandidate


class DummyDatabase:
    pass


def test_catalog_facade_imports_and_instantiates() -> None:
    catalog = Catalog(DummyDatabase())
    assert catalog.works is catalog.repositories.works
    assert catalog.agents is catalog.repositories.agents
    assert catalog.matching.works is not None
    assert catalog.retrieval.bundles is not None
    assert catalog.storage.policy is not None


def test_api_dataclasses_are_lightweight() -> None:
    candidate = MetadataCandidate(data={"title": "Example"})
    identifier = IdentifierCandidate(identifier_type="isbn", value="9780000000000")
    assert candidate.data["title"] == "Example"
    assert identifier.identifier_type == "isbn"


def test_catalog_matches_protocol_shape() -> None:
    catalog = Catalog(DummyDatabase())
    assert isinstance(catalog, CatalogAPI)
