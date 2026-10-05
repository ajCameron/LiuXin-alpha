"""
Smoke-test Catalog construction, public imports, and runtime protocol shapes.

An attribute-free database double isolates facade composition from persistence.
The tests inspect shared collaborators, candidate fields, callable writer entry
points, and field-metadata protocol membership. They provide no live database,
identity matching, schema writer, or transaction evidence; those behaviors are
covered by adjacent Catalog integration suites.
"""

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.write import (
    BaseCatalogWriter,
    CatalogColumnWriter,
    CatalogLinkWriter,
    CatalogOwnedRowOneToOneWriter,
    CatalogOwnedRowUpdate,
    CatalogTableValueLinkWriter,
    CatalogValueWriter,
    create_catalog_writer,
)
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
    """
    Supply an attribute-free stand-in for Catalog composition smoke tests.

    Constructing Catalog with this object verifies that initial wiring does not require a live
    database. It supplies no macros, rows, or schema discovery and cannot establish that subsequent
    persistence or retrieval operations work.

    Example:
        >>> database = DummyDatabase()
        >>> vars(database)
        {}
    """
    pass


def test_catalog_facade_imports_and_instantiates() -> None:
    """
    Check concrete Catalog composition against a database with no capabilities.

    Assert that legacy tools share the borrowed database and selected collaborators, six repository
    shortcuts share grouped instances, and matching/retrieval/mutation groups are present. A
    callable legacy work helper and absence of a storage attribute protect the facade shape. No
    entity operation or database query runs.

    Example:
        >>> test_catalog_facade_imports_and_instantiates()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    catalog = Catalog(DummyDatabase())
    assert catalog.add.db is catalog.db
    assert catalog.ensure.db is catalog.db
    assert catalog.apply.db is catalog.db
    assert catalog.intralink.db is catalog.db
    assert catalog.add.ensure is catalog.ensure
    assert catalog.add.apply is catalog.apply
    assert catalog.ensure.add is catalog.add
    assert catalog.apply.add is catalog.add
    assert catalog.apply.ensure is catalog.ensure
    assert callable(catalog.add.work)
    assert catalog.works is catalog.repositories.works
    assert catalog.expressions is catalog.repositories.expressions
    assert catalog.manifestations is catalog.repositories.manifestations
    assert catalog.items is catalog.repositories.items
    assert catalog.agents is catalog.repositories.agents
    assert catalog.identifiers is catalog.repositories.identifiers
    assert catalog.matching.works is not None
    assert catalog.retrieval.bundles is not None
    assert catalog.mutations.policy is not None
    assert catalog.mutations.writer is not None
    assert not hasattr(catalog, "storage")


def test_api_dataclasses_are_lightweight() -> None:
    """
    Construct metadata and identifier candidates without database dependencies.

    Assert that a title and identifier scheme remain readable after construction. The chosen
    identifier text is a smoke value; this case neither normalizes it nor establishes identifier
    validity, mapping copying, or deep immutability.

    Example:
        >>> test_api_dataclasses_are_lightweight()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    candidate = MetadataCandidate(data={"title": "Example"})
    identifier = IdentifierCandidate(identifier_type="isbn", value="9780000000000")
    assert candidate.data["title"] == "Example"
    assert identifier.identifier_type == "isbn"


def test_catalog_matches_protocol_shape() -> None:
    """
    Check runtime protocol membership and writer entry points on a composed Catalog.

    A DummyDatabase supports construction while isinstance checks verify the facade, legacy tools,
    and mutation group against their runtime-checkable protocols. Writer attributes must be
    callable. These structural checks do not invoke writes or prove signature, return-type, or
    database compatibility.

    Example:
        >>> test_catalog_matches_protocol_shape()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    catalog = Catalog(DummyDatabase())
    assert isinstance(catalog, CatalogAPI)
    assert isinstance(catalog, CatalogMetadataToolsAPI)
    assert isinstance(catalog.add, AddAPI)
    assert isinstance(catalog.ensure, EnsureAPI)
    assert isinstance(catalog.apply, ApplyAPI)
    assert isinstance(catalog.intralink, IntralinkerAPI)
    assert isinstance(catalog.mutations, CatalogMutationsAPI)
    assert callable(catalog.create_writer)
    assert callable(catalog.write)
    assert callable(catalog.write_one)
    assert callable(catalog.write_column_update)
    assert callable(catalog.write_link_update)
    assert callable(catalog.write_owned_row_update)


def test_metadata_tools_api_contracts_import() -> None:
    """
    Protect availability of the seven imported legacy metadata-tool protocols.

    Assert that Add, Apply, Ensure, Intralinker, grouped tools, backend-getter, and fingerprint
    contracts resolve to non-None imports. The case checks public availability only; it does not
    instantiate implementations or run their methods.

    Example:
        >>> test_metadata_tools_api_contracts_import()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    assert AddAPI is not None
    assert ApplyAPI is not None
    assert BackendGetterAPI is not None
    assert CatalogMetadataToolsAPI is not None
    assert EnsureAPI is not None
    assert FingerprintToolsAPI is not None
    assert IntralinkerAPI is not None


def test_catalog_link_writer_contract_imports() -> None:
    """
    Protect public imports for writer bases, concrete routes, and the schema factory.

    Require the imported column, value, link, table-value-link, owned-row writer, owned-row update,
    base writer, and factory symbols to exist. This is import coverage, independent of schema
    selection and actual database writes.

    Example:
        >>> test_catalog_link_writer_contract_imports()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    assert BaseCatalogWriter is not None
    assert CatalogColumnWriter is not None
    assert CatalogLinkWriter is not None
    assert CatalogOwnedRowOneToOneWriter is not None
    assert CatalogOwnedRowUpdate is not None
    assert CatalogTableValueLinkWriter is not None
    assert CatalogValueWriter is not None
    assert create_catalog_writer is not None


def test_field_metadata_api_contracts_import_and_match() -> None:
    """
    Check default field-metadata implementations against their public protocols.

    Construct both FieldMetadata and CalibreFieldMetadata and verify runtime protocol membership.
    The case exercises default construction and structural conformance, without validating
    individual field definitions or search behavior.

    Example:
        >>> test_field_metadata_api_contracts_import_and_match()  # doctest: +SKIP


    :return: None after the stated contract assertions pass.
    """
    assert isinstance(FieldMetadata(), FieldMetadataAPI)
    assert isinstance(CalibreFieldMetadata(), CalibreFieldMetadataAPI)
