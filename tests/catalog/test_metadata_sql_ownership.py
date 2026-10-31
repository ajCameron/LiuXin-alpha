"""Verify semantic ownership and rollback after retiring metadata SQL adapters."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from LiuXin_alpha.catalog.api import MetadataCandidate
from LiuXin_alpha.catalog.api.common import CatalogNotFoundError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.metadata.identifiers import normalise_identifier_scheme


@pytest.fixture
def catalog(db):
    return Catalog(db)


@pytest.mark.parametrize("repository", ["tags", "series"])
def test_lookup_creation_and_deletion_use_catalog_identity_and_transactions(
    catalog, repository
):
    repo = getattr(catalog, repository)
    entity_id = repo.match_or_create(MetadataCandidate({"name": "Café History"}))
    assert repo.resolve("  CAFÉ   HISTORY  ")[repo.id_column] == entity_id
    assert (
        repo.match_or_create(MetadataCandidate({"name": "CAFE\u0301 HISTORY"}))
        == entity_id
    )
    with pytest.raises(RuntimeError, match="rollback"):
        with catalog.db.macros.transaction():
            repo.delete(entity_id)
            assert repo.resolve("Café History") is None
            raise RuntimeError("rollback")
    assert repo.resolve("Café History")[repo.id_column] == entity_id
    repo.delete(entity_id)
    assert repo.resolve("Café History") is None


@pytest.mark.parametrize("level", ["work", "expression", "manifestation", "item"])
def test_primary_identifier_preserves_alternatives_and_other_owners(catalog, level):
    repo = getattr(
        catalog,
        {
            "work": "works",
            "expression": "expressions",
            "manifestation": "manifestations",
            "item": "items",
        }[level],
    )
    field = {
        "work": "title",
        "expression": "label",
        "manifestation": "edition_statement",
        "item": "inventory_code",
    }[level]
    owner = repo.create({field: "Identifier owner"})
    other = repo.create({field: "Other owner"})
    ids = catalog.identifiers
    first = ids.set_primary_for_wemi(
        level=level,
        entity_id=owner,
        scheme=" UUID ",
        value="12345678-1234-5678-1234-567812345678",
    )
    assert (
        ids.set_primary_for_wemi(
            level=level,
            entity_id=owner,
            scheme="uuid",
            value="12345678123456781234567812345678",
        )
        == first
    )
    other_id = ids.set_primary_for_wemi(
        level=level,
        entity_id=other,
        scheme="uuid",
        value="12345678-1234-5678-1234-567812345678",
    )
    second = ids.set_primary_for_wemi(
        level=level,
        entity_id=owner,
        scheme="uuid",
        value="12345678-1234-5678-1234-567812345679",
    )
    assert first != second != other_id
    assert ids.require(first)["entity_identifier_is_primary"] == 0
    assert ids.require(second)["entity_identifier_is_primary"] == 1
    assert ids.require(other_id)["entity_identifier_is_primary"] == 1
    assert {row["entity_identifier_id"] for row in ids.list_for_level(level)} >= {
        first,
        second,
        other_id,
    }
    ids.clear_for_wemi(level=level, entity_id=owner, scheme="UUID")
    assert not ids.list_for_wemi(level=level, entity_id=owner)
    assert ids.require(other_id)["entity_identifier_value"] == (
        "12345678-1234-5678-1234-567812345678"
    )


def test_identifier_validation_and_clearing(catalog):
    owner = catalog.works.create({"title": "Validation"})
    ids = catalog.identifiers
    for scheme, value in (
        ("isbn10", "0306406152"),
        ("isbn13", "9780306406157"),
        ("doi", "10.1000/example"),
    ):
        ids.set_primary_for_wemi(
            level="work", entity_id=owner, scheme=scheme, value=value
        )
    isbn_id = next(
        row["entity_identifier_id"]
        for row in ids.list_for_wemi(level="work", entity_id=owner)
        if row["entity_identifier_scheme"] == "isbn13"
    )
    catalog.db.macros.update_row(
        "entity_identifiers", isbn_id, {"entity_identifier_scheme": "isbn_13"}
    )
    assert (
        ids.set_primary_for_wemi(
            level="work", entity_id=owner, scheme="ISBN-13", value="978-0-306-40615-7"
        )
        == isbn_id
    )
    assert ids.require(isbn_id)["entity_identifier_scheme"] == "isbn13"
    before = ids.list_for_wemi(level="work", entity_id=owner)
    for scheme, value in (
        ("isbn", "1234567890123"),
        ("uuid", "broken"),
        ("doi", "broken"),
        ("isbn", ""),
        ("isbn", False),
        (None, None),
    ):
        with pytest.raises((TypeError, ValueError)):
            ids.set_primary_for_wemi(
                level="work", entity_id=owner, scheme=scheme, value=value
            )
        assert ids.list_for_wemi(level="work", entity_id=owner) == before
    ids.set_primary_for_wemi(level="work", entity_id=owner, scheme="ISBN", value=None)
    assert [
        r["entity_identifier_scheme"]
        for r in ids.list_for_wemi(level="work", entity_id=owner)
    ] == ["doi"]
    for operation in (
        lambda: ids.clear_for_wemi(level="work", entity_id=999999),
        lambda: ids.set_primary_for_wemi(
            level="work", entity_id=999999, scheme="doi", value="10.1000/missing"
        ),
    ):
        with pytest.raises(CatalogNotFoundError):
            operation()


def test_primary_identifier_rolls_back_promotion_failure(catalog, monkeypatch):
    owner = catalog.works.create({"title": "Rollback"})
    ids = catalog.identifiers
    original = ids.set_primary_for_wemi(
        level="work", entity_id=owner, scheme="isbn", value="9780306406157"
    )

    def fail(*args, **kwargs):
        raise RuntimeError("insertion failed")

    with monkeypatch.context() as patch:
        patch.setattr(ids, "create", fail)
        with pytest.raises(RuntimeError, match="insertion failed"):
            ids.set_primary_for_wemi(
                level="work", entity_id=owner, scheme="isbn", value="9780140328721"
            )
    assert ids.require(original)["entity_identifier_is_primary"] == 1
    with pytest.raises(RuntimeError, match="outer rollback"):
        with catalog.db.macros.transaction():
            ids.clear_for_wemi(level="work", entity_id=owner)
            raise RuntimeError("outer rollback")
    assert ids.require(original)["entity_identifier_is_primary"] == 1


def test_original_language_adapter_uses_work_owner(catalog):
    db = catalog.db
    work = catalog.works.create({"title": "Language ownership"})
    english = catalog.languages.exact("English").entity_id
    french = catalog.languages.exact("French").entity_id
    expression = catalog.expressions.create({"language_id": french})
    language_row = db.get_row_from_id("languages", row_id=english)
    for table in ("works", "titles"):
        catalog.apply.primary_language(
            language_row, db.get_row_from_id(table, row_id=work)
        )
        assert catalog.works.require(work)["work_original_language_id"] == english
    assert catalog.expressions.require(expression)["expression_language_id"] == french
    with pytest.raises(CatalogNotFoundError):
        catalog.works.set_original_language(work, 999999)
    assert catalog.works.require(work)["work_original_language_id"] == english
    with pytest.raises(TypeError):
        catalog.apply.primary_language(
            language_row, db.get_row_from_id("expressions", row_id=expression)
        )
    with pytest.raises(RuntimeError):
        with db.macros.transaction():
            catalog.works.set_original_language(work, french)
            raise RuntimeError("rollback")
    assert catalog.works.require(work)["work_original_language_id"] == english
    catalog.works.set_original_language(work, None)
    assert catalog.works.require(work)["work_original_language_id"] is None


def test_legacy_identifier_cache_uses_catalog_and_rolls_back(catalog):
    from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.specific_one_to_many_tables import (
        CalibreIdentifiersTable,
    )

    table = object.__new__(CalibreIdentifiersTable)
    first = catalog.works.create({"title": "Cache one"})
    second = catalog.works.create({"title": "Cache two"})
    table.update_db(
        {first: {"isbn": ["9780306406157", "9780140328721"], "doi": ["10.1000/a:b"]}},
        catalog.db,
    )
    table.read_maps(catalog.db)
    assert table.book_col_map[first]["isbn"] == {"9780306406157", "9780140328721"}
    table.write_to_db(first, catalog.db)
    before = catalog.identifiers.list_for_wemi(level="work", entity_id=first)
    with pytest.raises(ValueError):
        table.update_db({first: ["0306406152"], second: "1234567890123"}, catalog.db)
    assert catalog.identifiers.list_for_wemi(level="work", entity_id=first) == before
    table.update_db({first: "doi:10.1000/another:part"}, catalog.db)
    assert (
        catalog.identifiers.primary_values_for_wemi(level="work", entity_id=first)[
            "doi"
        ]
        == "10.1000/another:part"
    )


def test_upward_dependency_boundary():
    root = Path(__file__).resolve().parents[2] / "src" / "LiuXin_alpha"
    # Existing version metadata and custom-column dependencies are separate debt.
    allowed = {
        (
            "database_driver_plugins/SQLite/__init__.py",
            "LiuXin_alpha.catalog.legacy_versions",
        ),
        (
            "database_driver_plugins/SQLite_apsw/__init__.py",
            "LiuXin_alpha.catalog.legacy_versions",
        ),
        ("api/custom_columns_api.py", "LiuXin_alpha.catalog.api.field_metadata_api"),
        ("custom_columns/custom_columns.py", "LiuXin_alpha.catalog.field_metadata"),
    }
    for path in (root / "databases").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            modules = (
                [node.module]
                if isinstance(node, ast.ImportFrom)
                else [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else []
            )
            for module in modules:
                if module and module.startswith("LiuXin_alpha.catalog"):
                    assert (
                        path.relative_to(root / "databases").as_posix(),
                        module,
                    ) in allowed
    assert normalise_identifier_scheme(" ISBN-13 ") == "isbn13"
    assert normalise_identifier_scheme("Custom scheme") == "custom_scheme"
