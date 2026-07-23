"""Tests for schema-driven catalog writer construction."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.write import (
    CatalogColumnUpdate,
    CatalogColumnWriter,
    CatalogOwnedRowOneToOneWriter,
    CatalogOwnedRowUpdate,
    CatalogTableValueLinkWriter,
    LinkUpdate,
    create_catalog_writer,
)
from LiuXin_alpha.databases.macro_types import LinkRow, LinkValue
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    RelationKind,
    StorageColumnSpec,
    StorageLinkSpec,
    StorageSchemaSpec,
    StorageTableSpec,
)
from LiuXin_alpha.errors import DatabaseIntegrityError


def _column(
    name: str,
    ordinal: int,
    *,
    primary_key: bool = False,
) -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        affinity="INTEGER" if primary_key else "TEXT",
        is_primary_key=primary_key,
    )


def _table(
    name: str,
    *columns: StorageColumnSpec,
    is_link_table: bool = False,
) -> StorageTableSpec:
    return StorageTableSpec(
        name=name,
        relation_kind=RelationKind.TABLE,
        columns=columns,
        id_column=columns[0].name if columns else None,
        is_main_table=not is_link_table,
        is_link_table=is_link_table,
    )


def _link_spec(
    cardinality: LinkCardinality = LinkCardinality.MANY_TO_MANY,
) -> StorageLinkSpec:
    return StorageLinkSpec(
        primary_table="books",
        secondary_table="tags",
        link_table="book_tag_links",
        cardinality=cardinality,
        primary_id_col="book_id",
        secondary_id_col="tag_id",
        primary_link_col="book_id",
        secondary_link_col="tag_id",
        destination_owned=cardinality is LinkCardinality.ONE_TO_ONE,
    )


def _schema(
    *tables: StorageTableSpec,
    link_spec: StorageLinkSpec | None = None,
) -> StorageSchemaSpec:
    return StorageSchemaSpec(
        tables={table.name: table for table in tables},
        interlinks=() if link_spec is None else (link_spec,),
        intralinks=(),
    )


class _Wrapper:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        link_spec: StorageLinkSpec | None = None,
    ) -> None:
        self.schema = schema
        self.link_spec = link_spec
        self.schema_refreshes: list[bool] = []
        self.link_requests: list[tuple[str, str, bool]] = []

    def get_schema_spec(self, *, force_refresh: bool = False) -> StorageSchemaSpec:
        self.schema_refreshes.append(force_refresh)
        return self.schema

    def get_link_spec(
        self,
        source_table: str,
        destination_table: str,
        *,
        force_refresh: bool = False,
    ) -> StorageLinkSpec | None:
        self.link_requests.append(
            (source_table, destination_table, force_refresh)
        )
        return self.link_spec


class _Macros:
    def __init__(self) -> None:
        self.ensured: list[tuple[str, str, Any, str | None]] = []
        self.found: list[tuple[str, str, Any, str | None]] = []
        self.owned_writes: list[
            tuple[StorageLinkSpec, str, dict[int, Any | None]]
        ] = []

    def ensure_table_value(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        id_column: str | None = None,
    ) -> int:
        self.ensured.append((table, column, value, id_column))
        return {"Science Fiction": 20, "Classic": 21, 5: 22}[value]

    def find_table_value(
        self,
        table: str,
        column: str,
        value: Any,
        *,
        id_column: str | None = None,
    ) -> int | None:
        self.found.append((table, column, value, id_column))
        return {"Science Fiction": 20, "Classic": 21, 5: 22}.get(value)

    def get_link_rows_bulk(
        self,
        _link_spec: StorageLinkSpec,
        primary_ids: object,
        *,
        link_type: object,
    ) -> dict[int, tuple[LinkRow, ...]]:
        return {source_id: () for source_id in primary_ids}  # type: ignore[union-attr]

    def replace_owned_one_to_one_values_bulk(
        self,
        link_spec: StorageLinkSpec,
        value_column: str,
        replacements: Mapping[int, Any | None],
    ) -> dict[int, tuple[LinkRow, ...]]:
        materialized = dict(replacements)
        self.owned_writes.append((link_spec, value_column, materialized))
        return {
            source_id: (
                ()
                if value is None
                else (LinkRow(source_id, 20 + source_id),)
            )
            for source_id, value in materialized.items()
        }


class _Database:
    def __init__(self, wrapper: _Wrapper) -> None:
        self.driver_wrapper = wrapper
        self.macros = _Macros()
        self.column_writes: list[tuple[dict[int, Any], str, str]] = []

    def update_columns(
        self,
        values_map: Mapping[int, Any],
        field: str,
        table: str,
    ) -> None:
        self.column_writes.append((dict(values_map), field, table))


class _Catalog:
    def __init__(
        self,
        schema: StorageSchemaSpec,
        link_spec: StorageLinkSpec | None = None,
    ) -> None:
        self.db = _Database(_Wrapper(schema, link_spec))
        self.column_updates: list[CatalogColumnUpdate[Any]] = []
        self.link_updates: list[LinkUpdate] = []
        self.owned_updates: list[CatalogOwnedRowUpdate[Any]] = []

    def write_column_update(
        self,
        update: CatalogColumnUpdate[Any],
    ) -> Mapping[int, Any]:
        self.column_updates.append(update)
        return update.write(self.db)  # type: ignore[arg-type]

    def write_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        self.link_updates.append(update)
        return {
            source_id: tuple(
                LinkRow(source_id, link.secondary_id)
                for link in links
            )
            for source_id, links in update.replacements.items()
        }

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[Any],
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        self.owned_updates.append(update)
        return update.write(self.db.macros)  # type: ignore[arg-type]


def test_factory_creates_and_writes_a_same_table_column_writer() -> None:
    books = _table(
        "books",
        _column("book_id", 0, primary_key=True),
        _column("book_title", 1),
    )
    catalog = _Catalog(_schema(books))

    writer = create_catalog_writer(
        catalog,  # type: ignore[arg-type]
        "books",
        "book_title",
        force_refresh=True,
    )
    result = writer.write_one(1, "A New Title")

    assert isinstance(writer, CatalogColumnWriter)
    assert writer.table_spec is books
    assert writer.column_spec.name == "book_title"
    assert result == {1: "A New Title"}
    assert catalog.db.column_writes == [
        ({1: "A New Title"}, "book_title", "books")
    ]
    assert catalog.db.driver_wrapper.schema_refreshes == [True]
    assert catalog.db.driver_wrapper.link_requests == []


def test_factory_prefers_a_column_on_the_source_table() -> None:
    books = _table(
        "books",
        _column("book_id", 0, primary_key=True),
        _column("display_name", 1),
    )
    agents = _table(
        "agents",
        _column("agent_id", 0, primary_key=True),
        _column("display_name", 1),
    )

    writer = create_catalog_writer(
        _Catalog(_schema(books, agents)),  # type: ignore[arg-type]
        "books",
        "display_name",
    )

    assert isinstance(writer, CatalogColumnWriter)
    assert writer.table_spec is books


@pytest.mark.parametrize(
    "cardinality",
    (
        LinkCardinality.ONE_TO_ONE,
        LinkCardinality.ONE_TO_MANY,
        LinkCardinality.MANY_TO_ONE,
        LinkCardinality.MANY_TO_MANY,
    ),
)
def test_factory_creates_a_link_writer_for_every_cardinality(
    cardinality: LinkCardinality,
) -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )
    link_table = _table(
        "book_tag_links",
        _column("book_id", 0),
        _column("tag_id", 1),
        is_link_table=True,
    )
    link_spec = _link_spec(cardinality)
    catalog = _Catalog(
        _schema(books, tags, link_table, link_spec=link_spec),
        link_spec,
    )

    writer = create_catalog_writer(
        catalog,  # type: ignore[arg-type]
        "books",
        "tag_name",
    )
    result = writer.write_one(1, "Science Fiction")

    expected_type = (
        CatalogOwnedRowOneToOneWriter
        if cardinality is LinkCardinality.ONE_TO_ONE
        else CatalogTableValueLinkWriter
    )
    assert isinstance(writer, expected_type)
    assert writer.link_spec is link_spec
    assert writer.destination_table is tags
    assert writer.destination_column.name == "tag_name"
    assert catalog.db.driver_wrapper.link_requests == [
        ("books", "tags", False)
    ]
    if cardinality is LinkCardinality.ONE_TO_ONE:
        assert catalog.db.macros.ensured == []
        assert catalog.db.macros.owned_writes == [
            (link_spec, "tag_name", {1: "Science Fiction"})
        ]
        assert catalog.owned_updates[0].values == {1: "Science Fiction"}
        assert result == {1: (LinkRow(1, 21),)}
    else:
        assert catalog.db.macros.ensured == [
            ("tags", "tag_name", "Science Fiction", "tag_id")
        ]
        assert catalog.link_updates[0].replacements == {1: (LinkValue(20),)}
        assert result == {1: (LinkRow(1, 20),)}


def test_factory_rejects_ambiguous_destination_columns() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("display_name", 1),
    )
    agents = _table(
        "agents",
        _column("agent_id", 0, primary_key=True),
        _column("display_name", 1),
    )

    with pytest.raises(ValueError, match="ambiguous.*agents, tags"):
        create_catalog_writer(
            _Catalog(_schema(books, tags, agents)),  # type: ignore[arg-type]
            "books",
            "display_name",
        )


def test_factory_does_not_infer_ownership_from_one_to_one_cardinality() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )
    links = _table(
        "book_tag_links",
        _column("book_id", 0),
        _column("tag_id", 1),
        is_link_table=True,
    )
    link_spec = replace(
        _link_spec(LinkCardinality.ONE_TO_ONE),
        destination_owned=False,
    )
    catalog = _Catalog(
        _schema(books, tags, links, link_spec=link_spec),
        link_spec,
    )

    writer = create_catalog_writer(
        catalog,  # type: ignore[arg-type]
        "books",
        "tag_name",
    )

    assert isinstance(writer, CatalogTableValueLinkWriter)


def test_factory_rejects_owned_plural_links() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )
    links = _table(
        "book_tag_links",
        _column("book_id", 0),
        _column("tag_id", 1),
        is_link_table=True,
    )
    link_spec = _link_spec(LinkCardinality.MANY_TO_MANY)
    catalog = _Catalog(
        _schema(books, tags, links, link_spec=link_spec),
        link_spec,
    )

    with pytest.raises(ValueError, match="only a one-to-one"):
        create_catalog_writer(
            catalog,  # type: ignore[arg-type]
            "books",
            "tag_name",
            destination_owned=True,
        )


def test_factory_link_writer_treats_integer_scalars_as_column_values() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )
    link_table = _table(
        "book_tag_links",
        _column("book_id", 0),
        _column("tag_id", 1),
        is_link_table=True,
    )
    link_spec = _link_spec()
    catalog = _Catalog(
        _schema(books, tags, link_table, link_spec=link_spec),
        link_spec,
    )
    writer = create_catalog_writer(
        catalog,  # type: ignore[arg-type]
        "books",
        "tag_name",
    )

    update = writer.build_update({1: 5})
    id_update = writer.build_update({1: LinkValue(30)})

    assert catalog.db.macros.ensured == []
    assert id_update.replacements == {1: (LinkValue(30),)}

    writer.apply_update(update)

    assert catalog.db.macros.ensured == [("tags", "tag_name", 5, "tag_id")]
    assert catalog.link_updates[-1].replacements == {1: (LinkValue(22),)}


def test_shared_value_link_deletions_find_without_ensuring() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )
    link_table = _table(
        "book_tag_links",
        _column("book_id", 0),
        _column("tag_id", 1),
        is_link_table=True,
    )
    link_spec = _link_spec()
    catalog = _Catalog(
        _schema(books, tags, link_table, link_spec=link_spec),
        link_spec,
    )
    writer = create_catalog_writer(
        catalog,  # type: ignore[arg-type]
        "books",
        "tag_name",
    )

    update = writer.build_update(
        deletions={
            1: ("Science Fiction", "Missing", LinkValue(30)),
        }
    )

    assert catalog.db.macros.ensured == []
    assert catalog.db.macros.found == []

    writer.apply_update(update)

    assert catalog.db.macros.found == [
        ("tags", "tag_name", "Science Fiction", "tag_id"),
        ("tags", "tag_name", "Missing", "tag_id"),
    ]
    assert catalog.link_updates[-1].deletions == {}
    assert catalog.link_updates[-1].replacements == {1: ()}


def test_factory_rejects_an_unlinked_destination_table() -> None:
    books = _table("books", _column("book_id", 0, primary_key=True))
    tags = _table(
        "tags",
        _column("tag_id", 0, primary_key=True),
        _column("tag_name", 1),
    )

    with pytest.raises(ValueError, match="no link exists"):
        create_catalog_writer(
            _Catalog(_schema(books, tags)),  # type: ignore[arg-type]
            "books",
            "tag_name",
        )


@pytest.mark.parametrize(
    ("src_table", "dst_column", "error", "message"),
    (
        ("missing", "book_title", KeyError, "unknown source table"),
        ("books", "missing", KeyError, "unknown destination column"),
        ("", "book_title", TypeError, "src_table"),
        ("books", "", TypeError, "dst_column"),
    ),
)
def test_factory_rejects_invalid_minimal_identifiers(
    src_table: str,
    dst_column: str,
    error: type[Exception],
    message: str,
) -> None:
    books = _table(
        "books",
        _column("book_id", 0, primary_key=True),
        _column("book_title", 1),
    )

    with pytest.raises(error, match=message):
        create_catalog_writer(
            _Catalog(_schema(books)),  # type: ignore[arg-type]
            src_table,
            dst_column,
        )


def test_factory_same_table_writer_round_trips_through_real_database(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE catalog_factory_sources (
            catalog_factory_source_id INTEGER PRIMARY KEY,
            catalog_factory_source_value TEXT NOT NULL
        );
        INSERT INTO catalog_factory_sources VALUES (1, 'before');
        """
    )
    catalog = Catalog(db)
    writer = catalog.create_writer(
        "catalog_factory_sources",
        "catalog_factory_source_value",
        force_refresh=True,
    )

    result = catalog.write(
        "catalog_factory_sources",
        "catalog_factory_source_value",
        {1: "after"},
        force_refresh=True,
    )
    single_result = catalog.write_one(
        "catalog_factory_sources",
        "catalog_factory_source_value",
        1,
        "final",
    )

    assert isinstance(writer, CatalogColumnWriter)
    assert result == {1: "after"}
    assert single_result == {1: "final"}
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT catalog_factory_source_value "
            "FROM catalog_factory_sources "
            "WHERE catalog_factory_source_id = 1"
        )
    ) == "final"


def test_factory_link_writer_round_trips_through_real_database(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE factory_sources (
            factory_source_id INTEGER PRIMARY KEY
        );
        CREATE TABLE factory_values (
            factory_value_id INTEGER PRIMARY KEY,
            factory_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE factory_source_factory_value_links (
            factory_source_factory_value_link_factory_source_id
                INTEGER NOT NULL UNIQUE,
            factory_source_factory_value_link_factory_value_id
                INTEGER NOT NULL UNIQUE,
            UNIQUE(
                factory_source_factory_value_link_factory_source_id,
                factory_source_factory_value_link_factory_value_id
            ),
            FOREIGN KEY(
                factory_source_factory_value_link_factory_source_id
            )
                REFERENCES factory_sources(
                    factory_source_id
                ),
            FOREIGN KEY(
                factory_source_factory_value_link_factory_value_id
            )
                REFERENCES factory_values(
                    factory_value_id
                )
        );
        INSERT INTO factory_sources VALUES (1);
        """
    )
    catalog = Catalog(db)
    writer = catalog.create_writer(
        "factory_sources",
        "factory_value_name",
        force_refresh=True,
        destination_owned=True,
    )

    result = catalog.write_one(
        "factory_sources",
        "factory_value_name",
        1,
        "created by factory",
        destination_owned=True,
    )

    assert isinstance(writer, CatalogOwnedRowOneToOneWriter)
    assert writer.link_spec.cardinality is LinkCardinality.ONE_TO_ONE
    assert len(result[1]) == 1
    destination_id = result[1][0].secondary_id
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT factory_value_name "
            "FROM factory_values AS values_table "
            "JOIN factory_source_factory_value_links AS links_table "
            "ON links_table."
            "factory_source_factory_value_link_factory_value_id = "
            "values_table.factory_value_id "
            "WHERE links_table."
            "factory_source_factory_value_link_factory_source_id = 1"
        )
    ) == "created by factory"

    changed = catalog.write(
        "factory_sources",
        "factory_value_name",
        {1: "updated in place"},
        destination_owned=True,
    )
    assert changed[1][0].secondary_id == destination_id
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT factory_value_name FROM factory_values "
            "WHERE factory_value_id = ?",
            (destination_id,),
        )
    ) == "updated in place"
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM factory_values"
        )
    ) == 1

    assert writer.write({1: None}) == {1: ()}
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM factory_source_factory_value_links"
        )
    ) == 0
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM factory_values"
        )
    ) == 1


def test_factory_link_writer_enforces_the_live_allowed_type_registry(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE typed_sources (
            typed_source_id INTEGER PRIMARY KEY
        );
        CREATE TABLE typed_values (
            typed_value_id INTEGER PRIMARY KEY,
            typed_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE typed_source_typed_value_links (
            typed_source_typed_value_link_typed_source_id INTEGER NOT NULL,
            typed_source_typed_value_link_typed_value_id INTEGER NOT NULL,
            typed_source_typed_value_link_type TEXT,
            UNIQUE(
                typed_source_typed_value_link_typed_source_id,
                typed_source_typed_value_link_typed_value_id,
                typed_source_typed_value_link_type
            ),
            FOREIGN KEY(typed_source_typed_value_link_typed_source_id)
                REFERENCES typed_sources(typed_source_id),
            FOREIGN KEY(typed_source_typed_value_link_typed_value_id)
                REFERENCES typed_values(typed_value_id)
        );
        CREATE TABLE typed_source_typed_value_links__types (
            type TEXT PRIMARY KEY
        );
        INSERT INTO typed_sources VALUES (1);
        INSERT INTO typed_source_typed_value_links__types VALUES ('author');
        """
    )
    catalog = Catalog(db)
    writer = catalog.create_writer(
        "typed_sources",
        "typed_value_name",
        force_refresh=True,
    )

    assert isinstance(writer, CatalogTableValueLinkWriter)
    assert writer.link_spec.allowed_types_table == (
        "typed_source_typed_value_links__types"
    )
    assert db.driver_wrapper.get_allowed_link_types(writer.link_spec) == (
        "author",
    )

    with pytest.raises(ValueError, match="does not exist in allowed-types"):
        catalog.write_one(
            "typed_sources",
            "typed_value_name",
            1,
            "Ada",
            link_type="reviewer",
        )
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM typed_values"
        )
    ) == 0

    db.driver_wrapper.execute(
        "INSERT INTO typed_source_typed_value_links__types VALUES (?)",
        ("reviewer",),
    )
    result = catalog.write(
        "typed_sources",
        "typed_value_name",
        {1: {"reviewer": "Ada"}},
    )

    assert result[1][0].link_type == "reviewer"
    assert db.driver_wrapper.get_allowed_link_types(writer.link_spec) == (
        "author",
        "reviewer",
    )

    db.driver_wrapper.executescript(
        """
        CREATE TABLE allowed_types__typed_source_typed_value_links (
            legacy_link_type TEXT PRIMARY KEY
        );
        INSERT INTO allowed_types__typed_source_typed_value_links
            VALUES ('legacy-author'), ('legacy-editor');
        CREATE TABLE malformed_link_types (
            first_type TEXT,
            second_type TEXT
        );
        """
    )
    legacy_spec = replace(
        writer.link_spec,
        allowed_types_table=(
            "allowed_types__typed_source_typed_value_links"
        ),
    )
    assert db.driver_wrapper.get_allowed_link_types(legacy_spec) == (
        "legacy-author",
        "legacy-editor",
    )
    assert db.driver_wrapper.get_allowed_link_types(
        replace(writer.link_spec, allowed_types_table=None)
    ) is None
    with pytest.raises(TypeError, match="StorageLinkSpec"):
        db.driver_wrapper.get_allowed_link_types(object())
    with pytest.raises(DatabaseIntegrityError, match="does not exist"):
        db.driver_wrapper.get_allowed_link_types(
            replace(writer.link_spec, allowed_types_table="missing_types")
        )
    with pytest.raises(DatabaseIntegrityError, match="unambiguous"):
        db.driver_wrapper.get_allowed_link_types(
            replace(
                writer.link_spec,
                allowed_types_table="malformed_link_types",
            )
        )

    db.driver_wrapper.execute(
        "INSERT INTO typed_source_typed_value_links__types VALUES ('')"
    )
    with pytest.raises(DatabaseIntegrityError, match="invalid type value"):
        db.driver_wrapper.get_allowed_link_types(writer.link_spec)


def test_factory_shared_link_writer_deletes_without_creating_values(db) -> None:
    db.driver_wrapper.executescript(
        """
        CREATE TABLE shared_sources (
            shared_source_id INTEGER PRIMARY KEY
        );
        CREATE TABLE shared_values (
            shared_value_id INTEGER PRIMARY KEY,
            shared_value_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE shared_source_shared_value_links (
            shared_source_shared_value_link_shared_source_id INTEGER NOT NULL,
            shared_source_shared_value_link_shared_value_id INTEGER NOT NULL,
            UNIQUE(
                shared_source_shared_value_link_shared_source_id,
                shared_source_shared_value_link_shared_value_id
            ),
            FOREIGN KEY(shared_source_shared_value_link_shared_source_id)
                REFERENCES shared_sources(shared_source_id),
            FOREIGN KEY(shared_source_shared_value_link_shared_value_id)
                REFERENCES shared_values(shared_value_id)
        );
        INSERT INTO shared_sources VALUES (1);
        INSERT INTO shared_values VALUES (10, 'existing');
        """
    )
    catalog = Catalog(db)
    writer = catalog.create_writer(
        "shared_sources",
        "shared_value_name",
        force_refresh=True,
    )

    before_invalid = next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM shared_values"
        )
    )
    with pytest.raises(ValueError, match="requires a typed link spec"):
        catalog.write_one(
            "shared_sources",
            "shared_value_name",
            1,
            "must not be created",
            link_type="author",
        )
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM shared_values"
        )
    ) == before_invalid
    initial = catalog.write(
        "shared_sources",
        "shared_value_name",
        {1: ("existing", "created")},
    )
    before = next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM shared_values"
        )
    )
    final = catalog.write(
        "shared_sources",
        "shared_value_name",
        deletions={1: ("existing", "missing")},
    )

    assert isinstance(writer, CatalogTableValueLinkWriter)
    assert writer.link_spec.cardinality is LinkCardinality.MANY_TO_MANY
    assert len(initial[1]) == 2
    assert len(final[1]) == 1
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT COUNT(*) FROM shared_values"
        )
    ) == before
    assert next(
        row[0]
        for row in db.driver_wrapper.execute(
            "SELECT shared_value_name "
            "FROM shared_values AS values_table "
            "JOIN shared_source_shared_value_links AS links_table "
            "ON links_table."
            "shared_source_shared_value_link_shared_value_id = "
            "values_table.shared_value_id "
            "WHERE links_table."
            "shared_source_shared_value_link_shared_source_id = 1"
        )
    ) == "created"
