from __future__ import annotations

import pytest

from LiuXin_alpha.caches import SchemaBackedStorageCache
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_many_field import (
    LinkDstUpdate as ManyManyLinkDstUpdate,
    ManyManyInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_one_field import (
    ManyOneInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_many_field import (
    LinkDstUpdate as OneManyLinkDstUpdate,
    OneManyInTwoTableFieldUpdate,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_one_field import (
    OneOneInTwoTableFieldUpdate,
    OneOneInOneTableFieldUpdate,
)
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
    StorageSchemaSpec,
)
from tests.support.storage_cache_test_harness import (
    FakeDB,
    create_loaded_test_cache,
    make_fake_db,
    make_table,
)


@pytest.fixture()
def _schema_backed_cache_db() -> FakeDB:
    books = make_table(
        "books",
        ("id", "title", "shared_code"),
        is_main_table=True,
        linked_tables=("covers",),
    )
    covers = make_table(
        "covers",
        ("id", "path", "shared_code"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_covers = make_table(
        "book_covers",
        ("id", "book_id", "cover_id"),
        is_link_table=True,
        linked_tables=("books", "covers"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "covers": covers,
            "book_covers": book_covers,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="covers",
                link_table="book_covers",
                cardinality=LinkCardinality.ONE_TO_ONE,
                primary_link_col="book_id",
                secondary_link_col="cover_id",
            ),
        ),
        intralinks=(),
    )

    db = make_fake_db(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One", "shared_code": "A-1"},
                {"id": 2, "title": "Book Two", "shared_code": "A-2"},
            ],
            "covers": [
                {"id": 10, "path": "/covers/one.jpg", "shared_code": "C-1"},
                {"id": 11, "path": "/covers/two.jpg", "shared_code": "C-2"},
            ],
            "book_covers": [
                {"id": 100, "book_id": 1, "cover_id": 10},
                {"id": 101, "book_id": 2, "cover_id": 11},
            ],
        },
    )
    return db


@pytest.fixture()
def schema_backed_cache(_schema_backed_cache_db: FakeDB) -> SchemaBackedStorageCache:
    return create_loaded_test_cache(_schema_backed_cache_db, "schema_backed")


@pytest.fixture()
def many_one_schema_backed_cache() -> SchemaBackedStorageCache:
    books = make_table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("publishers",),
    )
    publishers = make_table(
        "publishers",
        ("id", "publisher_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_publishers = make_table(
        "book_publishers",
        ("id", "book_id", "publisher_id"),
        is_link_table=True,
        linked_tables=("books", "publishers"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "publishers": publishers,
            "book_publishers": book_publishers,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="publishers",
                link_table="book_publishers",
                cardinality=LinkCardinality.MANY_TO_ONE,
                primary_link_col="book_id",
                secondary_link_col="publisher_id",
            ),
        ),
        intralinks=(),
    )

    db = make_fake_db(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
                {"id": 3, "title": "Book Three"},
            ],
            "publishers": [
                {"id": 20, "publisher_name": "Tor Books"},
            ],
            "book_publishers": [
                {"id": 200, "book_id": 1, "publisher_id": 20},
                {"id": 201, "book_id": 2, "publisher_id": 20},
            ],
        },
    )

    return create_loaded_test_cache(db, "schema_backed")


@pytest.fixture()
def one_many_schema_backed_cache() -> SchemaBackedStorageCache:
    books = make_table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("notes",),
    )
    notes = make_table(
        "notes",
        ("id", "note_text"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_notes = make_table(
        "book_notes",
        ("id", "book_id", "note_id", "note_priority", "note_type"),
        is_link_table=True,
        linked_tables=("books", "notes"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "notes": notes,
            "book_notes": book_notes,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="notes",
                link_table="book_notes",
                cardinality=LinkCardinality.ONE_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="note_id",
                priority_link_col="note_priority",
                type_link_col="note_type",
                ordered=True,
                typed=True,
            ),
        ),
        intralinks=(),
    )

    db = make_fake_db(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
            ],
            "notes": [
                {"id": 30, "note_text": "Existing note"},
                {"id": 31, "note_text": "Other owner's note"},
            ],
            "book_notes": [
                {"id": 300, "book_id": 1, "note_id": 30, "note_priority": 2, "note_type": "main"},
                {"id": 301, "book_id": 2, "note_id": 31, "note_priority": 1, "note_type": "main"},
            ],
        },
    )

    return create_loaded_test_cache(db, "schema_backed")


@pytest.fixture()
def many_many_schema_backed_cache() -> SchemaBackedStorageCache:
    books = make_table(
        "books",
        ("id", "title"),
        is_main_table=True,
        linked_tables=("tags",),
    )
    tags = make_table(
        "tags",
        ("id", "tag_name"),
        is_main_table=True,
        linked_tables=("books",),
    )
    book_tags = make_table(
        "book_tags",
        ("id", "book_id", "tag_id", "tag_priority"),
        is_link_table=True,
        linked_tables=("books", "tags"),
    )

    schema = StorageSchemaSpec(
        tables={
            "books": books,
            "tags": tags,
            "book_tags": book_tags,
        },
        interlinks=(
            StorageLinkSpec(
                primary_table="books",
                secondary_table="tags",
                link_table="book_tags",
                cardinality=LinkCardinality.MANY_TO_MANY,
                primary_link_col="book_id",
                secondary_link_col="tag_id",
                priority_link_col="tag_priority",
                ordered=True,
            ),
        ),
        intralinks=(),
    )

    db = make_fake_db(
        schema=schema,
        rows_by_table={
            "books": [
                {"id": 1, "title": "Book One"},
                {"id": 2, "title": "Book Two"},
            ],
            "tags": [
                {"id": 40, "tag_name": "Science Fiction"},
                {"id": 41, "tag_name": "Classic"},
            ],
            "book_tags": [
                {"id": 400, "book_id": 1, "tag_id": 40, "tag_priority": 2},
                {"id": 401, "book_id": 2, "tag_id": 40, "tag_priority": 2},
                {"id": 402, "book_id": 2, "tag_id": 41, "tag_priority": 1},
            ],
        },
    )

    return create_loaded_test_cache(db, "schema_backed")
def test_one_to_one_relation_field_deleted_ids_unlink_without_deleting_dst_rows(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    assert cache.get_one_one_link_table("books", "covers").get_dst_id(1) is None
    assert cache.db.get_row_from_id("covers", 10).row_dict["path"] == "/covers/one.jpg"


def test_one_to_one_relation_field_updates_existing_linked_values(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={1: "/covers/one-updated.jpg"},
            deleted_ids=set(),
            dirtied=set(),
        )
    )

    assert field.get_value_from_src_id(1) == "/covers/one-updated.jpg"
    assert cache.get_main_table("covers").get_row_snapshot(10)["path"] == "/covers/one-updated.jpg"
    assert cache.db.get_row_from_id("covers", 10).row_dict["path"] == "/covers/one-updated.jpg"


def test_one_to_one_relation_field_can_recreate_missing_link_from_existing_value(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={1: "/covers/one.jpg"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
        )
    )

    assert field.get_dst_id_from_src_id(1) == 10
    assert cache.get_one_one_link_table("books", "covers").get_dst_id(1) == 10


def test_one_to_one_relation_field_can_create_missing_related_row_and_link(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={1: "/covers/one-fresh.jpg"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
            create_missing_related_rows=True,
        )
    )

    assert field.get_value_from_src_id(1) == "/covers/one-fresh.jpg"
    new_dst_id = field.get_dst_id_from_src_id(1)
    assert new_dst_id is not None
    assert new_dst_id != 10
    assert cache.db.get_row_from_id("covers", new_dst_id).row_dict["path"] == "/covers/one-fresh.jpg"


def test_one_to_one_relation_field_refuses_to_reassign_existing_linked_dst_row(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.covers.path")

    field.update(
        OneOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="covers",
            dst_table_target_column="path",
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    with pytest.raises(ValueError):
        field.update(
            OneOneInTwoTableFieldUpdate(
                src_table="books",
                dst_table="covers",
                dst_table_target_column="path",
                added_maps={1: "/covers/two.jpg"},
                updated_maps={},
                deleted_ids=set(),
                dirtied=set(),
                create_missing_links=True,
            )
        )


def test_relation_field_rejects_creating_related_rows_without_creating_links(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    field = schema_backed_cache.get_field("books.covers.path")

    with pytest.raises(ValueError):
        field.update(
            OneOneInTwoTableFieldUpdate(
                src_table="books",
                dst_table="covers",
                dst_table_target_column="path",
                added_maps={1: "/covers/one-fresh.jpg"},
                updated_maps={},
                deleted_ids=set(),
                dirtied=set(),
                create_missing_related_rows=True,
            )
        )


def test_many_one_relation_field_can_create_missing_link_from_existing_value(
    many_one_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_one_schema_backed_cache
    field = cache.get_field("books.publishers.publisher_name")

    field.update(
        ManyOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="publishers",
            dst_table_target_column="publisher_name",
            added_maps={3: "Tor Books"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
        )
    )

    assert field.get_dst_id_from_src_id(3) == 20
    assert field.get_value_from_src_id(3) == "Tor Books"


def test_many_one_relation_field_can_create_missing_related_row_and_link(
    many_one_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_one_schema_backed_cache
    field = cache.get_field("books.publishers.publisher_name")

    field.update(
        ManyOneInTwoTableFieldUpdate(
            src_table="books",
            dst_table="publishers",
            dst_table_target_column="publisher_name",
            added_maps={3: "Orbit Books"},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            create_missing_links=True,
            create_missing_related_rows=True,
        )
    )

    new_dst_id = field.get_dst_id_from_src_id(3)
    assert new_dst_id is not None
    assert new_dst_id != 20
    assert field.get_value_from_src_id(3) == "Orbit Books"
    assert cache.db.get_row_from_id("publishers", new_dst_id).row_dict["publisher_name"] == "Orbit Books"


def test_one_many_relation_field_explicit_link_replacements_can_create_and_order_rows(
    one_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = one_many_schema_backed_cache
    field = cache.get_field("books.notes.note_text")

    field.update(
        OneManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="notes",
            dst_table_target_column="note_text",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Existing note",
                        type="intro",
                    ),
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Fresh note",
                        type="sidebar",
                    ),
                ]
            },
        )
    )

    assert list(field.get_values_from_src_id(1, require_ordering=True)) == [
        "Existing note",
        "Fresh note",
    ]
    dst_ids = list(field.get_dst_ids_from_src_id(1, require_ordering=True))
    assert dst_ids[0] == 30
    assert field.get_link_properties(1, dst_ids[0]).type == "intro"
    assert field.get_link_properties(1, dst_ids[1]).type == "sidebar"
    assert cache.db.get_row_from_id("notes", dst_ids[1]).row_dict["note_text"] == "Fresh note"


def test_one_many_relation_field_explicit_replacement_wont_steal_other_src_dst_rows(
    one_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = one_many_schema_backed_cache
    field = cache.get_field("books.notes.note_text")

    field.update(
        OneManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="notes",
            dst_table_target_column="note_text",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    OneManyLinkDstUpdate(
                        dst_table="notes",
                        dst_table_target_column="note_text",
                        dst_col_val="Other owner's note",
                    ),
                ]
            },
        )
    )

    dst_id = field.get_dst_ids_from_src_id(1)[0]
    assert dst_id != 31
    assert cache.get_one_many_link_table("books", "notes").get_src_id(31) == 2
    assert cache.db.get_row_from_id("notes", dst_id).row_dict["note_text"] == "Other owner's note"


def test_many_many_relation_field_explicit_link_replacements_can_reuse_shared_dst_rows(
    many_many_schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = many_many_schema_backed_cache
    field = cache.get_field("books.tags.tag_name")

    field.update(
        ManyManyInTwoTableFieldUpdate(
            src_table="books",
            dst_table="tags",
            dst_table_target_column="tag_name",
            added_maps={},
            updated_maps={},
            deleted_ids=set(),
            dirtied=set(),
            link_replacements={
                1: [
                    ManyManyLinkDstUpdate(
                        dst_table="tags",
                        dst_table_target_column="tag_name",
                        dst_col_val="Science Fiction",
                    ),
                    ManyManyLinkDstUpdate(
                        dst_table="tags",
                        dst_table_target_column="tag_name",
                        dst_col_val="Space Opera",
                    ),
                ]
            },
        )
    )

    dst_ids = list(field.get_dst_ids_from_src_id(1, require_ordering=True))
    assert dst_ids[0] == 40
    assert field.get_values_from_src_id(1, require_ordering=True) == (
        "Science Fiction",
        "Space Opera",
    )
    assert cache.get_many_many_link_table("books", "tags").has_link(2, 40) is True
    assert cache.db.get_row_from_id("tags", dst_ids[1]).row_dict["tag_name"] == "Space Opera"


def test_same_table_field_deleted_ids_nullify_column_without_deleting_rows(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("title")

    field.update(
        OneOneInOneTableFieldUpdate(
            added_maps={},
            updated_maps={},
            deleted_ids={1},
            dirtied=set(),
        )
    )

    assert cache.get_main_table("books").has_id(1) is True
    assert cache.get_main_table("books").get_row_snapshot(1)["title"] is None
    assert field.get_value_from_id(1) is None
    assert cache.db.get_row_from_id("books", 1).row_dict["title"] is None


def test_same_table_field_can_refresh_and_remove_ids_after_external_changes(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("title")

    cache.db.driver_wrapper.update_column("books", 2, "title", "Retitled")
    field.refresh_ids({2})
    assert field.get_value_from_id(2) == "Retitled"

    cache.db.driver_wrapper.delete_by_id("books", {2})
    field.remove_ids({2})
    assert 2 not in field.ids
    assert field.get_value_from_id(2) is None


def test_same_table_field_refuses_to_clear_primary_key_values(
    schema_backed_cache: SchemaBackedStorageCache,
) -> None:
    cache = schema_backed_cache
    field = cache.get_field("books.id")

    with pytest.raises(ValueError):
        field.update(
            OneOneInOneTableFieldUpdate(
                added_maps={},
                updated_maps={},
                deleted_ids={1},
                dirtied=set(),
            )
        )
