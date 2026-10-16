"""Exercise public Catalog/Core writes on fresh SQLite, APSW and live PostgreSQL.

PostgreSQL uses a disposable schema and requires LIUXIN_TEST_POSTGRES_URL.
Every backend runs the same assertions; no SQL translation doubles are used.
"""

from __future__ import annotations

import os
import uuid

import pytest

from LiuXin_alpha.catalog.api.common import CatalogMutationError
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.library.library import Library


@pytest.fixture(params=("SQLite", "SQLite_apsw", "PostgreSQL"))
def write_runtime(request, tmp_path):
    """Create a fresh complete schema and close runtime/database before cleanup."""
    backend = request.param
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    schema = None
    if backend == "PostgreSQL":
        url = os.environ.get("LIUXIN_TEST_POSTGRES_URL", "").strip()
        if not url:
            pytest.skip("LIUXIN_TEST_POSTGRES_URL is not configured.")
        schema = f"liuxin_catalog_write_{uuid.uuid4().hex}"
        metadata = {"postgres_url": url, "schema": schema}
    else:
        metadata = {"database_path": str(tmp_path / "catalog-writes.sqlite")}
    try:
        with Database(
            metadata=metadata,
            db_type=backend,
            create=True,
            backup=False,
            enable_maintenance=False,
            enable_storage_manager=False,
        ) as db:
            catalog = Catalog(db)
            runtime = CoreRuntime(library=Library(database=db), catalog=catalog)
            try:
                yield runtime, catalog, db
            finally:
                runtime.shutdown()
    finally:
        if schema is not None:
            from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
                PostgresConnectionAdapter,
                connect_postgres,
            )

            connection = PostgresConnectionAdapter(
                connect_postgres(metadata, prompt_for_password=False)
            )
            try:
                with connection:
                    connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                connection.close()


def _stack(catalog, **overrides):
    """Create one complete WEMI graph with optional explicit Work-ID or Item data."""
    return catalog.mutations.writer.create_wemi_stack(
        **{
            "work": {"title": "Boundary parity"},
            "expression": {"label": "Primary"},
            "manifestation": {"format_detail": "EPUB"},
            "items": ({"inventory_code": "boundary-parity"},),
            **overrides,
        }
    )


def test_explicit_work_ids_obey_repository_policy_and_late_rollback(
    write_runtime, monkeypatch
):
    """A repository policy applies to automatic, explicit-ID and existing-Work writes."""
    _runtime, catalog, _db = write_runtime
    prepare = catalog.works._prepare_write

    def policy(data, *, existing=None):
        values = prepare(data, existing=existing)
        if values.get("work_title") == "Rejected by policy":
            raise ValueError("Work policy rejected title")
        values["work_discovery_note"] = "Prepared by repository"
        return values

    monkeypatch.setattr(catalog.works, "_prepare_write", policy)
    ordinary = _stack(catalog)
    explicit = _stack(catalog, work_id=700)
    for work_id in (ordinary.work_id, explicit.work_id):
        assert (
            catalog.works.require(work_id)["work_discovery_note"]
            == "Prepared by repository"
        )
    before = {
        name: getattr(catalog, name).list()
        for name in (
            "works",
            "expressions",
            "manifestations",
            "items",
        )
    }
    for work_id in (None, 701, ordinary.work_id):
        with pytest.raises(ValueError, match="Work policy rejected"):
            _stack(catalog, work_id=work_id, work={"title": "Rejected by policy"})
    with pytest.raises(CatalogMutationError):
        _stack(
            catalog,
            work_id=701,
            items=({"manifestation_id": ordinary.manifestation_id},),
        )
    assert {name: getattr(catalog, name).list() for name in before} == before
    assert catalog.works.get(701) is None


def test_core_and_catalog_identity_alias_and_identifier_parity(write_runtime):
    """Generic Core writes preserve identity, aliases and curated pair validation."""
    runtime, catalog, _db = write_runtime
    tag = runtime.command(
        "admin.row.create",
        {
            "table": "tags",
            "values": {
                "tag": "  Ｓｃｉ-Fi & Fantasy  ",
                "tag_phash": "incorrect",
            },
        },
    )["record"]
    assert tag["values"]["tag_phash"] == "sci-fi & fantasy"
    runtime.command(
        "admin.row.update",
        {
            "table": "tags",
            "row_id": tag["row_id"],
            "updates": {"tag": "Sci-Fi & Horror"},
        },
    )
    assert catalog.tags.require(tag["row_id"])["tag_phash"] == "sci-fi & horror"
    assert catalog.ensure.tag(" SCI-FI & HORROR ").row_id == tag["row_id"]
    agent = runtime.command(
        "admin.row.create",
        {
            "table": "agents",
            "values": {
                "name": "Parity author",
                "aliases": ["Pen Name", "pen name", "Other Name"],
            },
        },
    )["record"]
    assert agent["values"]["agent_aliases"] == "Pen Name(#BREAK#)Other Name"
    work_id = catalog.works.create({"title": "Identifier owner"})
    owner = {"entity_type": "work", "entity_id": work_id}
    valid = runtime.command(
        "admin.row.create",
        {
            "table": "entity_identifiers",
            "values": {
                **owner,
                "scheme": "ISBN",
                "value": "9780306406157",
            },
        },
    )["record"]
    before = catalog.identifiers.list()
    for command, payload in (
        ("admin.row.create", {"values": {**owner, "scheme": "isbn", "value": "bad"}}),
        ("admin.row.update", {"row_id": valid["row_id"], "updates": {"value": "bad"}}),
    ):
        with pytest.raises(CoreHandlerError):
            runtime.command(command, {"table": "entity_identifiers", **payload})
    assert catalog.identifiers.list() == before


def test_core_metadata_failure_rolls_back_earlier_attachments(write_runtime):
    """Late invalid identifiers undo an earlier tag creation through Core metadata.write."""
    runtime, catalog, _db = write_runtime
    stack = _stack(catalog)
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "metadata.write",
            {
                "item_id": stack.item_ids[0],
                "values": {
                    "tags": ["Must roll back"],
                    "identifiers": {"isbn": "invalid-checksum"},
                },
                "replace": True,
                "mark_dirty": False,
            },
        )
    assert catalog.tags.list() == ()
    assert catalog.identifiers.list() == ()
    runtime.command(
        "metadata.write",
        {
            "item_id": stack.item_ids[0],
            "values": {
                "tags": ["Recovered"],
            },
            "mark_dirty": False,
        },
    )
    assert catalog.tags.list()[0]["tag_phash"] == "recovered"


def test_core_relationship_validation_and_unlink_preserve_survivors(write_runtime):
    """Ordered links keep priorities; invalid types/endpoints leave existing links intact."""
    runtime, catalog, db = write_runtime
    work_id = catalog.works.create({"title": "Relationship owner"})
    tag_ids = [catalog.tags.create({"tag": value}) for value in ("First", "Second")]
    spec = db.driver_wrapper.get_link_spec("works", "tags")
    for tag_id in tag_ids:
        runtime.command(
            "admin.relation.link",
            {
                "table": "works",
                "row_id": work_id,
                "related_table": "tags",
                "related_row_id": tag_id,
            },
        )
    before = db.macros.get_link_rows(spec, work_id)
    assert len(before) == 2
    assert len({row.priority for row in before}) == 2
    for extra in ({"related_row_id": 999999}, {"type": "not-a-supported-tag-role"}):
        with pytest.raises(CoreHandlerError):
            runtime.command(
                "admin.relation.link",
                {
                    "table": "works",
                    "row_id": work_id,
                    "related_table": "tags",
                    "related_row_id": tag_ids[0],
                    **extra,
                },
            )
    assert db.macros.get_link_rows(spec, work_id) == before
    runtime.command(
        "admin.relation.unlink",
        {
            "table": "works",
            "row_id": work_id,
            "related_table": "tags",
            "related_row_id": tag_ids[0],
        },
    )
    remaining = db.macros.get_link_rows(spec, work_id)
    assert [(row.secondary_id, row.priority) for row in remaining] == [
        (row.secondary_id, row.priority)
        for row in before
        if row.secondary_id == tag_ids[1]
    ]
    other_work = catalog.works.create({"title": "Other relationship owner"})
    comment = catalog.comments.add_for_wemi(
        level="work", entity_id=work_id, data={"text": "Owned"}
    )
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "admin.relation.link",
            {
                "table": "comments",
                "row_id": comment,
                "related_table": "works",
                "related_row_id": other_work,
            },
        )


def test_explicit_work_id_does_not_break_generated_ids(write_runtime):
    """Creating an explicit Work ID leaves subsequent automatic allocation usable."""
    _runtime, catalog, _db = write_runtime
    first = _stack(catalog, work_id=1)
    generated = _stack(catalog)
    assert generated.work_id != first.work_id


def test_administrative_hierarchy_write_cannot_create_cycle(write_runtime):
    """Generic administrative row edits retain the hierarchy constraint."""
    runtime, catalog, _db = write_runtime
    parent = catalog.genres.create({"genre": "Cycle parent"})
    child = catalog.genres.create({"genre": "Cycle child", "parent_id": parent})
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "admin.row.update",
            {"table": "genres", "row_id": parent, "updates": {"parent_id": child}},
        )
    assert catalog.genres.require(parent)["genre_parent_id"] is None


def test_tree_nesting_rolls_back_on_late_identity_collision(write_runtime):
    """A later scoped genre collision cannot leave an earlier child reparented."""
    runtime, catalog, _db = write_runtime
    parent = catalog.genres.create({"genre": "Parent"})
    catalog.genres.create({"genre": "Collision", "parent_id": parent})
    first = catalog.genres.create({"genre": "First child"})
    collision = catalog.genres.create({"genre": "Collision"})
    before = catalog.genres.list()
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "tree.nest",
            {"table": "genres", "parent_id": parent, "child_ids": [first, collision]},
        )
    assert catalog.genres.list() == before


def test_tree_delete_rolls_back_on_late_failure(write_runtime, monkeypatch):
    """Deleting a subtree is atomic even when persistence fails after its first leaf."""
    runtime, catalog, db = write_runtime
    parent = catalog.genres.create({"genre": "Delete parent"})
    catalog.genres.create({"genre": "Delete child", "parent_id": parent})
    before = catalog.genres.list()
    delete = db.macros.delete_row

    def fail_parent(table, row_id, **kwargs):
        if table == "genres" and row_id == parent:
            raise RuntimeError("late parent failure")
        return delete(table, row_id, **kwargs)

    monkeypatch.setattr(db.macros, "delete_row", fail_parent)
    with pytest.raises(CoreHandlerError):
        runtime.command(
            "tree.delete", {"table": "genres", "row_id": parent, "confirm": True}
        )
    assert catalog.genres.list() == before
