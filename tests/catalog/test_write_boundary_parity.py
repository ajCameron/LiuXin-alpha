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
from LiuXin_alpha.catalog.metadata_tools import Add
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


@pytest.mark.parametrize("operation", ["creator", "organisation", "publisher"])
def test_agent_helper_late_failure_rolls_back_complete_aggregate(
    write_runtime, monkeypatch, operation
):
    """Row adapters roll back subtype, relation and metadata writes together."""
    _runtime, catalog, db = write_runtime
    parent = catalog.agents.create_organisation({"name": "Existing parent"})
    tables = (
        "agents",
        "human_agents",
        "org_agents",
        "org_agent_relations",
        "notes",
        "entity_identifiers",
    )
    before = {table: db.macros.get_rows(table) for table in tables}
    add = Add(db)

    def reject_attachment(**kwargs):
        raise RuntimeError("late agent attachment failure")

    monkeypatch.setattr(add, "_entity_identifier", reject_attachment)
    with pytest.raises(RuntimeError, match="late agent attachment failure"):
        if operation == "creator":
            add.creator("Rollback Person", creator_bio="Rolled back biography")
        elif operation == "organisation":
            add.organisation("Rollback Organisation", organisation_parent=parent)
        else:
            add.publisher("Rollback Publisher", publisher_parent=parent)

    assert {table: db.macros.get_rows(table) for table in tables} == before


def test_agent_helper_aliases_match_repository_policy(write_runtime):
    """Compatibility inputs preserve the repository's Unicode alias equivalence."""
    _runtime, catalog, db = write_runtime
    aliases = [" Ａｄａ ", "ada", "ß", "SS"]
    direct = catalog.agents.create({"name": "Direct aliases", "aliases": aliases})
    adapted = Add(db).agent("Adapted aliases", agent_aliases=aliases)
    assert catalog.agents.require(direct)["agent_aliases"] == "Ａｄａ(#BREAK#)ß"
    assert adapted["agent_aliases"] == catalog.agents.require(direct)["agent_aliases"]


def test_agent_helper_rejects_non_organisation_parent_atomically(write_runtime):
    """Legacy argument spelling does not bypass the aggregate parent's policy."""
    _runtime, catalog, db = write_runtime
    person = catalog.agents.create_person({"name": "Person cannot be parent"})
    before = catalog.agents.list()
    with pytest.raises(CatalogMutationError, match="not an organisation"):
        Add(db).organisation("Invalid child", organisation_parent=person)
    assert catalog.agents.list() == before


def test_surface_tag_lookup_uses_the_stored_identity_policy(write_runtime):
    """Local and Core views distinguish spaced keys and honor Unicode identity."""
    from LiuXin_alpha.core.proxies.local import LocalCoreClient
    from LiuXin_alpha.surfaces.core import CoreDatabaseView
    from LiuXin_alpha.surfaces.metadata_facets import search_tag_rows

    runtime, catalog, db = write_runtime
    spaced = catalog.tags.create({"text": "Space Opera"})
    compact = catalog.tags.create({"text": "SpaceOpera"})
    for view in (db, CoreDatabaseView(LocalCoreClient(runtime))):
        for text, expected in (
            ("Space Opera", spaced),
            ("Ｓｐａｃｅ\tＯｐｅｒａ", spaced),
            ("SpaceOpera", compact),
        ):
            rows = search_tag_rows(view, "tags", text)
            assert [int(row["tag_id"]) for row in rows] == [expected]


def test_terminal_tag_write_keeps_distinct_full_text_keys(write_runtime):
    """Terminal input cannot merge values that the Catalog treats as distinct."""
    import io

    from LiuXin_alpha.core.proxies.local import LocalCoreClient
    from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser

    runtime, catalog, db = write_runtime
    work_id = catalog.works.create({"title": "Distinct tag keys"})
    output = io.StringIO()
    browser = TextDatabaseBrowser(LocalCoreClient(runtime), output=output)
    assert browser.execute_line(f'on works {work_id} tag "Space Opera, SpaceOpera"'), (
        output.getvalue()
    )
    linked = db.get_interlinked_rows(
        primary_row=db.get_row_from_id("works", work_id), secondary_table="tags"
    )
    assert {row["tag"] for row in linked} == {"Space Opera", "SpaceOpera"}


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


@pytest.mark.parametrize(
    "table,column", [("genres", "genre"), ("subjects", "subject"), ("series", "series")]
)
def test_terminal_facet_resolution_uses_identity_not_sort_or_phonetic_keys(
    write_runtime, table, column
):
    """Distinct full-text keys survive lookup even when legacy sort keys collide."""
    from types import SimpleNamespace

    from LiuXin_alpha.core.proxies.local import LocalCoreClient
    from LiuXin_alpha.surfaces.core import CoreDatabaseView
    from LiuXin_alpha.surfaces.terminal.commands import on

    runtime, catalog, _db = write_runtime
    repository = getattr(catalog, table)
    compact = repository.create({column: "SpaceOpera", column + "_sort": "spaceopera"})
    spaced = repository.create({column: "Space Opera", column + "_sort": "spaceopera"})
    browser = SimpleNamespace(db=CoreDatabaseView(LocalCoreClient(runtime)))
    resolver = getattr(on, "_resolve_or_create_" + column + "_row")
    for text, expected in (("Space Opera", spaced), ("SpaceOpera", compact)):
        resolved_table, row = resolver(browser, text, create=False)
        assert resolved_table == table
        assert int(row[column + "_id"]) == expected
    assert resolver(browser, "Missing Value", create=False) is None


@pytest.mark.parametrize("table,column", [("genres", "genre"), ("subjects", "subject")])
def test_terminal_facet_resolution_preserves_root_scope(write_runtime, table, column):
    """An identically named child cannot replace a root facet during assignment."""
    from types import SimpleNamespace

    from LiuXin_alpha.core.proxies.local import LocalCoreClient
    from LiuXin_alpha.surfaces.core import CoreDatabaseView
    from LiuXin_alpha.surfaces.terminal.commands import on

    runtime, catalog, _db = write_runtime
    repository = getattr(catalog, table)
    parent = repository.create({column: "Parent"})
    repository.create({column: "Space Opera", column + "_parent_id": parent})
    root_id = repository.create({column: "Space Opera"})
    browser = SimpleNamespace(db=CoreDatabaseView(LocalCoreClient(runtime)))
    resolver = getattr(on, "_resolve_or_create_" + column + "_row")
    _table, row = resolver(browser, "Space Opera", create=False)
    assert int(row[column + "_id"]) == root_id


def test_organisation_relation_reuses_only_the_same_validated_edge(write_runtime):
    """Idempotent edges retain their note, and invalid endpoints still fail."""
    _runtime, catalog, db = write_runtime
    parent = catalog.agents.create_organisation({"name": "Relation parent"})
    child = catalog.agents.create_organisation({"name": "Relation child"})
    edge = catalog.agents.relate_organisations(child, parent, note="Original note")
    assert (
        catalog.agents.relate_organisations(
            child, parent, relation_type=" imprint_of ", note="Ignored duplicate note"
        )
        == edge
    )
    rows = db.macros.get_rows("org_agent_relations")
    assert len(rows) == 1
    assert rows[0]["org_agent_relation_note"] == "Original note"
    person = catalog.agents.create_person({"name": "Invalid relation child"})
    with pytest.raises(CatalogMutationError, match="not an organisation"):
        catalog.agents.relate_organisations(person, parent)
    with pytest.raises(ValueError, match="non-empty string"):
        catalog.agents.relate_organisations(child, parent, relation_type=" ")
    assert db.macros.get_rows("org_agent_relations") == rows


def test_tag_helpers_share_identity_for_creation_and_reuse(write_runtime):
    """Compatibility writes and ensure distinguish full-text keys and reuse Unicode forms."""
    _runtime, catalog, _db = write_runtime
    spaced = catalog.add.tag("Space Opera", tag_phash="wrong-policy")
    compact = catalog.ensure.tag("SpaceOpera")
    assert spaced["tag_phash"] == "space opera"
    assert compact["tag_phash"] == "spaceopera"
    assert spaced.row_id != compact.row_id
    assert catalog.ensure.tag(" ＳＰＡＣＥ   ＯＰＥＲＡ ").row_id == spaced.row_id
    assert len(catalog.tags.list()) == 2
