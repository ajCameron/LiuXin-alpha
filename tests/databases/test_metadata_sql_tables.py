"""Exercise the physical-table owners, grouped reads and their production callers."""

import inspect
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.storage_graph_api import CoreStorageGraphAPI
from LiuXin_alpha.databases import metadata_sql
from LiuXin_alpha.databases.api.metadata_sql_api import MetadataSQLAPI
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backup import BackupWorkflowRepository
from tests.databases.test_metadata_sql_wemi_reads import _count_reads


def test_every_physical_main_table_has_a_concrete_owner(db):
    schema = dict(db.get("SELECT name, type FROM sqlite_master"))
    tables = {table for table in db.main_tables if schema.get(table) == "table"}
    assert len(tables) == 36
    expected = {
        "".join(part.title() for part in table.split("_")) + "SQLMixin"
        for table in tables
    }
    composed = {cls.__name__ for cls in type(db.metadata_sql).__bases__}
    assert (
        composed - {"FeedsSQLMixin", "NormalizationSQLMixin", "MetadataSQLAPI"}
        == expected
    )
    for name in expected:
        owner = getattr(metadata_sql, name)
        methods = [
            value
            for key, value in vars(owner).items()
            if not key.startswith("_") and callable(value)
        ]
        assert methods, name
    for name in MetadataSQLAPI.__abstractmethods__:
        assert inspect.signature(
            getattr(type(db.metadata_sql), name)
        ) == inspect.signature(getattr(MetadataSQLAPI, name))


def test_link_owners_resolve_real_relationships(db):
    # Exercise each literal destination against a declared physical relationship.
    tables = "agents comments notes synopses genres subjects labels languages ratings images folders devices stores digital_assets composite_digital_assets tags series files".split()
    candidates = "works expressions manifestations items files folders stores devices agents digital_assets composite_digital_assets".split()
    for table in tables:
        reader = getattr(db.metadata_sql, f"get_{table}_linked_from")
        source = next(
            (
                candidate
                for candidate in candidates
                if candidate != table
                and db.driver_wrapper.get_link_table_name(candidate, table)
            ),
            None,
        )
        assert source is not None, table
        assert reader(source, -345678) == (), table


def test_foreign_key_owners_use_real_columns_and_keep_empty_groups(db):
    methods = {
        "get_items_for_manifestations": {},
        "get_annotations_for_items": {},
        "get_entity_identifiers_for_owners": {"entity_type": "work"},
        "get_item_identifiers_for_items": {},
        "get_human_agents_for_agents": {},
        "get_org_agents_for_agents": {},
        "get_asset_replicas_for_assets": {},
        "get_asset_replicas_for_stores": {},
        "get_file_derivations_for_parents": {},
        "get_file_derivations_for_children": {},
        "get_org_agent_relations_for_parents": {},
        "get_org_agent_relations_for_children": {},
        "get_backup_workflow_sources_for_workflows": {},
        "get_backup_workflow_outputs_for_workflows": {},
        "get_backup_workflow_state_for_workflows": {},
    }
    for name, kwargs in methods.items():
        reader = getattr(db.metadata_sql, name)
        assert reader([-5, -6, -5], **kwargs) == {-5: (), -6: ()}, name
        with pytest.raises(InputIntegrityError, match="integers"):
            reader([-5, True], **kwargs)


def test_subtype_and_lineage_batches_preserve_reference_direction(db):
    catalog = Catalog(db)
    person = catalog.agents.create_person({"name": "A Person"})
    parent = catalog.agents.create_organisation({"name": "Parent Press"})
    child = catalog.agents.create_organisation(
        {"name": "Child Imprint"},
        parent_id=parent,
    )
    people = db.metadata_sql.get_human_agents_for_agents([person, parent])
    assert people[parent] == ()
    assert people[person][0]["human_agent_agent_id"] == person
    organisations = db.metadata_sql.get_org_agents_for_agents([person, child])
    assert organisations[person] == ()
    assert organisations[child][0]["org_agent_agent_id"] == child
    parents = db.metadata_sql.get_org_agent_relations_for_parents([parent, child])
    children = db.metadata_sql.get_org_agent_relations_for_children([parent, child])
    assert parents[child] == children[parent] == ()
    assert parents[parent] == children[child]
    assert parents[parent][0]["org_agent_relation_type"] == "imprint_of"

    original = db.macros.insert_row("files", {"file_name": "original.epub"})
    derived = db.macros.insert_row("files", {"file_name": "derived.pdf"})
    edge = db.macros.insert_row(
        "file_derivations",
        {
            "file_derivation_parent_file_id": original,
            "file_derivation_child_file_id": derived,
            "file_derivation_kind": "conversion",
        },
    )
    outgoing = db.metadata_sql.get_file_derivations_for_parents([original, derived])
    incoming = db.metadata_sql.get_file_derivations_for_children([original, derived])
    assert outgoing[derived] == incoming[original] == ()
    assert outgoing[original] == incoming[derived]
    assert incoming[derived][0]["file_derivation_id"] == edge


def test_owner_batches_scope_identifiers_and_bound_queries(db, monkeypatch):
    # Entity IDs overlap across levels. Scope must be applied in every SQL chunk.
    with db.macros.transaction():
        ids = []
        for owner, level in ((10000, "work"), (11000, "work"), (10000, "expression")):
            ids.append(
                db.macros.insert_row(
                    "entity_identifiers",
                    {
                        "entity_identifier_entity_type": level,
                        "entity_identifier_entity_id": owner,
                        "entity_identifier_scheme": "uuid",
                        "entity_identifier_value": f"{level}-{owner}",
                    },
                )
            )
    counts = _count_reads(db, monkeypatch)
    rows = db.metadata_sql.get_entity_identifiers_for_owners(
        (*range(10000, 11001), 10000),
        entity_type="work",
    )
    assert tuple(rows) == tuple(range(10000, 11001))
    assert [
        row["entity_identifier_id"] for group in rows.values() for row in group
    ] == ids[:2]
    assert rows[10001] == ()
    assert counts == [
        ("entity_identifiers", 500),
        ("entity_identifiers", 500),
        ("entity_identifiers", 4),
    ]
    assert db.metadata_sql.get_entity_identifiers_for_owners(
        [10000], entity_type="WORK"
    ) == {10000: ()}
    counts.clear()
    with pytest.raises(InputIntegrityError):
        db.macros.get_rows_for_ids("entity_identifiers", "missing_column", [1])
    with pytest.raises(InputIntegrityError, match="integers"):
        db.macros.get_rows_for_ids(
            "entity_identifiers", "entity_identifier_entity_id", [1, "2"]
        )
    assert counts == []


def test_item_owner_reads_filter_in_sql_and_observe_rollback(db, monkeypatch):
    catalog = Catalog(db)
    manifestation = catalog.manifestations.create(
        {"edition_statement": "Owner batching"}
    )
    item = catalog.items.create({"manifestation_id": manifestation})
    other = catalog.items.create({"manifestation_id": manifestation})
    first = db.macros.insert_row(
        "annotations",
        {
            "annotation_item_id": item,
            "annotation_kind": "note",
            "annotation_user_id": 5,
        },
    )
    second = db.macros.insert_row(
        "annotations",
        {
            "annotation_item_id": item,
            "annotation_kind": "highlight",
            "annotation_user_id": 5,
        },
    )
    db.macros.insert_row(
        "annotations",
        {
            "annotation_item_id": other,
            "annotation_kind": "note",
            "annotation_user_id": 5,
        },
    )
    null_kind = db.macros.insert_row("annotations", {"annotation_item_id": item})
    assert [
        row["annotation_id"]
        for row in db.metadata_sql.get_annotations_for_items([item], user_id=5)[item]
    ] == [first, second]
    assert [
        row["annotation_id"]
        for row in db.macros.get_rows_for_ids(
            "annotations", "annotation_item_id", [item], where={"annotation_kind": None}
        )[item]
    ] == [null_kind]
    counts = _count_reads(db, monkeypatch)
    rows = catalog.annotations.list_for_item(item, user_id=5, kind=" note ")
    assert [row["annotation_id"] for row in rows] == [first]
    assert ("annotations", 3) in counts
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            pending = catalog.items.create({"manifestation_id": manifestation})
            assert pending in {
                row["item_id"]
                for row in catalog.items.list_for_manifestation(manifestation)
            }
            raise RuntimeError("rollback")
    assert pending not in {
        row["item_id"] for row in catalog.items.list_for_manifestation(manifestation)
    }


def test_pages_bound_materialization_and_preserve_completion(db, monkeypatch):
    ids = [
        db.macros.insert_row("digital_assets", {"digital_asset_name": "page"})
        for _ in range(5)
    ]
    seen = []
    original = db.macros._mapping_rows

    def capture(columns, cursor):
        rows = original(columns, cursor)
        seen.append(len(rows))
        return rows

    monkeypatch.setattr(db.macros, "_mapping_rows", capture)
    runtime = SimpleNamespace(database=db)
    for offset, limit, expected, complete in (
        (0, 2, ids[:2], False),
        (2, 3, ids[2:], True),
        (0, 0, [], False),
        (5, 0, [], True),
        (50, 3, [], True),
    ):
        seen.clear()
        result = CoreStorageGraphAPI.resource_list(
            runtime,
            CoreQuery(
                "storage.resource.list",
                payload={
                    "resource": "asset",
                    "where": {"name": "page"},
                    "offset": offset,
                    "limit": limit,
                },
            ),
        )
        assert [row["id"] for row in result["records"]] == expected
        assert result["complete"] is complete
        assert seen == [min(limit + 1, max(0, 5 - offset))]
    for bad in (True, -1, 1.5, "2"):
        with pytest.raises(InputIntegrityError):
            db.macros.get_rows_page("digital_assets", limit=bad)
    with db.macros.transaction() as conn:
        conn.execute("CREATE VIEW page_assets AS SELECT * FROM digital_assets")
    for method, kwargs in (
        (db.macros.get_rows_page, {}),
        (
            db.macros.get_rows_for_ids,
            {"foreign_id_column": "digital_asset_id", "owner_ids": []},
        ),
    ):
        with pytest.raises(InputIntegrityError, match="physical table"):
            method("page_assets", **kwargs)


def test_policy_scan_batches_shared_references_and_preserves_assessments(
    db, monkeypatch
):
    replication = db.macros.insert_row(
        "replication_policies",
        {"replication_policy_name": "batch", "replication_policy_min_copies": 2},
    )
    backup = db.macros.insert_row(
        "backup_policies",
        {"backup_policy_name": "batch", "backup_policy_min_backup_copies": 1},
    )
    assets = [
        db.macros.insert_row(
            "digital_assets",
            {
                "digital_asset_name": f"policy-{i}",
                "digital_asset_replication_policy_id": replication,
                "digital_asset_backup_policy_id": backup,
            },
        )
        for i in range(4)
    ]
    store = db.macros.insert_row("stores", {"store_name": "policy-store"})
    for i, asset in enumerate(assets):
        db.macros.insert_row(
            "asset_replicas",
            {
                "asset_replica_digital_asset_id": asset,
                "asset_replica_store_id": store,
                "asset_replica_storage_key": str(i),
                "asset_replica_mode": "active",
            },
        )
    runtime = SimpleNamespace(database=db)
    service = CoreStorageGraphAPI()
    expected = [service._assess(runtime, asset) for asset in assets]
    counts = _count_reads(db, monkeypatch)
    result = service.policy_violations(runtime, CoreQuery("storage.policy.violations"))
    assert [row for row in result["records"] if row["asset_id"] in assets] == expected
    assert [table for table, _ in counts].count("digital_assets") == 1
    assert [table for table, _ in counts].count("asset_replicas") == 1
    assert [table for table, _ in counts].count("replication_policies") == 1
    assert [table for table, _ in counts].count("backup_policies") == 1


def test_backup_readers_roundtrip_on_production_database(db, tmp_path, monkeypatch):
    repository = BackupWorkflowRepository(db)
    declaration = api.BackupWorkflowDeclaration(
        "table-owned",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        str(tmp_path / "out.sqsh"),
        sources=(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.LOCAL_PATH,
                str(tmp_path / "book.epub"),
                archive_path="book.epub",
            ),
        ),
    )
    workflow_id = repository.save_workflow_declaration(declaration)
    # Search fallback must not serve reads on a production database.
    monkeypatch.setattr(
        db, "search", lambda *args, **kwargs: pytest.fail("legacy search used")
    )
    assert repository.load_workflow_declaration(workflow_id) == declaration
    assert repository.load_checkpoint(workflow_id).status == api.WorkflowStatus.DRAFT
    counts = _count_reads(db, monkeypatch)
    assert (workflow_id, declaration) in list(
        repository.iter_workflow_declarations(status=api.WorkflowStatus.DRAFT)
    )
    assert [table for table, _ in counts] == [
        "backup_workflows",
        "backup_workflow_sources",
    ]
    assert db.metadata_sql.get_backup_workflow_outputs_for_workflows([workflow_id]) == {
        workflow_id: ()
    }
