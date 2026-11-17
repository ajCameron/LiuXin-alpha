"""Native field behavior across generated SQLite catalogues."""

from datetime import datetime

import pytest

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldPatch,
    OwnerRef,
    Predicate,
    SeriesValue,
    StalePlan,
    UnsupportedStorage,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import CustomColumnAdapter
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


@pytest.fixture
def native(db):
    service = CustomFieldService(db)
    owners = tuple(
        OwnerRef("works", db.macros.insert_row("works", {"work_title": title}))
        for title in ("One", "Two")
    )
    return service, owners


@pytest.mark.parametrize("mode", ["scalar", "set", "list"])
@pytest.mark.parametrize(
    "datatype,value",
    [
        ("text", "a|b"),
        ("comments", ""),
        ("int", 0),
        ("float", 1.5),
        ("bool", False),
        ("rating", 0),
        ("datetime", datetime(2026, 1, 2, 3, 4)),
        ("enumeration", "Ready"),
        ("series", SeriesValue("Saga", 2.5)),
    ],
)
def test_native_datatype_matrix(native, mode, datatype, value):
    service, (owner, _) = native
    field = service.create_field(
        "works",
        "sample",
        datatype=datatype,
        mode=mode,
        display={"enum_values": ["Ready"]},
    )
    service.set_value(owner, field.id, value if mode == "scalar" else [value, value])
    result = service.get_value(owner, field.id)
    if mode == "scalar":
        assert result == value
    else:
        assert len(result) == (2 if mode == "list" else 1)
        assert result[0].value == (
            value.value if isinstance(value, SeriesValue) else value
        )
        if datatype == "series":
            assert result[0].position == 2.5
    service.set_value(owner, field.id, None)
    assert service.get_value(owner, field.id) == (None if mode == "scalar" else ())


def test_compound_macro_rollback_and_savepoint_publication(native):
    service, (first, second) = native
    field = service.create_field("works", "status")
    events = []
    service.subscribe(events.append)
    with pytest.raises(RuntimeError):
        with service.db.macros.transaction():
            service.set_value(first, field.id, "aborted")
            assert service.get_value(first, field.id) == "aborted"
            assert events == []
            raise RuntimeError("outer rollback")
    assert service.get_value(first, field.id) is None and events == []
    with service.db.macros.transaction():
        service.set_value(first, field.id, "kept")
        with pytest.raises(RuntimeError):
            with service.db.macros.transaction():
                service.set_value(second, field.id, "lost")
                raise RuntimeError("savepoint rollback")
        assert events == []
    assert len(events) == 1
    assert service.get_value(first, field.id) == "kept"
    assert service.get_value(second, field.id) is None


def test_atomic_mixed_patch_validation_and_entries(native):
    service, (first, second) = native
    text = service.create_field("works", "text", mode="list")
    integer = service.create_field("works", "number", datatype="int")
    with pytest.raises(CustomFieldError):
        service.apply_patch(
            [FieldPatch(first, text.id, ["a"]), FieldPatch(second, integer.id, "bad")]
        )
    assert service.get_value(first, text.id) == ()
    service.apply_patch(
        [FieldPatch(first, text.id, ["a", "b"]), FieldPatch(second, integer.id, 3)]
    )
    a, b = service.get_value(first, text.id)
    service.edit_entries(first, text.id, update={a.id: "changed"}, order=[b.id, a.id])
    result = service.get_value(first, text.id)
    assert [v.value for v in result] == ["b", "changed"]
    assert [v.id for v in result] == [b.id, a.id]
    with pytest.raises(CustomFieldError):
        service.edit_entries(first, text.id, order=[a.id, a.id])


def test_queries_and_scoped_labels(native):
    service, (first, second) = native
    field = service.create_field("works", "status", mode="list")
    service.create_field("manifestations", "status")
    service.set_value(first, field.id, ["one", "two"])
    service.set_value(second, field.id, ["two"])
    assert service.usage_count(field.id, "two") == 2
    assert service.is_shared(field.id, "two")
    page = service.find_owners("works", [Predicate(field.id, "eq", "two")], limit=1)
    assert page.items == (first,) and page.next_cursor == first.id
    assert service.find_owners(
        "works", [Predicate(field.id, "eq", "two")], after_id=page.next_cursor
    ).items == (second,)
    assert service.find_owners(
        "works", [Predicate(field.id, "eq", "two", "all")]
    ).items == (second,)
    assert len(service.inventory(field.id).items) == 2


def test_field_migration_retirement_and_stale_plans(native):
    service, (first, second) = native
    field = service.create_field("works", "numbers", datatype="int", mode="list")
    service.set_value(first, field.id, [1, 2])
    blocked = service.plan_field_migration(field.id, {"mode": "scalar"})
    assert blocked.issues
    plan = service.plan_field_migration(field.id, {"mode": "scalar"}, reduce="last")
    service.set_value(second, field.id, [3])
    with pytest.raises(StalePlan):
        service.apply_field_migration(plan)
    plan = service.plan_field_migration(field.id, {"mode": "scalar"}, reduce="last")
    service.apply_field_migration(plan)
    assert service.get_value(first, field.id) == 2
    service.retire_field(field.id)
    with pytest.raises(CustomFieldError):
        service.set_value(first, field.id, 4)
    service.apply_purge(service.plan_purge(field.id))
    assert service.list_fields(include_retired=True) == ()


def test_unsupported_storage_refusal_preserves_ids_and_values(native):
    from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
    from LiuXin_alpha.databases.schema_readers import quote_identifier as q

    service, (first, _) = native
    field = service.create_field("works", "old")
    number = field.id
    table, link = names(field)
    ts, ls = stem(table), stem(link)
    with service.write() as conn:
        service.repository.drop_storage(conn, field)
        conn.execute(
            "UPDATE custom_columns SET custom_column_storage_version=1,custom_column_collection_mode=NULL WHERE custom_column_id=?",
            (number,),
        ).close()
        conn.execute(
            f"CREATE TABLE {q(table)} ({q(ts + '_id')} INTEGER PRIMARY KEY, {q(ts + '_value')} TEXT UNIQUE)"
        ).close()
        conn.execute(
            f"CREATE TABLE {q(link)} ({q(ls + '_id')} INTEGER PRIMARY KEY, {q(ls + '_book')} INTEGER, {q(ls + '_value')} INTEGER)"
        ).close()
        conn.execute(f"INSERT INTO {q(table)} VALUES (19, 'before')").close()
        conn.execute(f"INSERT INTO {q(link)} VALUES (47, ?, 19)", (first.id,)).close()
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    for operation in (
        lambda: service.get_value(first, number),
        lambda: service.set_value(first, number, "after"),
        lambda: service.create_field("works", "new"),
    ):
        with pytest.raises(
            UnsupportedStorage, match="v1 format support has been removed"
        ):
            operation()
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
        assert conn.execute(f"SELECT * FROM {q(table)}").fetchall() == [(19, "before")]
        assert conn.execute(f"SELECT * FROM {q(link)}").fetchall() == [
            (47, first.id, 19)
        ]
    assert not hasattr(service, "plan_upgrade") and not hasattr(
        service, "apply_upgrade"
    )


def test_shared_rename_merge_and_delete(native):
    service, (first, second) = native
    field = service.create_field(
        "works",
        "state",
        datatype="enumeration",
        mode="set",
        display={"enum_values": ["Ready", "Done"]},
    )
    service.set_value(first, field.id, ["Ready", "Done"])
    service.set_value(second, field.id, ["Ready"])
    ready = next(
        i["id"] for i in service.inventory(field.id).items if i["value"] == "Ready"
    )
    service.rename_value(field.id, ready, "READY")
    assert [v.value for v in service.get_value(second, field.id)] == ["READY"]
    service.rename_value(field.id, ready, "Done")
    assert [v.value for v in service.get_value(first, field.id)] == ["Done"]
    remaining = service.inventory(field.id).items
    assert len(remaining) == 1 and remaining[0]["owners"] == 2
    service.delete_value(field.id, remaining[0]["id"])
    assert service.get_value(first, field.id) == ()
    assert service.audit() == ()

    transition = service.create_field(
        "works",
        "transition",
        datatype="enumeration",
        mode="list",
        display={"enum_values": ["Keep", "Old"]},
    )
    service.set_value(first, transition.id, ["Keep", "Old"])
    plan = service.plan_field_migration(
        transition.id,
        {"display": {"enum_values": ["Keep", "New"]}},
        value_mapping={"Old": "New"},
    )
    receipt = service.apply_field_migration(plan)
    assert [v.value for v in service.get_value(first, transition.id)] == ["Keep", "New"]
    assert receipt.entry_ids and receipt.value_ids
    service.set_value(second, transition.id, ["Keep", "Keep"])
    keep_id = service.get_value(second, transition.id)[0].value_id
    collapsed = service.apply_field_migration(
        service.plan_field_migration(transition.id, {"mode": "set"}, collapse=True)
    )
    assert collapsed.value_ids[keep_id] == keep_id
    assert None in collapsed.entry_ids.values()
    cleared = service.plan_field_migration(
        transition.id,
        {"display": {"enum_values": ["Keep"]}},
        value_mapping={"New": None},
    )
    service.apply_field_migration(cleared)
    assert [v.value for v in service.get_value(first, transition.id)] == ["Keep"]


def test_legacy_adapter_and_explicit_retirement(native):
    from LiuXin_alpha.utils.calibre_compat.custom_columns import (
        CustomColumns,
    )
    from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
        CompatibilityError,
    )

    service, (first, _) = native
    scalar = service.create_field("works", "note")
    collection = service.create_field(
        "works", "notes", datatype="comments", mode="list"
    )
    legacy = CustomColumns(service.db, table="works")
    legacy.set_custom(first.id, "stored", num=scalar.id)
    assert legacy.get_custom(first.id, num=scalar.id, index_is_id=True) == "stored"
    service.set_value(first, collection.id, ["a", "a"])
    with pytest.raises(CompatibilityError):
        legacy.get_custom(first.id, num=collection.id, index_is_id=True)
    service.retire_field(collection.id)
    CustomColumns(service.db, table="works")
    assert service.get_field(collection.id, include_retired=True).retired
    service.apply_purge(service.plan_purge(collection.id))


def test_composite_templates_dependencies_and_cycles(native):
    from LiuXin_alpha.catalog.custom_fields import CustomFieldEvaluator

    service, (first, _) = native
    text = service.create_field("works", "status")
    service.set_value(first, text.id, "ready")
    computed = service.create_field(
        "works",
        "caption",
        datatype="composite",
        display={"composite_template": "{title}: {#works.status:uppercase()}"},
    )
    assert CustomFieldEvaluator(service).get_value(first, computed.id) == "One: READY"
    program = service.plan_field_migration(
        computed.id,
        {
            "display": {
                "composite_template": "program: uppercase(field('#works.status'))"
            }
        },
    )
    service.apply_field_migration(program)
    assert CustomFieldEvaluator(service).get_value(first, computed.id) == "READY"
    with pytest.raises(CustomFieldError):
        service.set_value(first, computed.id, "override")
    with pytest.raises(CustomFieldError, match="depends"):
        service.retire_field(text.id)
    with pytest.raises(CustomFieldError, match="cycle"):
        service.plan_field_migration(
            computed.id, {"display": {"composite_template": "{#caption}"}}
        )


def test_native_core_wire_and_preview_validation(native, request):
    from types import SimpleNamespace

    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.program_services.custom_fields import (
        native_fields_command,
        native_fields_query,
    )
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime

    service, (first, _) = native
    runtime = CoreRuntime(
        library=SimpleNamespace(database=service.db, storage=None), preferences={}
    )
    request.addfinalizer(runtime.shutdown)

    def command(name, payload):
        return native_fields_command(
            runtime, CoreCommand("custom-fields.native." + name, payload)
        )

    def query(name, payload):
        return native_fields_query(
            runtime, CoreQuery("custom-fields.native." + name, payload)
        )

    field = command(
        "create",
        {
            "attachment": "works",
            "label": "dates",
            "datatype": "datetime",
            "mode": "list",
        },
    )
    command(
        "patch",
        {
            "patches": [
                {
                    "owner": {"table": "works", "id": first.id},
                    "field": field["id"],
                    "value": [{"$datetime": "2026-01-01T00:00:00"}],
                }
            ]
        },
    )
    value = query(
        "value",
        {"owner": {"table": "works", "id": first.id}, "field": ["works", "dates"]},
    )
    assert value[0]["value"] == {"$datetime": "2026-01-01T00:00:00"}
    assert (
        runtime.query(
            "custom-fields.native.value",
            {"owner": {"table": "works", "id": first.id}, "field": field["id"]},
        )
        == value
    )
    assert (
        runtime.services.field_metadata.search_term_to_field_key("#dates")
        == "#works.dates"
    )
    intent = {"kind": "migration", "field": field["id"], "changes": {"mode": "scalar"}}
    preview = query("plan", intent)
    command(
        "patch",
        {
            "patches": [
                {
                    "owner": {"table": "works", "id": first.id},
                    "field": field["id"],
                    "value": [],
                }
            ]
        },
    )
    with pytest.raises(StalePlan):
        command("apply", {**intent, "fingerprint": preview["fingerprint"]})


def test_query_case_and_wrapper_hooks(native):
    service, (first, second) = native
    field = service.create_field("works", "note")
    CustomColumnAdapter(service.db).update_custom_column(
        "works", "note", "Ab%c", owner_id=first.id
    )
    service.set_value(second, field.id, "ab%c")
    assert service.find_owners(
        "works", [Predicate(field.id, "contains", "%")]
    ).items == (first, second)
    # Shared normalized spelling is authoritative; use comments to exercise case sensitivity.
    comments = service.create_field("works", "case", datatype="comments")
    service.set_value(first, comments.id, "Ab%c")
    service.set_value(second, comments.id, "ab%c")
    assert service.find_owners(
        "works", [Predicate(comments.id, "prefix", "A")]
    ).items == (first,)
    assert (
        CustomColumnAdapter(service.db)._get_custom_column_row("works", "note")[
            "custom_column_id"
        ]
        == field.id
    )


def test_migration_attachment_mapping_and_series_positions(native):
    service, (first, second) = native
    target = OwnerRef(
        "manifestations",
        service.db.macros.insert_row("manifestations", {"manifestation_id": 100}),
    )
    field = service.create_field("works", "sagas", datatype="series", mode="list")
    service.set_value(first, field.id, [SeriesValue("Saga", 0), SeriesValue("Saga", 3)])
    assert service.next_series_position(field.id, "Saga") == 4
    blocked = service.plan_field_migration(field.id, {"attachment": "manifestations"})
    assert blocked.issues
    plan = service.plan_field_migration(
        field.id, {"attachment": "manifestations"}, owner_mapping={first.id: target.id}
    )
    service.apply_field_migration(plan)
    assert [v.position for v in service.get_value(target, field.id)] == [0, 3]


def test_native_rollback_commit_failure_and_publication(native, monkeypatch):
    service, (first, _) = native
    field = service.create_field("works", "rollback")
    events = []
    service.subscribe(events.append)
    with monkeypatch.context() as patch:
        with pytest.raises(RuntimeError, match="commit failure"):
            with service.write() as conn:
                service.set_value(first, field.id, "lost")

                def fail():
                    raise RuntimeError("commit failure")

                patch.setattr(conn, "commit", fail)
    assert events == [] and service.get_value(first, field.id) is None

    def failed_projection(receipt):
        raise RuntimeError("projection failure")

    service.subscribe(failed_projection)
    with pytest.raises(ExceptionGroup, match="Committed transaction"):
        service.set_value(first, field.id, "committed")
    assert service.get_value(first, field.id) == "committed"


def test_native_schema_failure_is_atomic(native, monkeypatch):
    service, _ = native

    from LiuXin_alpha.caches.custom_fields import CustomFieldCache
    from LiuXin_alpha.caches.field_metadata import FieldMetadata

    projection = CustomFieldCache(service, FieldMetadata())

    def fail():
        raise RuntimeError("descriptor failure")

    with monkeypatch.context() as patch:
        patch.setattr(projection, "prepare", fail)
        with pytest.raises(RuntimeError, match="descriptor failure"):
            with projection.write():
                service.create_field("works", "unpublished")
    assert service.list_fields() == ()
    assert service.audit() == ()


def test_composite_relationship_paths_require_aggregation(native):
    from LiuXin_alpha.catalog.custom_fields import (
        CompositeEvaluationError,
        CustomFieldEvaluator,
    )
    from LiuXin_alpha.databases.db_types.macro_types import LinkValue

    service, (first, _) = native
    ids = [
        service.db.macros.insert_row("expressions", {"expression_label": v})
        for v in ("French", "German")
    ]
    spec = service.db.driver_wrapper.get_link_spec("works", "expressions")
    service.db.macros.replace_links(spec, first.id, [LinkValue(v) for v in ids])
    paths = {"translations": {"tables": ["expressions"], "field": "label"}}
    field = service.create_field(
        "works",
        "translations",
        datatype="composite",
        display={"composite_template": "{translations}", "paths": paths},
    )
    evaluator = CustomFieldEvaluator(service)
    with pytest.raises(CompositeEvaluationError, match="aggregation"):
        evaluator.get_value(first, field.id)
    paths["translations"]["aggregate"] = "join"
    service.apply_field_migration(
        service.plan_field_migration(
            field.id,
            {"display": {"composite_template": "{translations}", "paths": paths}},
        )
    )
    assert evaluator.get_value(first, field.id) == "French, German"


def test_scoped_metadata_aliases_and_dependencies(native):
    from LiuXin_alpha.caches.field_metadata import FieldMetadata
    from LiuXin_alpha.catalog.catalog import Catalog

    service, (owner, _) = native
    metadata = FieldMetadata()
    from LiuXin_alpha.caches.custom_fields import CustomFieldCache

    projection = CustomFieldCache(service, metadata)
    with projection.write():
        field = service.create_field("works", "status")
    assert metadata.search_term_to_field_key("#status") == "#works.status"
    with projection.write():
        service.create_field("manifestations", "status")
    assert metadata.search_term_to_field_key("#status") is None
    assert metadata.search_term_to_field_key("#works.status") == "#works.status"
    assert (
        metadata.search_term_to_field_key("#manifestations.status")
        == "#manifestations.status"
    )
    catalog = Catalog(service.db)
    catalog.custom_fields.set_value(owner, field.id, "ok")
    assert catalog.computed_fields.get_value(owner, field.id) == "ok"


def test_cleanup_repair_is_explicit_and_fingerprint_covers_ddl(native):
    from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
    from LiuXin_alpha.databases.schema_readers import quote_identifier as q

    service, (owner, _) = native
    field = service.create_field("works", "repair", mode="list")
    service.set_value(owner, field.id, ["old", "kept"])
    service.set_value(owner, field.id, ["kept"])
    table, link = names(field)
    ss = stem(link)
    before = service.plan_repair(actions=("unused_values",))
    with service.write() as conn:
        conn.execute(
            f"CREATE INDEX repair_extra_index ON {q(link)} ({q(ss + '_book')})"
        ).close()
    with pytest.raises(StalePlan):
        service.apply_repair(before)
    plan = service.plan_repair(actions=("unused_values",))
    service.apply_repair(plan)
    assert [v["value"] for v in service.inventory(field.id).items] == ["kept"]
    with service.write() as conn:
        conn.execute(f"UPDATE {q(link)} SET {q(ss + '_ordinal')}=9").close()
    assert any(i.code == "ordering" for i in service.audit())
    service.apply_repair(service.plan_repair(actions=("ordering",)))
    assert service.audit() == ()

    with service.write() as conn:
        conn.execute(
            f"UPDATE {q(table)} SET {q(stem(table) + '_key')}=?", ("incorrect",)
        ).close()
    assert any(i.code == "identity_key" for i in service.audit())
    service.apply_repair(service.plan_repair(actions=("identity_key",)))
    assert service.audit() == ()

    with service.write() as conn:
        conn.execute(
            "UPDATE custom_columns SET custom_column_display=? WHERE custom_column_id=?",
            ("broken JSON", field.id),
        ).close()
    assert any(i.code == "invalid_definition" for i in service.audit())
    assert service.plan_repair().issues


def test_old_definition_schema_refusal_preserves_inbound_references(native):
    import re
    from contextlib import closing

    from LiuXin_alpha.databases.custom_columns.native_repository import EXTRA_COLUMNS
    from LiuXin_alpha.databases.schema_readers import quote_identifier as q
    from LiuXin_alpha.utils.calibre_compat.custom_columns import (
        CustomColumns,
    )

    service, (owner, _) = native
    legacy = CustomColumns(service.db, table="works")
    number = legacy.create_custom_column("Old", label="old")
    legacy.set_custom(owner.id, "preserved", num=number)
    with closing(service.db.driver.get_connection()) as conn:
        conn.execute("PRAGMA foreign_keys=OFF").close()
        conn.execute("BEGIN IMMEDIATE").close()
        conn.execute(
            "CREATE TABLE custom_definition_refs (field_id INTEGER REFERENCES custom_columns(custom_column_id))"
        ).close()
        conn.execute("INSERT INTO custom_definition_refs VALUES (?)", (number,)).close()
        for column in EXTRA_COLUMNS:
            conn.execute(f"ALTER TABLE custom_columns DROP COLUMN {q(column)}").close()
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='custom_columns'"
        ).fetchone()[0]
        ddl = re.sub(r"UNIQUE \(`custom_column_in_table`, ", "UNIQUE (", ddl)
        ddl = (
            ddl.replace(
                "CREATE TABLE `custom_columns`", "CREATE TABLE old_custom_columns"
            )
            .replace('CREATE TABLE "custom_columns"', "CREATE TABLE old_custom_columns")
            .replace("CREATE TABLE custom_columns", "CREATE TABLE old_custom_columns")
        )
        conn.execute(ddl).close()
        conn.execute(
            "INSERT INTO old_custom_columns SELECT * FROM custom_columns"
        ).close()
        conn.execute("DROP TABLE custom_columns").close()
        conn.execute("ALTER TABLE old_custom_columns RENAME TO custom_columns").close()
        conn.commit()
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    with pytest.raises(UnsupportedStorage):
        service.get_value(owner, number)
    with pytest.raises(UnsupportedStorage):
        CustomColumns(service.db, table="works")
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
        assert (
            conn.execute("SELECT field_id FROM custom_definition_refs").fetchone()[0]
            == number
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_native_owner_cleanup_on_independent_connection(native):
    from contextlib import closing

    from LiuXin_alpha.databases.custom_columns.native_repository import names
    from LiuXin_alpha.databases.schema_readers import quote_identifier as q

    service, (owner, _) = native
    fields = [
        service.create_field("works", "normalized", mode="list"),
        service.create_field("works", "direct", datatype="comments", mode="list"),
    ]
    for field in fields:
        service.set_value(owner, field.id, ["a", "a"])
    with closing(service.db.driver.get_connection()) as conn:
        if not service.repository.postgres:
            conn.execute("PRAGMA foreign_keys=OFF").close()
        conn.execute("DELETE FROM works WHERE work_id=?", (owner.id,)).close()
        conn.commit()
    with service.read() as conn:
        for field in fields:
            table, link = names(field)
            storage = link if field.normalized else table
            assert conn.execute(f"SELECT COUNT(*) FROM {q(storage)}").fetchone()[0] == 0
    assert service.audit() == ()


def test_native_endpoint_authority_and_payload_metadata():
    from types import SimpleNamespace

    from LiuXin_alpha.core.program_endpoints.custom_fields import (
        install_commands,
        install_queries,
    )

    queries, commands = {}, {}

    def query(name, handler, **metadata):
        queries[name] = metadata

    def command(name, handler, **metadata):
        commands[name] = metadata

    registrar = SimpleNamespace(
        register_query_handler=query, register_command_handler=command
    )
    handlers = SimpleNamespace(
        native_fields_query=object(), native_fields_command=object()
    )
    install_queries(handlers, registrar)
    install_commands(handlers, registrar)
    assert queries["custom-fields.native.find"]["remote_capabilities"] == ("read",)
    assert queries["custom-fields.native.plan"]["remote_capabilities"] == ("admin",)
    assert commands["custom-fields.native.patch"]["remote_capabilities"] == ("execute",)
    assert commands["custom-fields.native.apply"]["remote_capabilities"] == ("admin",)
    assert commands["custom-fields.native.create"]["remote_capabilities"] == ("admin",)
