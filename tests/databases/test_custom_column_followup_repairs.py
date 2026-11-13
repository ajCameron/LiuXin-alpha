"""Protect native and legacy boundaries identified by CC07 through CC12."""

from contextlib import closing, nullcontext
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.custom_fields import CustomFieldEvaluator
from LiuXin_alpha.configuration import current_preferences
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.databases.custom_columns import (
    CustomFieldService,
)
from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    OwnerRef,
    UnsupportedStorage,
)
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.utils.calibre_compat.custom_columns import (
    CustomColumns,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import CustomColumnAdapter
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)
from tests.databases.test_custom_column_repairs import Cache
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


def _runtime(db):
    return CoreRuntime(
        library=SimpleNamespace(database=db, storage=None),
        preferences=current_preferences(),
    )


def _metadata_edit(db, route, number, **changes):
    if route == "wrapper":
        return CustomColumnAdapter(db).set_custom_column_metadata(number, **changes)
    runtime = _runtime(db)
    try:
        result = runtime.execute_command(
            CoreCommand("custom-fields.update", {"num": number, "changes": changes})
        )
        assert result.ok
        return result.result["changed_row_ids"]
    finally:
        runtime.shutdown()


@pytest.mark.parametrize("route", ["native", "core"])
@pytest.mark.parametrize("schema", ["current", "old"])
def test_unsupported_storage_retirement_is_rejected(db, route, schema):
    """Refusal cannot set the legacy purge flag, even without native metadata columns."""
    owner = OwnerRef("works", db.macros.insert_row("works", {"work_title": "Kept"}))
    legacy = CustomColumns(db, table="works")
    number = legacy.create_custom_column("Review", label="review")
    legacy.set_custom(owner.id, "valuable", num=number)
    with db.macros.transaction() as conn:
        conn.execute(
            "UPDATE custom_columns SET custom_column_storage_version=1 WHERE custom_column_id=?",
            (number,),
        ).close()
    if schema == "old":
        with db.macros.transaction() as conn:
            for suffix in ("collection_mode", "revision", "storage_version"):
                conn.execute(
                    f'ALTER TABLE custom_columns DROP COLUMN "custom_column_{suffix}"'
                ).close()
    service = CustomFieldService(db)
    notices = []
    service.subscribe(notices.append)
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    if route == "native":
        with pytest.raises(
            UnsupportedStorage, match="Unsupported custom-field storage"
        ):
            service.retire_field(number)
    else:
        runtime = _runtime(db)
        try:
            with pytest.raises(
                CoreHandlerError, match="Unsupported custom-field storage"
            ):
                runtime.execute_command(
                    CoreCommand("custom-fields.native.retire", {"field": number})
                )
        finally:
            runtime.shutdown()
    assert not notices
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
    with pytest.raises(UnsupportedStorage):
        legacy.refresh_db_custom_columns_metadata()
    with pytest.raises(UnsupportedStorage):
        CustomColumns(db, table="works")
    with pytest.raises(UnsupportedStorage):
        CustomColumnAdapter(db).deleted_marked_custom_columns()
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
        assert rows(
            conn,
            "SELECT custom_column_mark_for_delete FROM custom_columns WHERE custom_column_id=?",
            (number,),
        ) == [(0,)]


@pytest.mark.parametrize("route", ["wrapper", "core"])
def test_native_metadata_routes_preserve_enum_values_and_revisions(native, route):
    """Both public adapters validate conversions and advance valid-edit revisions."""
    service, (owner, _) = native
    field = service.create_field(
        "works", "state", datatype="enumeration", display={"enum_values": ["Ready"]}
    )
    service.set_value(owner, field.id, "Ready")
    original = service.get_field(field.id)
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    error = CustomFieldError if route == "wrapper" else CoreHandlerError
    with pytest.raises(error, match="conversion"):
        _metadata_edit(service.db, route, field.id, display={"enum_values": ["Done"]})
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
    assert service.get_value(owner, field.id) == "Ready"
    assert _metadata_edit(
        service.db, route, field.id, display={"enum_values": ["Ready", "Done"]}
    )
    assert service.get_field(field.id).revision == original.revision + 1
    assert service.get_value(owner, field.id) == "Ready"
    service.set_value(owner, field.id, "Done")
    assert service.get_value(owner, field.id) == "Done"
    assert service.audit() == ()


def test_facade_creates_and_uses_native_storage(db):
    """Every facade instance uses native validation and retirement."""
    owner = OwnerRef("works", db.macros.insert_row("works", {"work_title": "Kept"}))
    facade = CustomColumns(db, table="works")
    number = facade.create_custom_column(
        "Status",
        datatype="enumeration",
        label="status",
        display={"enum_values": ["Ready"]},
    )
    facade.set_custom(owner.id, "Ready", num=number)
    retirement_facade = CustomColumns(db, table="works")
    service = CustomFieldService(db)
    assert facade.custom_field_metadata(num=number)["storage_version"] == 2
    revision = service.get_field(number).revision
    with pytest.raises(CustomFieldError, match="conversion"):
        facade.set_custom_column_metadata(number, display={"enum_values": ["Done"]})
    assert service.get_value(owner, number) == "Ready"
    assert service.get_field(number).revision == revision
    facade.set_custom_column_metadata(
        number, display={"enum_values": ["Ready", "Done"]}
    )
    assert service.get_field(number).revision == revision + 1
    with db.macros.transaction():
        with pytest.raises(RuntimeError, match="own transaction"):
            retirement_facade.delete_custom_column(number)
    assert not service.get_field(number).retired
    retirement_facade.delete_custom_column(number)
    assert service.get_field(number, include_retired=True).retired
    assert number not in retirement_facade.custom_column_num_map


@pytest.mark.parametrize("route", ["wrapper", "core"])
def test_native_metadata_routes_preserve_composite_dependencies(native, route):
    service, (owner, _) = native
    field = service.create_field("works", "state")
    service.set_value(owner, field.id, "Ready")
    composite = service.create_field(
        "works",
        "caption",
        datatype="composite",
        display={"composite_template": "{#state}"},
    )
    error = CustomFieldError if route == "wrapper" else CoreHandlerError
    with pytest.raises(error, match="depend"):
        _metadata_edit(service.db, route, field.id, label="renamed")
    assert service.get_field(field.id).label == "state"
    assert CustomFieldEvaluator(service).get_value(owner, composite.id) == "Ready"


@pytest.mark.parametrize("commit", [False, True])
def test_native_wrapper_metadata_joins_managed_transaction(native, commit):
    service, _ = native
    field = service.create_field("works", "state")
    boundary = (
        nullcontext() if commit else pytest.raises(RuntimeError, match="outer rollback")
    )
    with boundary, service.db.macros.transaction():
        assert CustomColumnAdapter(service.db).set_custom_column_metadata(
            field.id, name="Changed"
        )
        assert service.get_field(field.id).revision == 1
        with closing(service.db.driver.get_connection()) as independent:
            assert rows(
                independent,
                "SELECT custom_column_name FROM custom_columns WHERE custom_column_id=?",
                (field.id,),
            ) == [(field.name,)]
        if not commit:
            raise RuntimeError("outer rollback")
    current = service.get_field(field.id)
    assert current.name == ("Changed" if commit else field.name)
    assert current.revision == int(commit)
    assert service.audit() == ()


def test_native_wrapper_metadata_refuses_unmanaged_transaction(native):
    service, _ = native
    field = service.create_field("works", "state")
    with transaction_scope(service.db.driver.conn, owns_transaction=True) as conn:
        conn.execute("INSERT INTO works(work_title) VALUES ('caller work')").close()
        with pytest.raises(RuntimeError, match="owned or managed transaction"):
            CustomColumnAdapter(service.db).set_custom_column_metadata(
                field.id, name="Changed"
            )
        assert rows(conn, "SELECT work_id FROM works WHERE work_title='caller work'")
    assert service.get_field(field.id) == field


def test_native_retirement_requires_version_two(native):
    """The storage marker is checked before retirement writes on every backend."""
    service, (owner, _) = native
    field = service.create_field("works", "state")
    service.set_value(owner, field.id, "kept")
    with service.write() as conn:
        conn.execute(
            "UPDATE custom_columns SET custom_column_storage_version=1 WHERE custom_column_id=?",
            (field.id,),
        ).close()
    with pytest.raises(UnsupportedStorage):
        service.retire_field(field.id)
    with service.read() as conn:
        assert rows(
            conn,
            "SELECT custom_column_mark_for_delete FROM custom_columns WHERE custom_column_id=?",
            (field.id,),
        ) == [(0,)]
        assert service.repository.version(conn, field) == 1
    with service.write() as conn:
        conn.execute(
            "UPDATE custom_columns SET custom_column_storage_version=2 WHERE custom_column_id=?",
            (field.id,),
        ).close()
    assert service.get_value(owner, field.id) == "kept"


@pytest.mark.parametrize("operation", ["metadata", "retire"])
@pytest.mark.parametrize("transaction", ["primary", "macro"])
@pytest.mark.parametrize("commit", [False, True])
def test_native_facade_edits_refuse_borrowed_publication(
    native, operation, transaction, commit
):
    service, (owner, _) = native
    field = service.create_field("works", "state")
    service.set_value(owner, field.id, "kept")
    facade = CustomColumns(service.db, table="works")
    fields_before = deepcopy(facade.field_metadata.copy())
    maps_before = deepcopy(facade.custom_column_num_map)
    notices = []
    facade.notify = lambda *args: notices.append(args)

    def edit():
        if operation == "metadata":
            facade.set_custom_column_metadata(field.id, label="changed")
        else:
            facade.delete_custom_column(field.id)

    unit = (
        service.db.macros.transaction()
        if transaction == "macro"
        else transaction_scope(service.db.driver.conn, owns_transaction=True)
    )
    outcome = (
        nullcontext() if commit else pytest.raises(RuntimeError, match="outer rollback")
    )
    with outcome, unit as conn:
        conn.execute("INSERT INTO works(work_title) VALUES ('caller work')").close()
        with pytest.raises(RuntimeError, match="own transaction"):
            edit()
        assert not notices
        assert facade.custom_column_num_map == maps_before
        assert facade.field_metadata.copy() == fields_before
        if not commit:
            raise RuntimeError("outer rollback")
    with service.read() as conn:
        assert len(
            rows(conn, "SELECT work_id FROM works WHERE work_title='caller work'")
        ) == int(commit)
    assert service.get_field(field.id).label == "state"
    assert service.get_value(owner, field.id) == "kept"
    edit()
    if operation == "metadata":
        assert service.get_field(field.id).label == "changed"
        assert "changed" in facade.custom_column_label_map
        assert len(notices) == 1
    else:
        assert service.get_field(field.id, include_retired=True).retired
        assert field.id not in facade.custom_column_num_map


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    "operation", ["set", "bulk_set", "append", "bulk_add", "rename", "rename_unused"]
)
def test_incompatible_legacy_values_are_rejected_before_commit(
    native, cached, operation
):
    service, owners = native
    field = service.create_field("works", "tags", mode="set")
    for owner in owners:
        service.set_value(owner, field.id, ["initial"])
    if operation == "rename_unused":
        service.set_value(owners[1], field.id, ["unused"])
        service.set_value(owners[1], field.id, ["initial"])
    facade = CustomColumns(service.db, table="works")
    cache = Cache([owner.id for owner in owners]) if cached else None
    if cache is not None:
        for owner in owners:
            cache._data[owner.id][1] = "initial"
        facade.data = cache
        facade.FIELD_MAP = {"id": 0, field.id: 1}
    before_cache = deepcopy(cache._data) if cache is not None else None
    notices, dirtied = [], []
    facade.notify = lambda *args: notices.append(args)
    facade.dirtied = lambda *args, **kwargs: dirtied.append(args)
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    with pytest.raises(CompatibilityError, match="Delimiter-bearing"):
        if operation in {"set", "append"}:
            facade.set_custom(
                owners[0].id,
                ["valid", "a|b"],
                num=field.id,
                append=operation == "append",
            )
        elif operation == "bulk_set":
            facade.set_custom_bulk(
                [owner.id for owner in owners], ["valid", "a|b"], num=field.id
            )
        elif operation == "bulk_add":
            facade.set_custom_bulk_multiple(
                [owner.id for owner in owners], add=["valid", "a|b"], num=field.id
            )
        else:
            spelling = "unused" if operation == "rename_unused" else "initial"
            value_id = next(
                item["id"]
                for item in service.inventory(field.id).items
                if item["value"] == spelling
            )
            facade.rename_custom_item(value_id, "a|b", num=field.id)
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before
    assert not notices and not dirtied
    assert facade._data_cache is cache
    if cache is not None:
        assert cache._data == before_cache
    for owner in owners:
        assert facade.get_custom(owner.id, num=field.id, index_is_id=True) == [
            "initial"
        ]
    service.set_value(owners[0], field.id, ["a|b"])
    assert [entry.value for entry in service.get_value(owners[0], field.id)] == ["a|b"]


def test_scalar_compatibility_still_accepts_delimiters(native):
    service, (owner, _) = native
    field = service.create_field("works", "note")
    facade = CustomColumns(service.db, table="works")
    facade.set_custom(owner.id, "a|b", num=field.id)
    assert facade.get_custom(owner.id, num=field.id, index_is_id=True) == "a|b"


@pytest.mark.parametrize(
    "spelling,removed",
    [
        ("Ready", "READY"),
        ("café", "CAFE\u0301"),
        ("Straße", "STRASSE"),
        ("A; B", " A,  B "),
    ],
)
@pytest.mark.parametrize("add_wins", [False, True])
def test_native_bulk_removal_matches_canonical_identity(
    native, spelling, removed, add_wins
):
    service, owners = native
    field = service.create_field("works", "tags", mode="set")
    for owner in owners:
        service.set_value(owner, field.id, [spelling, "unrelated"])
    originals = {owner: service.get_value(owner, field.id) for owner in owners}
    facade = CustomColumns(service.db, table="works")
    facade.set_custom_bulk_multiple(
        [owner.id for owner in owners],
        add=[removed] if add_wins else [],
        remove=iter([removed]),
        num=field.id,
    )
    for owner in owners:
        expected = (
            originals[owner] if add_wins else (replace(originals[owner][1], ordinal=0),)
        )
        assert service.get_value(owner, field.id) == expected


_CLEANUP_CASES = [
    ("text", "owner_delete"),
    ("text", "value_delete"),
    ("text", "book_insert"),
    ("text", "book_update"),
    ("text", "value_insert"),
    ("text", "value_update"),
    ("int", "owner_delete"),
    ("int", "book_insert"),
    ("int", "book_update"),
]


@pytest.mark.parametrize("datatype,role", _CLEANUP_CASES)
@pytest.mark.parametrize("damage", ["missing", "ineffective"])
def test_cleanup_audit_and_repair_cover_every_required_trigger(
    native, datatype, role, damage
):
    """Repair the reported protection, then exercise it with FK enforcement off."""
    service, (owner, second) = native
    field = service.create_field("works", "protected", datatype=datatype)
    service.set_value(owner, field.id, "kept" if datatype == "text" else 42)
    table, link = names(field)
    storage = link if field.normalized else table
    ss = stem(storage)
    if role == "owner_delete":
        trigger = "custom_owner_delete_" + storage
    elif role == "value_delete":
        trigger = "custom_value_delete_" + table
    else:
        trigger = "custom_check_" + storage + "_" + role
    with service.write() as conn:
        sql = rows(conn, "SELECT sql FROM sqlite_master WHERE name=?", (trigger,))[0][0]
        conn.execute(f"DROP TRIGGER {q(trigger)}").close()
        if damage == "ineffective":
            altered = (
                sql.replace(" THEN RAISE", " AND 0 THEN RAISE")
                if role.endswith(("insert", "update"))
                else sql.replace("; END", " AND 0; END")
            )
            assert altered != sql
            conn.execute(altered).close()
    issues = service.audit()
    assert len(issues) == 1
    assert issues[0].code == "cleanup_trigger" and issues[0].field_id == field.id
    assert trigger in issues[0].detail
    plan = service.plan_repair(actions=("cleanup_trigger",))
    assert not plan.issues and len(plan.effects) == 1 and trigger in plan.effects[0]
    with service.read() as conn:
        actual = rows(conn, "SELECT sql FROM sqlite_master WHERE name=?", (trigger,))
        assert actual == ([] if damage == "missing" else [(altered,)])
    service.apply_repair(plan)
    assert service.audit() == ()

    with closing(service.db.driver.get_connection()) as conn:
        conn.execute("PRAGMA foreign_keys=OFF").close()
        before = rows(conn, f"SELECT * FROM {q(storage)}")
        if role == "owner_delete":
            conn.execute("DELETE FROM works WHERE work_id=?", (owner.id,)).close()
        elif role == "value_delete":
            conn.execute(f"DELETE FROM {q(table)}").close()
        elif role.endswith("update"):
            column = ss + ("_book" if role.startswith("book") else "_value")
            with pytest.raises(Exception, match="Custom field reference violation"):
                conn.execute(f"UPDATE {q(storage)} SET {q(column)}=999999").close()
        else:
            record = {
                ss + "_book": 999999 if role.startswith("book") else second.id,
                ss + "_ordinal": 0,
                ss + "_value": 999999 if role.startswith("value") else before[0][3],
            }
            if not field.normalized:
                record[ss + "_key"] = "test-key"
            with pytest.raises(Exception, match="Custom field reference violation"):
                conn.execute(
                    f"INSERT INTO {q(storage)} ({','.join(q(k) for k in record)}) VALUES ({','.join('?' for _ in record)})",
                    tuple(record.values()),
                ).close()
        conn.commit()
        assert rows(conn, f"SELECT * FROM {q(storage)}") == (
            [] if role.endswith("delete") else before
        )
    assert service.audit() == ()
