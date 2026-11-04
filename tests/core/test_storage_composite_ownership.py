"""Composite admin writes must keep the record, member rows and revision together."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.errors import CoreDispatchError, CoreHandlerError
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.storage import api

TABLE = "composite_digital_assets"
LINK_TABLE = "composite_digital_asset_digital_asset_links"
LINK_PREFIX = "composite_digital_asset_digital_asset_link_"


@pytest.fixture(params=["SQLite", "SQLite_apsw"])
def composite_core(tmp_path, request):
    if request.param == "SQLite_apsw":
        pytest.importorskip("apsw")
    with create_core(
        database_path=tmp_path / "library.sqlite",
        db_type=request.param,
        create=True,
        enable_maintenance=False,
    ) as runtime:
        yield runtime


def _assets(runtime):
    return tuple(
        runtime.library.storage.declare_digital_asset(
            api.DigitalAssetDeclaration(1, (api.Digest("sha256", digit * 64),))
        ).digital_asset_id
        for digit in ("a", "b")
    )


def _owned_composite(runtime):
    first, second = _assets(runtime)
    return runtime.library.storage.declare_composite_digital_asset(
        api.CompositeDigitalAssetDeclaration(
            (
                api.CompositeDigitalAssetMembership(first, 0, role="chapter"),
                api.CompositeDigitalAssetMembership(
                    second, 1, role="cover", required=False, logical_name="cover.jpg"
                ),
            ),
            name="original",
            attributes=(("source", "fixture"),),
        )
    )


def _rows(runtime, identifier):
    macros = runtime.library.db.macros
    return (
        dict(macros.get_row(TABLE, identifier)),
        [
            dict(row)
            for row in macros.get_rows(
                LINK_TABLE,
                where={LINK_PREFIX + "composite_digital_asset_id": identifier},
                order_by=(LINK_PREFIX + "sequence_number",),
            )
        ],
    )


def test_composite_creation_persists_complete_membership_and_reopens(
    composite_core, tmp_path
):
    runtime = composite_core
    first, second = _assets(runtime)
    rows_before = runtime.library.db.macros.get_rows(TABLE)
    for incomplete in ({"name": "missing members"}, {"members": []}):
        with pytest.raises(CoreHandlerError, match="requires"):
            runtime.command(
                "storage.resource.create",
                {"resource": "composite", "values": incomplete},
            )
    assert runtime.library.db.macros.get_rows(TABLE) == rows_before
    receipt = runtime.command(
        "storage.resource.create",
        {
            "resource": "composite",
            "values": {
                "name": "novel with introduction",
                "attributes": {"edition": "illustrated"},
                "members": [
                    {"digital_asset_id": first, "role": "introduction"},
                    {
                        "digital_asset_id": second,
                        "title": "Novel",
                        "logical_path": "novel.epub",
                        "required": False,
                    },
                ],
            },
        },
    )
    record = runtime.library.storage.get_composite_digital_asset_record(receipt["id"])
    assert record.revision.startswith("d-")
    assert receipt["revision"] == record.revision
    assert record.attributes == (("edition", "illustrated"),)
    assert tuple(member.sequence_number for member in record.members) == (0, 1)
    assert record.members[0].role == "introduction"
    assert record.members[1].required is False
    row, links = _rows(runtime, receipt["id"])
    assert row["composite_digital_asset_name"] == record.name
    assert [link[LINK_PREFIX + "digital_asset_id"] for link in links] == [first, second]
    assert [link[LINK_PREFIX + "is_required"] for link in links] == [1, 0]
    runtime.shutdown()
    with create_core(
        database_path=tmp_path / "library.sqlite",
        create=False,
        enable_maintenance=False,
    ) as reopened:
        assert (
            reopened.library.storage.get_composite_digital_asset_record(receipt["id"])
            == record
        )


@pytest.mark.parametrize("operation", ["create", "update", "delete"])
def test_raw_member_link_writes_are_refused(composite_core, operation):
    runtime = composite_core
    before = _owned_composite(runtime)
    rows = _rows(runtime, before.composite_digital_asset_id)
    member_id = rows[1][0][LINK_PREFIX + "id"]
    with pytest.raises(CoreHandlerError) as failed:
        runtime.command(
            f"storage.resource.{operation}",
            {
                "resource": "composite-member-link",
                "id": member_id,
                "values": {
                    "composite_digital_asset_id": before.composite_digital_asset_id,
                    "digital_asset_id": before.members[0].digital_asset_id,
                    "sequence_number": 2,
                },
            },
        )
    assert failed.value.code == "storage_resource_read_only"
    assert _rows(runtime, before.composite_digital_asset_id) == rows
    inspected = runtime.query(
        "storage.resource.get", {"resource": "composite-member-link", "id": member_id}
    )
    assert inspected["record"] is not None


def test_composite_partial_patch_and_membership_replacement(composite_core):
    runtime = composite_core
    before = _owned_composite(runtime)
    identifier = before.composite_digital_asset_id
    receipt = runtime.command(
        "storage.resource.update",
        {
            "resource": "composite",
            "id": identifier,
            "if_revision": before.revision,
            "values": {"name": "renamed"},
        },
    )
    after = runtime.library.storage.get_composite_digital_asset_record(identifier)
    assert after.name == "renamed"
    assert after.members == before.members
    assert after.attributes == before.attributes
    assert after.revision == receipt["revision"] != before.revision
    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "composite",
                "id": identifier,
                "if_revision": before.revision,
                "values": {"name": "stale"},
            },
        )
    receipt = runtime.command(
        "storage.resource.update",
        {
            "resource": "composite",
            "id": identifier,
            "values": {
                "name": None,
                "attributes": [],
                "members": [
                    {
                        "digital_asset_id": before.members[1].digital_asset_id,
                        "role": "chapter",
                    },
                    {
                        "digital_asset_id": before.members[1].digital_asset_id,
                        "role": "part",
                    },
                ],
            },
        },
    )
    changed = runtime.library.storage.get_composite_digital_asset_record(identifier)
    assert changed.name is None
    assert changed.attributes == ()
    assert [member.digital_asset_id for member in changed.members] == [
        before.members[1].digital_asset_id
    ] * 2
    assert changed.revision == receipt["revision"] != after.revision
    assert len(_rows(runtime, identifier)[1]) == 2


def test_existing_sqlite_member_constraint_failure_preserves_complete_record(
    composite_core,
):
    """Retain the owner's rollback until the ordered-membership schema is repaired."""
    runtime = composite_core
    before = _owned_composite(runtime)
    identifier = before.composite_digital_asset_id
    rows = _rows(runtime, identifier)
    with pytest.raises(CoreHandlerError, match="UNIQUE constraint failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "composite",
                "id": identifier,
                "values": {
                    "name": "must roll back",
                    "members": [
                        {"digital_asset_id": before.members[0].digital_asset_id},
                        {"digital_asset_id": before.members[0].digital_asset_id},
                    ],
                },
            },
        )
    assert _rows(runtime, identifier) == rows
    assert (
        runtime.library.storage.get_composite_digital_asset_record(identifier) == before
    )


def test_composite_partial_patch_fences_intervening_write(composite_core, monkeypatch):
    runtime = composite_core
    manager = runtime.library.storage
    before = _owned_composite(runtime)
    original = manager.replace_composite_digital_asset
    intervening = []

    def interleave(identifier, declaration, *, if_revision=None):
        intervening.append(
            manager.set_composite_digital_asset_attributes(
                identifier,
                (("concurrent", "preserve me"),),
                if_revision=before.revision,
            )
        )
        return original(identifier, declaration, if_revision=if_revision)

    monkeypatch.setattr(manager, "replace_composite_digital_asset", interleave)
    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "composite",
                "id": before.composite_digital_asset_id,
                "values": {"name": "stale patch"},
            },
        )
    assert (
        manager.get_composite_digital_asset_record(before.composite_digital_asset_id)
        == intervening[0]
    )


def test_composite_member_failure_rolls_back_record_and_links(
    composite_core, monkeypatch
):
    runtime = composite_core
    before = _owned_composite(runtime)
    identifier = before.composite_digital_asset_id
    rows = _rows(runtime, identifier)
    macros = runtime.library.db.macros
    original = macros.insert_rows
    inserted = []

    def fail_second_member(table, values):
        values = tuple(values)
        if table == LINK_TABLE:
            original(table, values[:1])
            inserted.extend(values[:1])
            raise OSError("second member persistence failed")
        return original(table, values)

    monkeypatch.setattr(macros, "insert_rows", fail_second_member)
    with pytest.raises(CoreHandlerError, match="second member persistence failed"):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "composite",
                "id": identifier,
                "values": {"name": "must roll back"},
            },
        )
    assert inserted
    assert _rows(runtime, identifier) == rows
    assert (
        runtime.library.storage.get_composite_digital_asset_record(identifier) == before
    )


def test_composite_delete_checks_references_and_revision(composite_core):
    runtime = composite_core
    manager = runtime.library.storage
    before = _owned_composite(runtime)
    identifier = before.composite_digital_asset_id
    item_id = runtime.library.db.macros.insert_row(
        "items", {"item_type": "digital", "item_source": "core-composite-test"}
    )
    manager.link_item_to_composite_digital_asset(item_id, identifier)
    with pytest.raises(CoreHandlerError, match="still linked"):
        runtime.command(
            "storage.resource.delete", {"resource": "composite", "id": identifier}
        )
    manager.unlink_item_digital_asset(item_id)
    changed = manager.set_composite_digital_asset_name(identifier, "changed")
    with pytest.raises(CoreHandlerError, match="revision precondition failed"):
        runtime.command(
            "storage.resource.delete",
            {"resource": "composite", "id": identifier, "if_revision": before.revision},
        )
    receipt = runtime.command(
        "storage.resource.delete",
        {"resource": "composite", "id": identifier, "if_revision": changed.revision},
    )
    assert receipt["deleted"] is True
    assert runtime.library.db.macros.get_row(TABLE, identifier) is None
    assert not runtime.library.db.macros.get_rows(
        LINK_TABLE, where={LINK_PREFIX + "composite_digital_asset_id": identifier}
    )
    for member in before.members:
        assert manager.get_digital_asset_record(member.digital_asset_id)
    assert (
        runtime.command(
            "storage.resource.delete", {"resource": "composite", "id": identifier}
        )["deleted"]
        is False
    )


def test_composite_readback_failure_retains_commit_receipt(composite_core, monkeypatch):
    from LiuXin_alpha.core import storage_graph_api

    runtime = composite_core
    first, _second = _assets(runtime)

    def fail_readback(*_args, **_kwargs):
        raise OSError("readback unavailable")

    monkeypatch.setattr(
        storage_graph_api,
        "_macros",
        lambda _runtime: SimpleNamespace(get_row=fail_readback),
    )
    with pytest.raises(CoreHandlerError) as failed:
        runtime.command(
            "storage.resource.create",
            {
                "resource": "composite",
                "values": {"members": [{"digital_asset_id": first}]},
            },
        )
    assert failed.value.code == "cache_reconciliation_failed"
    assert failed.value.details["canonical_write_committed"] is True
    receipt = failed.value.details["receipt"]
    assert receipt["created"] is True
    record = runtime.library.storage.get_composite_digital_asset_record(receipt["id"])
    assert record.revision == receipt["revision"]
    assert record.members[0].digital_asset_id == first


@pytest.mark.parametrize(
    "values",
    [
        {"members": []},
        {"members": [{"digital_asset_id": 999999}]},
        {"members": [{"digital_asset_id": 1, "sequence_number": 1}]},
        {"members": [{"digital_asset_id": True}]},
        {"members": [{"digital_asset_id": 1, "required": "false"}]},
        {"members": [{"digital_asset_id": 1, "surprise": "ignored?"}]},
        {"scratch": "{}"},
        {"media_category": "ebook"},
        {"name": "  "},
        {"attributes": [["duplicate", "one"], ["duplicate", "two"]]},
    ],
)
def test_invalid_composite_update_preserves_complete_record(composite_core, values):
    runtime = composite_core
    before = _owned_composite(runtime)
    rows = _rows(runtime, before.composite_digital_asset_id)
    with pytest.raises((CoreDispatchError, CoreHandlerError)):
        runtime.command(
            "storage.resource.update",
            {
                "resource": "composite",
                "id": before.composite_digital_asset_id,
                "values": values,
            },
        )
    assert _rows(runtime, before.composite_digital_asset_id) == rows
    assert (
        runtime.library.storage.get_composite_digital_asset_record(
            before.composite_digital_asset_id
        )
        == before
    )
