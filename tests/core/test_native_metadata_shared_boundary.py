"""Cover shared record families, hierarchy invariants and owner-specific identities."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreRuntime
from tests.core.test_metadata_workflow_boundary import public_client
from tests.core.test_native_metadata_completion import (
    ERRORS,
    node,
    records,
    result,
    step,
)
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_shared_records_and_contextual_attachments(metadata_db, mode, remote):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        work = result(client, "metadata.create", data={"title": "Shared owner"})
        other = result(client, "metadata.create", data={"title": "Other owner"})
        item = result(client, "metadata.create", level="item", data={"type": "digital"})
        agent = result(
            client,
            "metadata.shared.create",
            kind="agents",
            data={"name": "One Agent", "sort_name": "Preserved sort"},
        )
        snapshot = client.query(
            "metadata.shared.read_for_edit", {"kind": "agents", "entity_id": agent}
        )
        result(
            client,
            "metadata.agents.rename",
            entity_id=agent,
            name="Renamed Agent",
            expected_revision=snapshot["revision"],
        )
        assert (
            records(
                client, "metadata.shared.read_many", kind="agents", entity_ids=[agent]
            )[0]["fields"]["sort_name"]
            == "Preserved sort"
        )
        with pytest.raises(ERRORS) as stale:
            client.command(
                "metadata.shared.delete",
                {
                    "kind": "agents",
                    "entity_id": agent,
                    "expected_revision": snapshot["revision"],
                },
            )
        assert stale.value.code == "metadata_conflict"
        for kind in ("tags", "labels", "genres", "subjects", "series"):
            entity_id = result(
                client, "metadata.shared.ensure", kind=kind, text="Shared " + kind
            )
            assert not client.command(
                "metadata.shared.ensure", {"kind": kind, "text": "shared " + kind}
            )["changed"]
            page = client.query(
                "metadata.shared.suggest", {"kind": kind, "text": "SHARED", "limit": 1}
            )
            assert (
                page["total_count"] == 1
                and page["records"][0]["entity_id"] == entity_id
            )
            assert (
                records(
                    client,
                    "metadata.shared.lookup",
                    kind=kind,
                    fields={"name": "Shared " + kind},
                )[0]["entity_id"]
                == entity_id
            )
            token = client.query(
                "metadata.shared.read_for_edit", {"kind": kind, "entity_id": entity_id}
            )["revision"]
            result(
                client,
                "metadata.shared.update",
                kind=kind,
                entity_id=entity_id,
                data={"name": "Changed " + kind},
                expected_revision=token,
            )
            listed = records(client, "metadata.shared.list", kind=kind)
            assert [row["entity_id"] for row in listed] == sorted(
                row["entity_id"] for row in listed
            )
            selected = next(row for row in listed if row["entity_id"] == entity_id)
            assert selected["fields"]["name"] == "Changed " + kind
            result(client, "metadata.shared.delete", kind=kind, entity_id=entity_id)
        rating = result(
            client,
            "metadata.shared.ensure",
            kind="ratings",
            value=3.5,
            out_of=5,
            source="reader",
        )
        assert not client.command(
            "metadata.shared.ensure",
            {"kind": "ratings", "value": 3.5, "out_of": 5, "source": "reader"},
        )["changed"]
        result(client, "metadata.shared.delete", kind="ratings", entity_id=rating)
        for kind in ("notes", "comments", "synopses"):
            attached = result(
                client,
                "metadata.text.add",
                kind=kind,
                entity_id=work,
                text="Same wording",
            )
            distinct = result(
                client,
                "metadata.text.add",
                kind=kind,
                entity_id=other,
                text="Same wording",
            )
            assert attached != distinct
            assert (
                records(client, "metadata.shared.for_owner", kind=kind, entity_id=work)[
                    0
                ]["entity_id"]
                == attached
            )
            result(
                client,
                "metadata.shared.update",
                kind=kind,
                entity_id=attached,
                data={"text": "Edited wording"},
            )
            result(client, "metadata.shared.delete", kind=kind, entity_id=attached)
            assert (
                records(client, "metadata.shared.for_owner", kind=kind, entity_id=work)
                == []
            )
        identifier = result(
            client,
            "metadata.shared.create",
            kind="identifiers",
            data={"scheme": "uuid", "value": "7fc20567-8c3b-4fb2-91b2-42d338f7cfd4"},
        )
        assigned = result(
            client,
            "metadata.identifiers.link",
            identifier_id=identifier,
            entity_id=work,
        )
        copied = result(
            client, "metadata.identifiers.link", identifier_id=assigned, entity_id=other
        )
        with pytest.raises(ERRORS):
            client.command(
                "metadata.identifiers.link",
                {"identifier_id": assigned, "level": "agent", "entity_id": agent},
            )
        agent_source = result(
            client,
            "metadata.shared.create",
            kind="identifiers",
            data={"scheme": "url", "value": "https://example.org/author"},
        )
        agent_identifier = result(
            client,
            "metadata.identifiers.link",
            identifier_id=agent_source,
            level="agent",
            entity_id=agent,
        )
        assert len({assigned, copied, agent_identifier}) == 3
        assert (
            records(
                client,
                "metadata.shared.for_owner",
                kind="identifiers",
                level="agent",
                entity_id=agent,
            )[0]["entity_id"]
            == agent_identifier
        )
        assert (
            records(
                client, "metadata.shared.for_owner", kind="identifiers", entity_id=work
            )[0]["entity_id"]
            == assigned
        )
        observation = result(
            client,
            "metadata.shared.create",
            kind="item_identifiers",
            data={"item_id": item, "scheme": "barcode", "value": "ABC"},
        )
        annotation = result(
            client,
            "metadata.shared.create",
            kind="annotations",
            data={
                "item_id": item,
                "kind": "bookmark",
                "anchor_type": "percentage",
                "anchor_start": "0.5",
                "user_id": 7,
            },
        )
        assert (
            records(
                client,
                "metadata.shared.for_owner",
                kind="item_identifiers",
                level="item",
                entity_id=item,
            )[0]["entity_id"]
            == observation
        )
        assert (
            records(
                client,
                "metadata.shared.for_owner",
                kind="annotations",
                level="item",
                entity_id=item,
                user_id=7,
                annotation_kind="bookmark",
            )[0]["entity_id"]
            == annotation
        )
        assert (
            records(
                client,
                "metadata.shared.for_owner",
                kind="annotations",
                level="item",
                entity_id=item,
                user_id=8,
            )
            == []
        )
        result(
            client,
            "metadata.batch",
            operations=[
                step(
                    "obs",
                    "metadata.shared.update",
                    kind="item_identifiers",
                    entity_id=observation,
                    data={"source": "scanner"},
                ),
                step(
                    "note",
                    "metadata.shared.update",
                    kind="annotations",
                    entity_id=annotation,
                    data={"note_text": "Reviewed"},
                ),
            ],
        )
        for kind, entity_id in (
            ("item_identifiers", observation),
            ("annotations", annotation),
            ("identifiers", assigned),
        ):
            values = records(
                client,
                "metadata.shared.read_many",
                kind=kind,
                entity_ids=[entity_id, 999999, entity_id],
            )
            assert values[0] == values[2] and values[1] is None
            result(client, "metadata.shared.delete", kind=kind, entity_id=entity_id)
        # Composite metadata calls retain Catalog omission-versus-clearing semantics.
        result(
            client,
            "metadata.attach",
            entity_id=work,
            data={
                "notes": ["Retained"],
                "agents": [{"role": "aut", "agent_id": agent}],
            },
        )
        result(client, "metadata.replace", entity_id=work, data={"agents": []})
        assert records(
            client, "metadata.shared.for_owner", kind="notes", entity_id=work
        )
        assert (
            client.query("metadata.credits.list", {"entity_id": work})["credits"] == []
        )
        result(
            client,
            "metadata.credits.set",
            entity_id=work,
            role="aut",
            agent_ids=[agent],
        )
        result(
            client,
            "metadata.credits.set",
            entity_id=work,
            role="edt",
            agent_ids=[agent],
        )
        result(client, "metadata.credits.set", entity_id=work, role="aut", agent_ids=[])
        assert [
            row["role"]
            for row in client.query("metadata.credits.list", {"entity_id": work})[
                "credits"
            ]
        ] == ["edt"]
        tag = result(
            client, "metadata.memberships.tags.add", work_id=work, text="Temporary"
        )
        result(client, "metadata.memberships.tags.remove", work_id=work, entity_id=tag)
        series = result(
            client, "metadata.shared.ensure", kind="series", text="Sequence"
        )
        result(
            client, "metadata.memberships.series.set", work_id=work, entity_ids=[series]
        )
        assert not client.command(
            "metadata.memberships.series.set", {"work_id": work, "entity_ids": [series]}
        )["changed"]
        result(
            client, "metadata.memberships.series.remove", work_id=work, entity_id=series
        )
        result(client, "metadata.shared.delete", kind="series", entity_id=series)
        schema = client.query("metadata.schema", {})
        assert schema["immutable_kinds"] == ["languages"]
        assert (
            "metadata.create" in schema["batch_operations"]
            and "metadata.batch" not in schema["batch_operations"]
        )
        assert records(client, "metadata.shared.list", kind="languages", limit=1)


@pytest.mark.parametrize("kind", ("genres", "subjects", "series"))
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_tree_cycle_reorder_limits_and_atomic_creation(metadata_db, mode, remote, kind):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        root, first, leaf, second = result(
            client,
            "metadata.tree.create",
            kind=kind,
            node=node("Root", node("First", node("Leaf")), node("Second")),
        )
        before = records(client, "metadata.tree.subtree", kind=kind, entity_id=root)
        for name, payload in (
            ("metadata.tree.move", {"entity_id": root, "parent_id": leaf}),
            ("metadata.tree.move", {"entity_id": first, "parent_id": first}),
            (
                "metadata.tree.move",
                {"entity_id": first, "parent_id": root, "before_id": leaf},
            ),
            (
                "metadata.tree.reorder",
                {"parent_id": root, "entity_ids": [first, first]},
            ),
            ("metadata.tree.reorder", {"parent_id": root, "entity_ids": [second]}),
            ("metadata.shared.delete", {"entity_id": first}),
            (
                "metadata.tree.create",
                {"node": node("Too", node("Many")), "max_nodes": 1},
            ),
            (
                "metadata.tree.create",
                {"node": {"data": {"name": "Bad", "parent_id": root}}},
            ),
        ):
            with pytest.raises(ERRORS):
                client.command(name, {"kind": kind, **payload})
            assert (
                records(client, "metadata.tree.subtree", kind=kind, entity_id=root)
                == before
            )
        for name in ("metadata.tree.subtree", "metadata.tree.ancestors"):
            with pytest.raises(ERRORS):
                client.query(
                    name,
                    {
                        "kind": kind,
                        "entity_id": root if name.endswith("subtree") else leaf,
                        "max_nodes": 1,
                    },
                )
        result(client, "metadata.tree.move", kind=kind, entity_id=leaf, parent_id=None)
        assert [
            row["entity_id"]
            for row in records(client, "metadata.tree.roots", kind=kind)
        ] == [root, leaf]
        result(client, "metadata.shared.delete", kind=kind, entity_id=first)
        result(
            client,
            "metadata.tree.reorder",
            kind=kind,
            parent_id=None,
            entity_ids=[leaf, root],
        )
        assert [
            row["entity_id"]
            for row in records(client, "metadata.tree.roots", kind=kind)
        ] == [leaf, root]
        # A tree request with a bad descendant leaves no partly-created roots.
        with pytest.raises(ERRORS):
            client.command(
                "metadata.tree.create",
                {
                    "kind": kind,
                    "node": node("Rolled back", {"data": {"unsupported": "invalid"}}),
                },
            )
        assert len(records(client, "metadata.tree.roots", kind=kind)) == 2


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_reserved_series_sentinel_does_not_enter_user_tree_pages(
    metadata_db, mode, remote
):
    metadata_db.ensure_null_rows()
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        assert records(client, "metadata.tree.roots", kind="series") == []
        assert (
            records(client, "metadata.shared.read_many", kind="series", entity_ids=[0])[
                0
            ]["entity_id"]
            == 0
        )
        first = result(
            client, "metadata.shared.ensure", kind="series", text="First root"
        )
        second = result(
            client, "metadata.shared.ensure", kind="series", text="Second root"
        )
        for offset, expected in ((0, [first]), (1, [second]), (2, [])):
            assert [
                row["entity_id"]
                for row in records(
                    client, "metadata.tree.roots", kind="series", limit=1, offset=offset
                )
            ] == expected
        result(
            client,
            "metadata.tree.reorder",
            kind="series",
            parent_id=None,
            entity_ids=[second, first],
        )
        assert [row["series_id"] for row in runtime.catalog.series.roots()] == [
            second,
            first,
        ]
        result(
            client,
            "metadata.tree.move",
            kind="series",
            entity_id=second,
            parent_id=first,
        )
        result(client, "metadata.tree.delete_subtree", kind="series", entity_id=first)
        assert records(client, "metadata.tree.roots", kind="series") == []
        assert runtime.catalog.series.require(0)["series"] is None
        for name, payload in (
            ("metadata.shared.delete", {"kind": "series", "entity_id": 0}),
            ("metadata.tree.delete_subtree", {"kind": "series", "entity_id": 0}),
        ):
            with pytest.raises(ERRORS) as refused:
                client.command(name, payload)
            assert refused.value.code == "invalid_payload"
