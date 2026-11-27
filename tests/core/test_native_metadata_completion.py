"""Exercise complete native client workflows, atomic edits and failure receipts."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.errors import CoreError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.databases.transaction_participants import after_commit
from tests.core.test_metadata_workflow_boundary import public_client
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

ERRORS = (CoreError, RemoteProxyError)


def node(name, *children):
    return {"data": {"name": name}, "children": list(children)}


def result(client, operation, **payload):
    receipt = client.command(operation, payload)
    assert receipt["persistence"] == "committed"
    assert receipt["cache"] in ("current", "not_configured")
    return receipt["result"]


def step(key, operation, **payload):
    return {"key": key, "operation": operation, "payload": payload}


def ref(key, *path):
    return {"$ref": key, "path": list(path)}


def records(client, name, **payload):
    return client.query(name, payload)["records"]


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_complete_native_client_workflow(metadata_db, mode, remote):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        ids = result(
            client,
            "metadata.wemi.create_stack",
            work={"title": "Native library"},
            expression={"label": "Original"},
            manifestation={"edition_statement": "First"},
            items=[{"inventory_code": "COPY-1"}],
        )
        work, expression, manifestation, item = (
            ids["work_id"],
            ids["expression_id"],
            ids["manifestation_id"],
            ids["item_ids"][0],
        )
        agent = result(
            client, "metadata.agents.create_person", data={"name": "Native Author"}
        )
        result(
            client, "metadata.credits.add", entity_id=work, agent_id=agent, role="aut"
        )
        result(client, "metadata.memberships.tags.add", work_id=work, text="Native Tag")
        tag = client.query("metadata.memberships.tags.list", {"work_id": work})[
            "memberships"
        ][0]["member"]["entity_id"]
        assert (
            client.query("metadata.credits.list", {"entity_id": work})["credits"][0][
                "agent"
            ]["entity_id"]
            == agent
        )
        assert (
            records(client, "metadata.shared.suggest", kind="tags", text="native")[0][
                "entity_id"
            ]
            == tag
        )
        assert client.query("metadata.search", {"query": "work.title:library"})[
            "references"
        ] == [{"level": "work", "entity_id": work}]
        for selected in ("work", "expression", "manifestation", "item"):
            schema = client.query("metadata.search.schema", {"level": selected})
            assert schema["fields"] and schema["sort_fields"]
            assert client.query(
                "metadata.search.validate", {"level": selected, "query": ""}
            ) == {"valid": True}
        for selected, entity_id, data in (
            ("work", work, {"title": "Revised library"}),
            ("expression", expression, {"label": "Revised"}),
            ("manifestation", manifestation, {"edition_statement": "Second"}),
            ("item", item, {"inventory_code": "COPY-2"}),
        ):
            snapshot = client.query(
                "metadata.read_for_edit", {"level": selected, "entity_id": entity_id}
            )
            result(
                client,
                "metadata.update",
                level=selected,
                entity_id=entity_id,
                data=data,
                expected_revision=snapshot["revision"],
            )
            with pytest.raises(ERRORS) as stale:
                client.command(
                    "metadata.delete",
                    {
                        "level": selected,
                        "entity_id": entity_id,
                        "expected_revision": snapshot["revision"],
                    },
                )
            assert stale.value.code == "metadata_conflict"
            no_change = client.command(
                "metadata.update",
                {"level": selected, "entity_id": entity_id, "data": {}},
            )
            assert not no_change["changed"]
        selected = client.query(
            "metadata.read_many", {"entity_ids": [work, 999999, work]}
        )["entities"]
        assert selected[0] == selected[2] and selected[1] is None
        selected[0]["work"]["work_title"] = "Detached edit"
        assert (
            client.query("metadata.read_many", {"entity_ids": [work]})["entities"][0][
                "work"
            ]["work_title"]
            == "Revised library"
        )
        # Trees and Work membership share one coordinated mutation boundary.
        root, first, leaf, second = result(
            client,
            "metadata.tree.create",
            kind="series",
            node=node("Root", node("First", node("Leaf")), node("Second")),
        )
        assert [
            row["entity_id"]
            for row in records(
                client, "metadata.tree.ancestors", kind="series", entity_id=leaf
            )
        ] == [root, first]
        assert not client.command(
            "metadata.tree.ensure_path",
            {"kind": "series", "names": ["root", "first", "leaf"]},
        )["changed"]
        result(
            client,
            "metadata.tree.reorder",
            kind="series",
            parent_id=root,
            entity_ids=[second, first],
        )
        assert [
            row["entity_id"]
            for row in records(
                client, "metadata.tree.children", kind="series", entity_id=root
            )
        ] == [second, first]
        result(
            client,
            "metadata.tree.move",
            kind="series",
            entity_id=leaf,
            parent_id=root,
            before_id=first,
        )
        result(client, "metadata.memberships.series.add", work_id=work, entity_id=leaf)
        with pytest.raises(ERRORS):
            client.command(
                "metadata.tree.delete_subtree", {"kind": "series", "entity_id": root}
            )
        assert (
            len(records(client, "metadata.tree.subtree", kind="series", entity_id=root))
            == 4
        )
        result(
            client,
            "metadata.tree.delete_subtree",
            kind="series",
            entity_id=root,
            detach=True,
        )
        assert (
            client.query("metadata.memberships.series.list", {"work_id": work})[
                "memberships"
            ]
            == []
        )
        assert records(client, "metadata.tree.roots", kind="series") == []
        # Native unlink/link preserves both entities and their hydrated projections.
        edge = {
            "parent_level": "work",
            "parent_id": work,
            "child_level": "expression",
            "child_id": expression,
        }
        assert result(client, "metadata.wemi.unlink", **edge)
        assert not result(client, "metadata.wemi.unlink", **edge)
        result(client, "metadata.wemi.link", **edge)
        result(client, "metadata.memberships.tags.set", work_id=work, entity_ids=[])
        result(
            client,
            "metadata.credits.remove",
            entity_id=work,
            agent_id=agent,
            role="aut",
        )
        for selected, entity_id in (
            ("item", item),
            ("manifestation", manifestation),
            ("expression", expression),
            ("work", work),
        ):
            snapshot = client.query(
                "metadata.read_for_edit", {"level": selected, "entity_id": entity_id}
            )
            result(
                client,
                "metadata.delete",
                level=selected,
                entity_id=entity_id,
                expected_revision=snapshot["revision"],
            )
            assert client.query(
                "metadata.read_many", {"level": selected, "entity_ids": [entity_id]}
            )["entities"] == [None]
        assert client.query("metadata.search", {})["total_count"] == 0
        assert (
            records(
                client, "metadata.shared.read_many", kind="agents", entity_ids=[agent]
            )[0]["entity_id"]
            == agent
        )


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("remote", (False, True))
def test_batch_connected_creation_rollback_revisions_and_publication(
    metadata_db, mode, remote, monkeypatch
):
    with (
        CoreRuntime(
            library=SimpleNamespace(database=metadata_db), cache_type=mode
        ) as runtime,
        public_client(runtime, remote, timeout_seconds=120) as (client, _),
    ):
        operations = [
            step(
                "stack",
                "metadata.wemi.create_stack",
                work={"title": "Batch library"},
                expression={"label": "Original"},
                manifestation={"edition_statement": "First"},
                items=[{"inventory_code": "BATCH-COPY"}],
            ),
            step(
                "agent",
                "metadata.agents.create_organisation",
                data={"name": "Batch Press"},
            ),
            step(
                "credit",
                "metadata.credits.add",
                level="manifestation",
                entity_id=ref("stack", "manifestation_id"),
                agent_id=ref("agent"),
                role="pbl",
            ),
            step("tag", "metadata.shared.ensure", kind="tags", text="Batch Tag"),
            step(
                "membership",
                "metadata.memberships.tags.add",
                work_id=ref("stack", "work_id"),
                tag_id=ref("tag"),
            ),
        ]
        receipt = client.command("metadata.batch", {"operations": operations})
        assert receipt["persistence"] == "committed" and receipt["changed"]
        results = {row["key"]: row["result"] for row in receipt["result"]}
        work = results["stack"]["work_id"]
        assert (
            client.query("metadata.search", {"query": "work.title:Batch"})[
                "total_count"
            ]
            == 1
        )
        snapshot = client.query("metadata.read_for_edit", {"entity_id": work})
        shared_snapshot = client.query(
            "metadata.shared.read_for_edit",
            {"kind": "tags", "entity_id": results["tag"]},
        )
        result(
            client,
            "metadata.batch",
            expected=[snapshot["revision"], shared_snapshot["revision"]],
            operations=[
                step(
                    "update",
                    "metadata.update",
                    entity_id=work,
                    data={"title": "Batch edited"},
                ),
                step(
                    "rename",
                    "metadata.shared.update",
                    kind="tags",
                    entity_id=results["tag"],
                    data={"name": "Edited Tag"},
                ),
            ],
        )
        with pytest.raises(ERRORS) as stale:
            client.command(
                "metadata.batch",
                {
                    "expected": [snapshot["revision"]],
                    "operations": [
                        step(
                            "create",
                            "metadata.create",
                            data={"title": "Should not exist"},
                        )
                    ],
                },
            )
        assert stale.value.code == "metadata_conflict"
        before = client.query("metadata.search", {})["total_count"]
        for failure in (
            step("bad", "metadata.update", entity_id=999999, data={"title": "Missing"}),
            step(
                "bad",
                "metadata.shared.create",
                kind="tags",
                data={"unknown_alias": "No"},
            ),
            step("bad", "metadata.update", entity_id=ref("new", "missing"), data={}),
        ):
            with pytest.raises(ERRORS) as failed:
                client.command(
                    "metadata.batch",
                    {
                        "operations": [
                            step(
                                "new", "metadata.create", data={"title": "Rolled back"}
                            ),
                            failure,
                        ]
                    },
                )
            assert failed.value.details["operation_index"] == 1
            assert client.query("metadata.search", {})["total_count"] == before
        assert not client.command("metadata.batch", {"operations": []})["changed"]
        assert not client.command(
            "metadata.batch",
            {"operations": [step("no-op", "metadata.update", entity_id=work, data={})]},
        )["changed"]
        # Successful canonical effects retain the entire aggregate receipt if publication fails.
        original = runtime.catalog.works.create

        def broken_publication():
            raise OSError("batch publication failed")

        def create_then_fail(data):
            value = original(data)
            after_commit(metadata_db.macros, broken_publication)
            return value

        with monkeypatch.context() as broken:
            if mode in (None, "database_backed"):
                broken.setattr(runtime.catalog.works, "create", create_then_fail)
            else:
                broken.setattr(
                    runtime.cache,
                    "reload_data",
                    Mock(side_effect=OSError("batch cache refresh failed")),
                )
            with pytest.raises(ERRORS) as committed:
                client.command(
                    "metadata.batch",
                    {
                        "operations": [
                            step(
                                "survives",
                                "metadata.create",
                                data={"title": "Durable batch"},
                            )
                        ]
                    },
                )
        assert committed.value.code == "metadata_reconciliation_failed"
        assert committed.value.details["canonical_write_committed"] is True
        record = committed.value.details["receipt"]["result"][0]
        assert record["key"] == "survives"
        assert (
            client.query("metadata.read_many", {"entity_ids": [record["result"]]})[
                "entities"
            ][0]["work"]["work_title"]
            == "Durable batch"
        )
