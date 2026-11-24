"""Refuse malformed native operations and unauthorized batch escalation before writes."""

from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreHttpDaemon
from LiuXin_alpha.core.errors import CoreError
from LiuXin_alpha.core.program_services.native_metadata import ROUTES
from LiuXin_alpha.core.proxies.remote import RemoteCoreClient, RemoteProxyError
from tests.core.test_metadata_workflow_boundary import public_client

INVALID = (
    ("metadata.create", {"level": "files", "data": {}}),
    ("metadata.create", {"level": True, "data": {}}),
    ("metadata.create", {"data": {}, "physical_table": "works"}),
    ("metadata.delete", {"entity_id": True}),
    ("metadata.delete", {"entity_id": "1"}),
    ("metadata.delete", {"entity_id": 0}),
    ("metadata.update", {"entity_id": 1, "data": []}),
    (
        "metadata.update",
        {
            "entity_id": 1,
            "data": {},
            "expected_revision": {
                "reference": {"kind": "tags", "entity_id": 1},
                "digest": "x",
            },
        },
    ),
    (
        "metadata.update",
        {
            "entity_id": 1,
            "data": {},
            "expected_revision": {
                "reference": {"level": "work", "entity_id": True},
                "digest": "x",
            },
        },
    ),
    (
        "metadata.update",
        {
            "entity_id": 1,
            "data": {},
            "expected_revision": {
                "reference": {"level": "work", "entity_id": 1},
                "digest": "",
            },
        },
    ),
    ("metadata.read_many", {"entity_ids": [1, False]}),
    ("metadata.read_many", {"entity_ids": [1] * 1001}),
    ("metadata.search", {"limit": None}),
    ("metadata.search", {"limit": 1001}),
    ("metadata.search", {"offset": -1}),
    ("metadata.search", {"filters": [True]}),
    ("metadata.search", {"sort": [{"field": "id", "unknown": 1}]}),
    ("metadata.shared.create", {"kind": "languages", "data": {"name": "New"}}),
    ("metadata.shared.read_for_edit", {"kind": "languages", "entity_id": 1}),
    (
        "metadata.shared.update",
        {
            "kind": "tags",
            "entity_id": 1,
            "data": {},
            "expected_revision": {
                "reference": {"level": "work", "entity_id": 1},
                "digest": "x",
            },
        },
    ),
    ("metadata.shared.delete", {"kind": "tags", "entity_id": 1, "detach": True}),
    ("metadata.shared.ensure", {"kind": "agents", "text": "No guessing"}),
    ("metadata.shared.ensure", {"kind": "ratings", "value": 3}),
    (
        "metadata.shared.ensure",
        {"kind": "ratings", "value": True, "out_of": 5, "source": "x"},
    ),
    ("metadata.shared.ensure", {"kind": "tags", "text": "X", "parent_id": None}),
    ("metadata.shared.ensure", {"kind": "genres", "text": "X", "value": 3}),
    ("metadata.shared.suggest", {"kind": "ratings", "text": "3"}),
    ("metadata.shared.suggest", {"kind": "tags", "text": "x", "mode": "regex"}),
    ("metadata.shared.lookup", {"kind": "__class__", "fields": {}}),
    ("metadata.tree.create", {"kind": "tags", "node": {"data": {"name": "X"}}}),
    ("metadata.tree.create", {"kind": "genres", "node": {"name": "X"}}),
    (
        "metadata.tree.create",
        {
            "kind": "genres",
            "node": {"data": {"name": "X"}, "children": [{"data": {"name": "Y"}}]},
            "max_nodes": 1,
        },
    ),
    ("metadata.tree.ensure_path", {"kind": "genres", "names": [1]}),
    ("metadata.tree.move", {"kind": "genres", "entity_id": 1}),
    ("metadata.tree.move", {"kind": "genres", "entity_id": 1, "parent_id": False}),
    (
        "metadata.tree.reorder",
        {"kind": "series", "parent_id": None, "entity_ids": [1, "2"]},
    ),
    (
        "metadata.tree.delete_subtree",
        {"kind": "series", "entity_id": 1, "detach": "false"},
    ),
    ("metadata.tree.subtree", {"kind": "series", "entity_id": 1, "max_nodes": 1001}),
    ("metadata.memberships.tags.add", {"work_id": 1}),
    ("metadata.memberships.tags.add", {"work_id": 1, "tag_id": 2, "text": "Both"}),
    ("metadata.memberships.tags.set", {"work_id": 1, "entity_ids": [True]}),
    ("metadata.credits.add", {"entity_id": 1, "agent_id": False, "role": "aut"}),
    ("metadata.credits.set", {"entity_id": 1, "agent_ids": ["1"], "role": "aut"}),
    ("metadata.identifiers.link", {"entity_id": 1, "identifier_id": 0}),
    (
        "metadata.shared.for_owner",
        {"kind": "annotations", "entity_id": 1, "level": "work"},
    ),
    ("metadata.batch", {"operations": [{}] * 101}),
    ("metadata.batch", {"operations": [{"operation": "db.execute", "payload": {}}]}),
    (
        "metadata.batch",
        {
            "operations": [
                {"operation": "metadata.batch", "payload": {"operations": []}}
            ]
        },
    ),
    (
        "metadata.batch",
        {"operations": [{"operation": "metadata.search", "payload": {}}]},
    ),
    (
        "metadata.batch",
        {
            "operations": [
                {"operation": "metadata.create", "payload": {"data": {}}, "unknown": 1}
            ]
        },
    ),
    (
        "metadata.batch",
        {"operations": [{"operation": "metadata.create", "payload": {}}]},
    ),
    (
        "metadata.batch",
        {"operations": [{"operation": "metadata.create", "payload": {"data": []}}]},
    ),
    (
        "metadata.batch",
        {
            "operations": [
                {
                    "key": "first",
                    "operation": "metadata.delete",
                    "payload": {"entity_id": {"$ref": "first"}},
                }
            ]
        },
    ),
    (
        "metadata.batch",
        {
            "operations": [
                {
                    "key": "first",
                    "operation": "metadata.create",
                    "payload": {"data": {}},
                },
                {
                    "key": "first",
                    "operation": "metadata.create",
                    "payload": {"data": {}},
                },
            ]
        },
    ),
    (
        "metadata.batch",
        {
            "operations": [
                {
                    "key": "first",
                    "operation": "metadata.create",
                    "payload": {"data": {}},
                },
                {
                    "operation": "metadata.delete",
                    "payload": {"entity_id": {"$ref": "first", "path": [True]}},
                },
            ]
        },
    ),
    (
        "metadata.batch",
        {
            "operations": [],
            "expected": [
                {"reference": {"kind": "languages", "entity_id": 1}, "digest": "x"}
            ],
        },
    ),
)


@pytest.mark.parametrize("remote", (False, True))
@pytest.mark.parametrize("name,payload", INVALID)
def test_invalid_native_requests_never_call_metadata_mutators(
    core_runtime_factory, remote, name, payload
):
    with (
        core_runtime_factory() as runtime,
        public_client(runtime, remote) as (client, _),
    ):
        api = Mock()
        runtime.services._metadata = api
        query = name in {item.name for item in ROUTES if not item.write}
        with pytest.raises((CoreError, RemoteProxyError)) as failed:
            (client.query if query else client.command)(name, payload)
        assert failed.value.code == "invalid_payload"
        assert not api.mock_calls


def test_all_native_writes_refused_by_read_only_transport(core_runtime_factory):
    with core_runtime_factory() as runtime:
        api = Mock()
        runtime.services._metadata = api
        daemon = CoreHttpDaemon(runtime)
        daemon.start()
        try:
            client = RemoteCoreClient(endpoint=daemon.base_url)
            writes = [item for item in ROUTES if item.write]
            assert len(writes) >= 30
            for operation in writes:
                with pytest.raises(RemoteProxyError) as refused:
                    client.command(operation.name, {})
                assert refused.value.code == "permission_denied"
            assert not api.mock_calls
            described = client.query("api.describe", {})
            descriptions = {
                row["name"]: row
                for family in ("commands", "queries")
                for row in described[family]
            }
            for operation in ROUTES:
                assert descriptions[operation.name]["remote_capabilities"] == [
                    "write" if operation.write else "read"
                ]
        finally:
            daemon.stop()


@pytest.mark.parametrize("value", (float("nan"), float("inf"), object()))
def test_direct_metadata_inputs_have_the_same_json_domain(core_runtime_factory, value):
    with core_runtime_factory() as runtime:
        api = Mock()
        runtime.services._metadata = api
        with pytest.raises(CoreError) as failed:
            runtime.command("metadata.create", {"data": {"title": value}})
        assert failed.value.code == "invalid_payload"
        assert not api.mock_calls


def test_cyclic_local_inputs_are_bounded_before_mutation(core_runtime_factory):
    data = {}
    data["cycle"] = data
    with core_runtime_factory() as runtime:
        api = Mock()
        runtime.services._metadata = api
        with pytest.raises(CoreError) as failed:
            runtime.command("metadata.create", {"data": data})
        assert failed.value.code == "invalid_payload"
        assert not api.mock_calls


def test_metadata_family_discovery_does_not_construct_domain_services(
    core_runtime_factory,
):
    with core_runtime_factory() as runtime:
        assert runtime.services._metadata is None
        schema = runtime.query("metadata.schema")
        assert schema["reserved_record_ids"] == {"agents": [0], "series": [0]}
        assert schema["limits"]["input_values"] == 100000
        assert runtime.services._metadata is None


def test_native_input_value_budget_refuses_before_mutation(core_runtime_factory):
    with core_runtime_factory() as runtime:
        api = Mock()
        runtime.services._metadata = api
        with pytest.raises(CoreError) as failure:
            runtime.command("metadata.create", {"data": {"values": [0] * 100000}})
        assert failure.value.code == "invalid_payload"
        assert not api.mock_calls
