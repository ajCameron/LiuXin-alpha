from __future__ import annotations

import base64
import datetime
import threading
import uuid

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.core import (
    CoreClientAPI,
    CoreCommand,
    CoreHttpDaemon,
    CoreQuery,
    CoreRuntime,
    LocalCoreClient,
    RemoteCoreClient,
    core_client,
    create_core,
)
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.core.services import CoreServiceReconciliationError
from LiuXin_alpha.library import Library


class _DriverWrapper:
    @staticmethod
    def get_id_column(table: str) -> str:
        return {
            "works": "work_id",
            "tags": "tag_id",
        }.get(str(table), "{}_id".format(str(table).rstrip("s")))

    @staticmethod
    def is_view(table: str) -> bool:
        del table
        return False

    @staticmethod
    def get_interlinked_tables(table: str) -> list[str]:
        if table == "works":
            return ["works", "tags"]
        if table == "tags":
            return ["works", "tags"]
        return [table]


@dataclass
class _Database:
    driver_wrapper: _DriverWrapper = field(default_factory=_DriverWrapper)


class _ReadSource:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {
            "works": [
                {
                    "work_id": 1,
                    "work_title": "Álpha",
                    "work_year": 2020,
                    "work_blob": b"\x00\x01",
                    "work_date": datetime.date(2020, 1, 2),
                },
                {
                    "work_id": 2,
                    "work_title": "Beta",
                    "work_year": 2024,
                    "work_blob": b"\x02",
                    "work_date": datetime.date(2024, 2, 3),
                },
            ],
            "tags": [
                {"tag_id": 10, "tag": "science"},
                {"tag_id": 11, "tag": "history"},
            ],
        }
        self.links = {1: [10], 2: [11]}

    def get_tables(self, force_refresh: bool = False):
        del force_refresh
        return tuple(self.rows)

    def get_column_headings(self, table: str):
        rows = self.rows.get(table, [])
        return set(rows[0]) if rows else set()

    def get_row_from_id(self, table: str, row_id: int):
        id_column = _DriverWrapper.get_id_column(table)
        return next(
            (
                dict(row)
                for row in self.rows.get(table, [])
                if int(row[id_column]) == int(row_id)
            ),
            None,
        )

    def get_all_rows(self, table: str, iterator_return: bool = False):
        del iterator_return
        return tuple(dict(row) for row in self.rows.get(table, []))

    def get_interlinked_rows(
        self,
        target_row,
        secondary_table: str,
        type_filter: str | None = None,
    ):
        del type_filter
        if "work_id" in target_row and secondary_table == "tags":
            wanted = set(self.links.get(int(target_row["work_id"]), ()))
            return tuple(
                dict(row)
                for row in self.rows["tags"]
                if int(row["tag_id"]) in wanted
            )
        if "tag_id" in target_row and secondary_table == "works":
            tag_id = int(target_row["tag_id"])
            return tuple(
                dict(row)
                for row in self.rows["works"]
                if tag_id in self.links.get(int(row["work_id"]), ())
            )
        return ()

    def get_interlink_rows(self, primary_row, secondary_table: str):
        if "work_id" not in primary_row or secondary_table != "tags":
            return ()
        return tuple(
            {
                "work_tag_link_id": index,
                "work_id": int(primary_row["work_id"]),
                "tag_id": tag_id,
            }
            for index, tag_id in enumerate(
                self.links.get(int(primary_row["work_id"]), ()),
                start=1,
            )
        )


class _Repository:
    def __init__(self) -> None:
        self.rows = {
            1: {
                "work_id": 1,
                "work_title": "Existing",
            }
        }
        self.next_id = 2

    def get(self, entity_id: int):
        row = self.rows.get(int(entity_id))
        return None if row is None else dict(row)

    def require(self, entity_id: int):
        row = self.get(entity_id)
        if row is None:
            raise KeyError(entity_id)
        return row

    def list(self, *, limit: int = 100, offset: int = 0):
        return tuple(
            dict(row)
            for _row_id, row in sorted(self.rows.items())
        )[offset : offset + limit]

    def create(self, data):
        entity_id = self.next_id
        self.next_id += 1
        self.rows[entity_id] = {
            "work_id": entity_id,
            "work_title": data["title"],
        }
        return entity_id

    def update(self, entity_id: int, data):
        self.rows[int(entity_id)].update(data)

    def delete(self, entity_id: int):
        del self.rows[int(entity_id)]


@dataclass
class _TrackingJobManager:
    shutdown_calls: list[dict[str, bool]] = field(default_factory=list)

    def shutdown(
        self,
        *,
        wait: bool = True,
        cancel_pending: bool = False,
    ) -> None:
        self.shutdown_calls.append(
            {
                "wait": bool(wait),
                "cancel_pending": bool(cancel_pending),
            }
        )


@dataclass
class _Library:
    database: _Database
    files: dict[str, "_Location"] = field(default_factory=dict)

    def add_file(
        self,
        file_bytes: bytes,
        metadata=None,
        *,
        preferred_store: str | None = None,
    ):
        del metadata
        key = "{}/file-{}".format(
            preferred_store or "default",
            len(self.files) + 1,
        )
        location = _Location(key=key, content=bytes(file_bytes))
        self.files[location.file_url] = location
        return location

    def retrieve_file(
        self,
        file_url: str | None = None,
        metadata=None,
        *,
        preferred_store: str | None = None,
    ):
        del metadata, preferred_store
        return self.files[str(file_url)]

    def delete_file(
        self,
        file_url: str | None = None,
        metadata=None,
        file_container=None,
    ) -> bool:
        del metadata, file_container
        return self.files.pop(str(file_url), None) is not None

    def iter_files(self):
        return iter(tuple(self.files.values()))


@dataclass
class _Location:
    key: str
    content: bytes

    @property
    def store(self):
        return SimpleNamespace(name="fake-store")

    @property
    def file_url(self) -> str:
        return "fake://{}".format(self.key)

    @property
    def name(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def suffix(self) -> str:
        return ""

    @property
    def cached_size(self) -> int:
        return len(self.content)

    @property
    def cached_hash(self):
        return None

    def as_store_key(self) -> str:
        return self.key

    def exists(self) -> bool:
        return True

    def read_bytes(self) -> bytes:
        return self.content


def _fake_runtime() -> CoreRuntime:
    database = _Database()
    repository = _Repository()
    catalog = SimpleNamespace(
        db=database,
        repositories=SimpleNamespace(works=repository),
    )
    return CoreRuntime(
        library=_Library(database=database),
        catalog=catalog,
        read_source=_ReadSource(),
        core_uuid="core-application-test",
        core_version="core-test",
    )


def test_core_describes_complete_named_application_api() -> None:
    runtime = _fake_runtime()

    described = runtime.describe_api(include_targets=False)
    command_entries = {
        entry["name"]: entry
        for entry in described["commands"]
    }
    query_entries = {
        entry["name"]: entry
        for entry in described["queries"]
    }

    assert described["api_version"] == "2.0"
    assert described["services"]["catalog"] == "SimpleNamespace"
    assert {
        "invoke",
        "shutdown",
        "sync.store.start",
        "sync.store.cancel",
        "jobs.cancel",
        "metadata.write",
        "metadata.tags.replace",
        "metadata.labels.replace",
        "metadata.genre.replace",
        "metadata.series.replace",
        "metadata.identifiers.replace",
        "catalog.entity.create",
        "catalog.entity.update",
        "catalog.entity.delete",
        "catalog.entity.match-or-create",
        "catalog.agent.create-person",
        "catalog.agent.create-organisation",
        "catalog.wemi.create",
        "catalog.wemi.link",
        "catalog.wemi.unlink",
        "catalog.metadata.attach",
        "catalog.metadata.replace",
        "catalog.metadata.merge",
        "catalog.field.write",
        "catalog.field.write-one",
        "admin.row.create",
        "admin.row.update",
        "admin.row.delete",
        "admin.relation.link",
        "admin.relation.unlink",
        "storage.store.save",
        "storage.refresh",
        "storage.file.put",
        "storage.file.delete",
        "cache.reload",
        "read-source.refresh",
    } <= set(command_entries)
    assert {
        "invoke",
        "health",
        "api.describe",
        "jobs.list",
        "jobs.get",
        "jobs.wait",
        "schema.tables",
        "schema.table",
        "rows.get",
        "rows.query",
        "relations.list",
        "admin.row.delete-impact",
        "catalog.entity.get",
        "catalog.entity.list",
        "catalog.bundle.get",
        "catalog.graph.get",
        "catalog.item.summary",
        "catalog.match",
        "catalog.agent.resolve",
        "catalog.annotations.list",
        "metadata.get",
        "metadata.opf.export",
        "cache.status",
        "storage.stores.list",
        "storage.files.list",
        "storage.file.locate",
        "storage.file.read",
    } <= set(query_entries)
    capabilities = runtime.query("capabilities.list")
    assert capabilities["complete_program_boundary"] is True
    declared_program_operations = {
        operation
        for family in capabilities["families"].values()
        for operation in family["operations"]
    }
    assert declared_program_operations <= (
        set(command_entries) | set(query_entries)
    )
    assert all(
        detail["call_modes"] == ["direct", "rpc"]
        for detail in capabilities["operations"].values()
    )
    assert command_entries["invoke"]["transport_stable"] is False
    assert query_entries["invoke"]["transport_stable"] is False
    assert all(
        entry["transport_stable"] is True
        for name, entry in command_entries.items()
        if name != "invoke"
    )
    assert all(
        entry["transport_stable"] is True
        for name, entry in query_entries.items()
        if name != "invoke"
    )


def test_core_factory_and_client_selector_preserve_one_contract() -> None:
    database = _Database()
    runtime = create_core(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        core_uuid="factory-core",
    )

    assert core_client(runtime=runtime) is runtime
    assert isinstance(runtime, CoreClientAPI)
    with pytest.raises(ValueError):
        core_client()
    with pytest.raises(ValueError):
        core_client(
            runtime=runtime,
            endpoint="http://example.test",
        )


def test_core_shutdown_respects_injected_job_manager_ownership() -> None:
    database = _Database()
    supplied = _TrackingJobManager()
    runtime = CoreRuntime(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        job_manager=supplied,
    )
    runtime.shutdown()
    assert supplied.shutdown_calls == []

    database = _Database()
    owned = _TrackingJobManager()
    runtime = CoreRuntime(
        library=_Library(database=database),
        catalog=SimpleNamespace(
            db=database,
            repositories=SimpleNamespace(works=_Repository()),
        ),
        read_source=_ReadSource(),
        job_manager=owned,
        close_job_manager_on_shutdown=True,
    )
    runtime.shutdown()
    assert owned.shutdown_calls == [
        {
            "wait": False,
            "cancel_pending": True,
        }
    ]


def test_core_rows_query_and_relations_are_transport_shaped() -> None:
    runtime = _fake_runtime()

    page = runtime.query(
        "rows.query",
        {
            "table": "works",
            "predicates": [
                {
                    "field": "work_year",
                    "operator": "gte",
                    "value": 2020,
                }
            ],
            "text": "a",
            "text_fields": ["work_title"],
            "sort": [
                {
                    "field": "work_year",
                    "ascending": False,
                }
            ],
            "projection": [
                "work_title",
                "work_blob",
                "work_date",
            ],
            "offset": 0,
            "limit": 1,
        },
    )

    assert page["source"] == "database"
    assert page["total_count"] == 2
    assert page["records"] == [
        {
            "table": "works",
            "row_id": 2,
            "values": {
                "work_title": "Beta",
                "work_blob": {
                    "$type": "bytes",
                    "base64": "Ag==",
                },
                "work_date": {
                    "$type": "date",
                    "iso": "2024-02-03",
                },
                "work_id": 2,
            },
        }
    ]

    related = runtime.query(
        "relations.list",
        {
            "table": "works",
            "row_id": 1,
            "related_table": "tags",
            "include_link_rows": True,
        },
    )
    assert related["records"][0]["values"]["tag"] == "science"
    assert related["link_records"][0]["values"]["tag_id"] == 10


def test_core_catalog_repository_commands_return_receipts() -> None:
    runtime = _fake_runtime()
    events = []
    runtime.subscribe(events.append)

    created = runtime.command(
        "catalog.entity.create",
        {
            "repository": "works",
            "data": {"title": "Created through Core"},
        },
        command_id="create-work-2",
    )

    assert created == {
        "repository": "works",
        "entity_id": 2,
        "entity": {
            "work_id": 2,
            "work_title": "Created through Core",
        },
        "cache": {
            "configured": False,
            "reconciled": False,
        },
    }
    fetched = runtime.query(
        "catalog.entity.get",
        {
            "repository": "works",
            "entity_id": 2,
        },
    )
    assert fetched["entity"]["work_title"] == "Created through Core"
    write_events = [
        event
        for event in events
        if event.event_type == "write.completed"
    ]
    assert write_events[-1].payload["name"] == "catalog.entity.create"
    assert write_events[-1].payload["command_id"] == "create-work-2"


def test_core_storage_file_api_uses_explicit_wire_bytes() -> None:
    runtime = _fake_runtime()

    stored = runtime.command(
        "storage.file.put",
        {
            "content_base64": "aGVsbG8AY29yZQ==",
            "preferred_store": "archive",
        },
    )
    file_url = stored["location"]["file_url"]

    assert stored["size"] == 10
    assert stored["location"]["store_key"] == "archive/file-1"
    listed = runtime.query(
        "storage.files.list",
        {
            "limit": 10,
            "offset": 0,
        },
    )
    assert listed["total_count"] == 1
    assert listed["files"][0]["file_url"] == file_url
    read = runtime.query(
        "storage.file.read",
        {
            "file_url": file_url,
        },
    )
    assert read["content"] == {
        "$type": "bytes",
        "base64": "aGVsbG8AY29yZQ==",
    }
    assert runtime.command(
        "storage.file.delete",
        {
            "file_url": file_url,
        },
    )["deleted"] is True


def test_local_and_rpc_core_clients_have_envelope_and_result_parity() -> None:
    runtime = _fake_runtime()
    local = LocalCoreClient(runtime)
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-v1")
    daemon.start()
    remote = RemoteCoreClient(endpoint=daemon.base_url)
    try:
        assert isinstance(runtime, CoreClientAPI)
        assert isinstance(local, CoreClientAPI)
        assert isinstance(remote, CoreClientAPI)
        assert remote.core_uuid == local.core_uuid == runtime.core_uuid
        assert remote.core_version == local.core_version == "core-test"
        assert remote.api_version == local.api_version == "2.0"

        envelope = CoreQuery(
            name="rows.get",
            payload={
                "table": "works",
                "row_id": 1,
            },
            query_id="query-envelope-id",
            correlation_id="correlation-id",
        )
        local_result = local.execute_query(envelope)
        remote_result = remote.execute_query(envelope)

        assert remote_result == local_result
        assert remote_result.query_id == "query-envelope-id"
        assert remote_result.correlation_id == "correlation-id"
        assert (
            remote.describe_api(include_targets=False)
            == local.describe_api(include_targets=False)
        )
    finally:
        daemon.stop()


def test_rpc_core_client_subscribes_to_typed_events() -> None:
    runtime = _fake_runtime()
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-events")
    daemon.start()
    remote = RemoteCoreClient(endpoint=daemon.base_url)
    received = []
    finished = threading.Event()

    def _capture(event) -> None:
        received.append(event)
        if event.event_type == "command.finished":
            finished.set()

    unsubscribe = remote.subscribe(_capture)
    try:
        remote.command(
            "catalog.entity.create",
            {
                "repository": "works",
                "data": {"title": "Event through RPC"},
            },
        )
        assert finished.wait(timeout=3.0) is True
        assert any(
            event.event_type == "write.completed"
            and event.payload["name"] == "catalog.entity.create"
            for event in received
        )
    finally:
        unsubscribe()
        daemon.stop()


def test_reconciliation_receipt_survives_direct_and_rpc_errors() -> None:
    runtime = _fake_runtime()

    def _fail_reconciliation(runtime, command):
        del runtime, command
        raise CoreServiceReconciliationError(
            "cache refresh failed",
            receipt={"entity_id": 7, "committed": True},
        )

    runtime.register_command_handler(
        "test.reconciliation-failure",
        _fail_reconciliation,
    )
    with pytest.raises(CoreHandlerError) as direct_error:
        runtime.command("test.reconciliation-failure")
    assert direct_error.value.code == "cache_reconciliation_failed"
    assert direct_error.value.details == {
        "receipt": {
            "entity_id": 7,
            "committed": True,
        },
        "canonical_write_committed": True,
    }

    daemon = CoreHttpDaemon(runtime, endpoint_namespace="core-errors")
    daemon.start()
    try:
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        with pytest.raises(RemoteProxyError) as remote_error:
            remote.command("test.reconciliation-failure")
        assert remote_error.value.code == "cache_reconciliation_failed"
        assert remote_error.value.details == direct_error.value.details
    finally:
        daemon.stop()


def test_stable_handler_rejects_non_wire_results_as_structured_errors() -> None:
    runtime = _fake_runtime()
    events = []
    runtime.subscribe(events.append)
    runtime.register_query_handler(
        "test.non-wire-query",
        lambda _runtime, _query: object(),
    )
    runtime.register_command_handler(
        "test.non-wire-command",
        lambda _runtime, _command: object(),
    )

    with pytest.raises(CoreHandlerError) as query_error:
        runtime.query("test.non-wire-query")
    assert query_error.value.code == "handler_error"
    assert query_error.value.details == {
        "exception_type": "CoreWireError",
    }

    with pytest.raises(CoreHandlerError) as command_error:
        runtime.command("test.non-wire-command")
    assert command_error.value.code == "handler_error"
    assert command_error.value.details == query_error.value.details
    assert any(
        event.event_type == "command.failed"
        and event.payload["name"] == "test.non-wire-command"
        for event in events
    )


def test_core_catalog_and_cache_api_round_trip_real_database(db) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
        cache_type="schema_backed",
        close_cache_on_shutdown=True,
    )
    title = "Core API {}".format(uuid.uuid4())
    try:
        created = runtime.command(
            "catalog.entity.create",
            {
                "repository": "works",
                "data": {
                    "title": title,
                    "canonical_title": title,
                },
            },
        )
        work_id = int(created["entity_id"])
        assert created["cache"]["configured"] is True
        assert created["cache"]["reconciled"] is True

        cached = runtime.query(
            "rows.query",
            {
                "table": "works",
                "predicates": [
                    {
                        "field": "work_title",
                        "operator": "eq",
                        "value": title,
                    }
                ],
            },
        )
        assert cached["source"] == "cache"
        assert cached["complete"] is True
        assert [record["row_id"] for record in cached["records"]] == [
            work_id
        ]

        updated = runtime.command(
            "catalog.entity.update",
            {
                "repository": "works",
                "entity_id": work_id,
                "data": {
                    "canonical_title": "{} updated".format(title),
                },
            },
        )
        assert (
            updated["entity"]["work_canonical_title"]
            == "{} updated".format(title)
        )
        assert runtime.query(
            "rows.get",
            {
                "table": "works",
                "row_id": work_id,
            },
        )["record"]["values"]["work_canonical_title"] == "{} updated".format(
            title
        )

        stack_title = "{} stack".format(title)
        stack = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": stack_title},
                "expression": {"label": "Core API expression"},
                "manifestation": {"subtitle": "Core API manifestation"},
                "items": [{"inventory_code": "core-api-item"}],
                "origin": "core-api-test",
            },
        )
        assert len(stack["item_ids"]) == 1
        bundle = runtime.query(
            "catalog.bundle.get",
            {
                "level": "item",
                "entity_id": stack["item_ids"][0],
            },
        )
        assert bundle["work"]["work_id"] == stack["work_id"]
        assert bundle["work"]["work_title"] == stack_title

        metadata = runtime.query(
            "metadata.get",
            {
                "item_id": stack["item_ids"][0],
            },
        )
        assert (
            metadata["database_ids"]["item_id"]
            == stack["item_ids"][0]
        )
        opf = runtime.query(
            "metadata.opf.export",
            {
                "item_id": stack["item_ids"][0],
            },
        )
        assert opf["content"]["$type"] == "bytes"
        assert b"<package" in base64.b64decode(
            opf["content"]["base64"]
        )

        metadata_write = runtime.command(
            "metadata.tags.replace",
            {
                "item_id": stack["item_ids"][0],
                "tags": ["Core API Tag"],
                "kind": "liuxin",
            },
        )
        assert metadata_write["changed"] is True
        assert metadata_write["cache"]["configured"] is True
        assert metadata_write["cache"]["reconciled"] is True
        assert "Core API Tag" in str(
            runtime.query(
                "metadata.get",
                {
                    "item_id": stack["item_ids"][0],
                },
            )
        )

        matched = runtime.command(
            "catalog.entity.match-or-create",
            {
                "repository": "works",
                "candidate": {"title": stack_title},
            },
        )
        assert matched["entity_id"] == stack["work_id"]

        person = runtime.command(
            "catalog.agent.create-person",
            {
                "data": {
                    "name": "Core Person {}".format(uuid.uuid4()),
                },
                "details": {},
            },
        )
        assert person["kind"] == "person"
        assert person["agent"]["agent_type"] == "person"

        field_tag = "Core Field Tag {}".format(uuid.uuid4())
        field_write = runtime.command(
            "catalog.field.write-one",
            {
                "src_table": "works",
                "dst_column": "tag",
                "src_id": stack["work_id"],
                "dst_value": field_tag,
            },
        )
        assert field_write["cache"] == {
            "configured": True,
            "reconciled": True,
        }
        related_tags = runtime.query(
            "relations.list",
            {
                "table": "works",
                "row_id": stack["work_id"],
                "related_table": "tags",
            },
        )
        assert related_tags["source"] == "cache"
        assert [
            record["values"]["tag"]
            for record in related_tags["records"]
        ] == [field_tag]

        deleted = runtime.command(
            "catalog.entity.delete",
            {
                "repository": "works",
                "entity_id": work_id,
            },
        )
        assert deleted["deleted"]["work_id"] == work_id
        assert runtime.query(
            "rows.get",
            {
                "table": "works",
                "row_id": work_id,
            },
        )["record"] is None
    finally:
        runtime.shutdown()


def test_catalog_conveniences_have_direct_and_rpc_parity(db) -> None:
    """New Catalog conveniences retain one transport-stable Core contract."""

    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    daemon = None
    title = "Core convenience {}".format(uuid.uuid4().hex)
    try:
        stack = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": title},
                "expression": {"label": "Primary expression"},
                "manifestation": {"subtitle": "Primary manifestation"},
                "items": [{"inventory_code": uuid.uuid4().hex}],
            },
        )
        expression = runtime.command(
            "catalog.entity.create",
            {
                "repository": "expressions",
                "data": {"label": "RPC expression"},
            },
        )
        annotation = runtime.command(
            "catalog.entity.create",
            {
                "repository": "annotations",
                "data": {
                    "item_id": stack["item_ids"][0],
                    "user_id": 23,
                    "kind": "highlight",
                    "anchor_type": "offset",
                    "anchor_start": "4",
                    "source": "core-convenience-test",
                },
            },
        )
        runtime.command(
            "catalog.metadata.replace",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "data": {
                    "identifiers": {
                        "doi": "10.1000/core-convenience",
                    },
                    "notes": ["Written through direct Core"],
                },
            },
        )

        daemon = CoreHttpDaemon(
            runtime,
            endpoint_namespace="catalog-conveniences",
        )
        daemon.start()
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        linked = remote.command(
            "catalog.wemi.link",
            {
                "parent_level": "work",
                "parent_id": stack["work_id"],
                "child_level": "expression",
                "child_id": expression["entity_id"],
                "primary": True,
                "origin": "rpc",
            },
        )
        assert linked["child_id"] == expression["entity_id"]
        assert linked["cache"] == {
            "configured": False,
            "reconciled": False,
        }

        graph_payload = {
            "work_id": stack["work_id"],
            "max_expressions": 10,
            "max_manifestations": 10,
            "max_items": 10,
        }
        assert remote.query("catalog.graph.get", graph_payload) == (
            runtime.query("catalog.graph.get", graph_payload)
        )
        assert remote.query(
            "catalog.hierarchy.list",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "direction": "children",
            },
        ) == runtime.query(
            "catalog.hierarchy.list",
            {
                "level": "work",
                "entity_id": stack["work_id"],
                "direction": "children",
            },
        )
        primary_payload = {
            "level": "work",
            "entity_id": stack["work_id"],
        }
        assert remote.query(
            "catalog.identifiers.primary-values",
            primary_payload,
        ) == runtime.query(
            "catalog.identifiers.primary-values",
            primary_payload,
        )
        annotations = remote.query(
            "catalog.annotations.list",
            {
                "item_id": stack["item_ids"][0],
                "user_id": 23,
                "kind": "highlight",
            },
        )
        assert annotations["count"] == 1
        assert annotations["annotations"][0]["annotation_id"] == (
            annotation["entity_id"]
        )

        unlinked = remote.command(
            "catalog.wemi.unlink",
            {
                "parent_level": "work",
                "parent_id": stack["work_id"],
                "child_level": "expression",
                "child_id": expression["entity_id"],
            },
        )
        assert unlinked["unlinked"] is True
    finally:
        if daemon is not None:
            daemon.stop()
        runtime.shutdown()
