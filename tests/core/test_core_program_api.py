from __future__ import annotations

import base64
import uuid
import zipfile
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime, RemoteCoreClient
from LiuXin_alpha.library import Library
from LiuXin_alpha.utils.jobs import JobRequest


@dataclass
class _CapturingJobManager:
    requests: list[JobRequest] = field(default_factory=list)

    def submit(self, request: JobRequest, **_kwargs: Any) -> str:
        self.requests.append(request)
        return "captured-job-{}".format(len(self.requests))


def test_managed_storage_graph_local_and_rpc_round_trip(db) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    token = uuid.uuid4().hex
    created: list[tuple[str, int]] = []
    daemon = None
    try:
        replication = runtime.command(
            "storage.resource.create",
            {
                "resource": "replication-policy",
                "values": {
                    "name": "core-replication-{}".format(token),
                    "min_copies": 1,
                    "target_copies": 1,
                },
            },
        )
        replication_id = int(replication["id"])
        created.append(("replication-policy", replication_id))

        backup = runtime.command(
            "storage.resource.create",
            {
                "resource": "backup-policy",
                "values": {
                    "name": "core-backup-{}".format(token),
                    "min_backup_copies": 1,
                    "target_backup_copies": 1,
                },
            },
        )
        backup_id = int(backup["id"])
        created.append(("backup-policy", backup_id))

        asset = runtime.command(
            "storage.resource.create",
            {
                "resource": "asset",
                "values": {
                    "name": "core-asset-{}".format(token),
                    "extension": "epub",
                    "media_category": "ebook",
                },
            },
        )
        asset_id = int(asset["id"])
        created.append(("asset", asset_id))
        runtime.command(
            "storage.asset.policies.set",
            {
                "asset_id": asset_id,
                "replication_policy_id": replication_id,
                "backup_policy_id": backup_id,
            },
        )

        before = runtime.query(
            "storage.policy.assess",
            {"asset_id": asset_id},
        )
        assert before["replication"]["meets_minimum"] is False
        assert before["backup"]["meets_minimum"] is False

        for mode in ("active", "backup"):
            replica = runtime.command(
                "storage.resource.create",
                {
                    "resource": "replica",
                    "values": {
                        "digital_asset_id": asset_id,
                        "storage_key": "{}/{}.epub".format(token, mode),
                        "mode": mode,
                        "presence_status": "present",
                        "integrity_status": "ok",
                    },
                },
            )
            created.append(("replica", int(replica["id"])))

        assessment = runtime.query(
            "storage.policy.assess",
            {"asset_id": asset_id},
        )
        assert assessment["replication"]["meets_target"] is True
        assert assessment["backup"]["meets_target"] is True

        complete_asset = runtime.query(
            "storage.asset.get",
            {"asset_id": asset_id},
        )
        assert complete_asset["asset"]["values"]["name"] == (
            "core-asset-{}".format(token)
        )
        assert {
            replica["values"]["mode"]
            for replica in complete_asset["replicas"]
        } == {"active", "backup"}

        updated = runtime.command(
            "storage.resource.update",
            {
                "resource": "asset",
                "id": asset_id,
                "values": {"critical": False},
            },
        )
        assert updated["record"]["values"]["critical"] in {False, 0}

        daemon = CoreHttpDaemon(
            runtime,
            endpoint_namespace="core-program-storage",
        )
        daemon.start()
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        assert remote.query(
            "storage.asset.get",
            {"asset_id": asset_id},
        ) == runtime.query(
            "storage.asset.get",
            {"asset_id": asset_id},
        )
    finally:
        if daemon is not None:
            daemon.stop()
        for resource, resource_id in reversed(created):
            try:
                runtime.command(
                    "storage.resource.delete",
                    {"resource": resource, "id": resource_id},
                )
            except Exception:
                pass
        runtime.shutdown()


def test_browse_projection_covers_work_list_detail_and_acquisition(
    db,
    tmp_path,
) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    title = "Core browse {}".format(uuid.uuid4().hex)
    try:
        stack = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": title},
                "expression": {"label": "Core browse expression"},
                "manifestation": {"subtitle": "Core browse manifestation"},
                "items": [{"inventory_code": uuid.uuid4().hex}],
            },
        )
        work_id = int(stack["work_id"])
        identifier_value = str(uuid.uuid4())
        runtime.command(
            "catalog.identifiers.replace",
            {
                "level": "work",
                "entity_id": work_id,
                "identifiers": {"uuid": identifier_value},
            },
        )
        identifiers = runtime.query(
            "catalog.identifiers.list",
            {"level": "work", "entity_id": work_id},
        )
        assert identifier_value in str(identifiers["identifiers"])

        person = runtime.command(
            "catalog.agent.create-person",
            {
                "data": {
                    "name": "Core Browse Author {}".format(
                        uuid.uuid4().hex
                    )
                },
                "details": {},
            },
        )
        agent_id = int(person["agent"]["agent_id"])
        runtime.command(
            "catalog.agent.link",
            {
                "agent_id": agent_id,
                "level": "work",
                "entity_id": work_id,
                "role": "author",
                "priority": 1,
            },
        )
        agents = runtime.query(
            "catalog.agents.list",
            {
                "level": "work",
                "entity_id": work_id,
                "role": "author",
            },
        )
        assert agent_id in {
            int(agent["agent_id"]) for agent in agents["agents"]
        }
        hierarchy = runtime.query(
            "catalog.hierarchy.list",
            {
                "level": "work",
                "entity_id": work_id,
                "direction": "children",
            },
        )
        assert int(hierarchy["entities"][0]["expression_id"]) == (
            stack["expression_id"]
        )
        global_search = runtime.query(
            "search.global",
            {"text": title, "tables": ["works"]},
        )
        assert global_search["total"] == 1

        store_root = tmp_path / "managed-store"
        managed_file = store_root / "managed" / "chapter.epub"
        managed_file.parent.mkdir(parents=True)
        managed_content = b"core-managed-acquisition"
        managed_file.write_bytes(managed_content)
        saved_store = runtime.command(
            "storage.store.save",
            {
                "store": {
                    "store_name": "core-managed-{}".format(
                        uuid.uuid4().hex
                    ),
                    "store_kind": "on_disk_existing_managed_drive",
                    "store_access_protocol": "file",
                    "store_root_uri": str(store_root),
                    "store_is_read_only": 0,
                    "store_online_status": "online",
                    "store_supports_folders": 1,
                    "store_supports_hierarchical_list": 1,
                    "store_supports_random_read": 1,
                    "store_supports_random_write": 1,
                    "store_supports_delete": 1,
                    "store_supports_checksums": 1,
                    "store_supports_immutable_objects": 0,
                }
            },
        )
        store_id = int(saved_store["store"]["store_id"])
        managed_asset = runtime.command(
            "storage.resource.create",
            {
                "resource": "asset",
                "values": {
                    "name": "chapter.epub",
                    "extension": "epub",
                    "media_category": "ebook",
                },
            },
        )
        managed_asset_id = int(managed_asset["id"])
        runtime.command(
            "storage.resource.create",
            {
                "resource": "replica",
                "values": {
                    "digital_asset_id": managed_asset_id,
                    "store_id": store_id,
                    "storage_key": "managed/chapter.epub",
                    "name": "chapter.epub",
                    "mode": "active",
                    "presence_status": "present",
                },
            },
        )
        composite = runtime.command(
            "storage.resource.create",
            {
                "resource": "composite",
                "values": {
                    "name": "Core multipart edition",
                    "media_category": "ebook",
                },
            },
        )
        composite_id = int(composite["id"])
        runtime.command(
            "storage.resource.create",
            {
                "resource": "composite-item-link",
                "values": {
                    "composite_digital_asset_id": composite_id,
                    "item_id": stack["item_ids"][0],
                    "type": "primary_payload",
                },
            },
        )
        runtime.command(
            "storage.resource.create",
            {
                "resource": "composite-member-link",
                "values": {
                    "composite_digital_asset_id": composite_id,
                    "digital_asset_id": managed_asset_id,
                    "type": "member",
                    "sequence_number": 1,
                },
            },
        )

        categories = runtime.query("browse.categories")
        assert {
            category["category"]
            for category in categories["categories"]
        } == {"all", "newest", "authors", "tags", "series"}

        page = runtime.query(
            "browse.works",
            {
                "category": "all",
                "text": title,
                "limit": 10,
            },
        )
        assert [record["work_id"] for record in page["records"]] == [work_id]

        detail = runtime.query("browse.work", {"work_id": work_id})
        assert detail["work"]["title"] == title
        assert detail["item_ids"] == stack["item_ids"]

        formats = runtime.query(
            "acquisition.formats",
            {"work_id": work_id},
        )
        assert formats["work_id"] == work_id
        assert len(formats["formats"]) == 1
        assert formats["formats"][0]["extension"] == "epub"
        assert formats["formats"][0]["composite_id"] == composite_id
        replica_id = int(formats["formats"][0]["id"])
        resolution = runtime.query(
            "acquisition.resolve",
            {"kind": "replica", "id": replica_id},
        )
        assert resolution["delivery"] == "core"
        acquired = runtime.query(
            "acquisition.read",
            {"kind": "replica", "id": replica_id},
        )
        assert base64.b64decode(acquired["content"]["base64"]) == (
            managed_content
        )
    finally:
        runtime.shutdown()


def test_database_identity_and_tree_semantics_are_core_operations(db) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    created: list[int] = []
    token = uuid.uuid4().hex
    try:
        identities = runtime.query("schema.identities.list")
        assert identities["identities"]
        tag_identity = runtime.query(
            "schema.identity.get",
            {"table": "tags", "value_column": "tag"},
        )
        assert tag_identity["identity"]["table"] == "tags"
        derived = runtime.query(
            "schema.identity.derive",
            {
                "table": "tags",
                "value_column": "tag",
                "value": "  Core Identity  ",
            },
        )
        assert derived["identity_value"] == "coreidentity"

        for name in ("parent", "child"):
            result = runtime.command(
                "admin.row.create",
                {
                        "table": "series",
                        "values": {
                            "series": "{}{}".format(name, token),
                        },
                },
            )
            created.append(int(result["record"]["row_id"]))
        parent_id, child_id = created
        nested = runtime.command(
            "tree.nest",
            {
                "table": "series",
                "parent_id": parent_id,
                "child_ids": [child_id],
            },
        )
        assert nested["nested"] is True
        assert runtime.query(
            "tree.root",
            {"table": "series", "row_id": child_id},
        )["root"]["row_id"] == parent_id
        assert [
            record["row_id"]
            for record in runtime.query(
                "tree.children",
                {"table": "series", "row_id": parent_id},
            )["records"]
        ] == [child_id]
        assert [
            record["row_id"]
            for record in runtime.query(
                "tree.lineage",
                {"table": "series", "row_id": child_id},
            )["records"]
        ] == [parent_id, child_id]
    finally:
        for row_id in reversed(created):
            try:
                runtime.command(
                    "admin.row.delete",
                    {"table": "series", "row_id": row_id},
                )
            except Exception:
                pass
        runtime.shutdown()


def test_backup_workflow_persistence_is_available_directly_and_over_rpc(
    db,
) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    token = uuid.uuid4().hex
    daemon = None
    try:
        saved = runtime.command(
            "backup.workflow.save",
            {
                "workflow_spec": {
                    "workflow_name": "core-backup-{}".format(token),
                    "workflow_kind": "squashfs_pack",
                    "output_url": "/tmp/core-backup-{}.squashfs".format(
                        token
                    ),
                    "sources": [
                        {
                            "source_kind": "local_path",
                            "source_identifier": "/tmp/source-{}".format(
                                token
                            ),
                            "archive_path": "books/source.epub",
                        }
                    ],
                    "verify_after_build": False,
                }
            },
        )
        workflow_id = int(saved["workflow_id"])
        assert saved["created"] is True

        detail = runtime.query(
            "backup.workflow.get",
            {"workflow_id": workflow_id},
        )
        assert detail["spec"]["workflow_name"] == (
            "core-backup-{}".format(token)
        )
        assert detail["state"]["status"] == "draft"
        assert detail["spec"]["sources"][0]["archive_path"] == (
            "books/source.epub"
        )

        listing = runtime.query("backup.workflows.list")
        assert workflow_id in {
            int(record["workflow_id"])
            for record in listing["records"]
        }

        daemon = CoreHttpDaemon(
            runtime,
            endpoint_namespace="core-program-backup",
        )
        daemon.start()
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        assert remote.query(
            "backup.workflow.get",
            {"workflow_id": workflow_id},
        ) == detail
    finally:
        if daemon is not None:
            daemon.stop()
        runtime.shutdown()


def test_program_discovery_and_local_support_cover_every_capability_family(
    db,
) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    daemon = None
    preference_key = "core-program-{}".format(uuid.uuid4().hex)
    try:
        capabilities = runtime.query("capabilities.list")
        assert capabilities["api_version"] == "2.0"
        assert capabilities["complete_program_boundary"] is True
        assert set(capabilities["families"]) == {
            "lifecycle",
            "jobs",
            "database",
            "schema",
            "tree",
            "preferences",
            "custom_fields",
            "catalog",
            "search",
            "browse",
            "acquisition",
            "metadata",
            "storage",
            "ingest",
            "conversion",
            "backup",
            "maintenance",
        }
        assert capabilities["operations"]["conversion.start"][
            "availability"
        ] == "conditional"
        assert capabilities["operations"]["browse.works"][
            "availability"
        ] == "available"

        assert runtime.query("database.info")["type"] in {
            "SQLite",
            "SQLite_apsw",
        }
        assert runtime.query("database.summary")["table_count"] > 0
        assert runtime.query(
            "schema.column",
            {"table": "works", "column": "work_title"},
        )
        assert isinstance(
            runtime.query("custom-fields.list")["fields"],
            list,
        )

        runtime.command(
            "preferences.set",
            {
                "scope": "library",
                "key": preference_key,
                "value": {"enabled": True},
            },
        )
        preference = runtime.query(
            "preferences.get",
            {"scope": "library", "key": preference_key},
        )
        assert preference["exists"] is True
        assert preference["value"] == {"enabled": True}

        assert runtime.query("ingest.formats")["ebook_extensions"]
        assert runtime.query("metadata.file.formats")["readable"]
        assert "sources" in runtime.query("metadata.online.sources")
        assert runtime.query("conversion.formats")["output"]
        assert runtime.query("storage.sources.supported")["kinds"]
        assert "plugins" in runtime.query("maintenance.status")

        daemon = CoreHttpDaemon(
            runtime,
            endpoint_namespace="core-program-capabilities",
        )
        daemon.start()
        remote = RemoteCoreClient(endpoint=daemon.base_url)
        assert remote.query("capabilities.list") == capabilities
    finally:
        try:
            runtime.command(
                "preferences.delete",
                {"scope": "library", "key": preference_key},
            )
        except Exception:
            pass
        if daemon is not None:
            daemon.stop()
        runtime.shutdown()


def test_named_workflows_submit_only_serializable_core_job_requests(db) -> None:
    manager = _CapturingJobManager()
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
        job_manager=manager,  # type: ignore[arg-type]
        close_job_manager_on_shutdown=False,
    )
    token = uuid.uuid4().hex
    try:
        runtime.command(
            "ingest.disk.start",
            {"disk_path": "/tmp/core-ingest-{}".format(token)},
        )
        runtime.command(
            "ingest.remote-html.start",
            {
                "kind": "native_html",
                "options": {"base_url": "https://example.invalid/"},
            },
        )
        runtime.command(
            "conversion.start",
            {
                "input_path": "/tmp/input-{}.epub".format(token),
                "output_path": "/tmp/output-{}.txt".format(token),
            },
        )
        runtime.command(
            "metadata.identify.start",
            {"title": "Core metadata {}".format(token)},
        )
        runtime.command(
            "metadata.covers.start",
            {"identifiers": {"isbn": "9780000000000"}},
        )
        saved = runtime.command(
            "backup.workflow.save",
            {
                "workflow_spec": {
                    "workflow_name": "core-job-{}".format(token),
                    "workflow_kind": "squashfs_pack",
                    "output_url": "/tmp/core-job-{}.squashfs".format(
                        token
                    ),
                    "sources": [],
                }
            },
        )
        runtime.command(
            "backup.workflow.start",
            {"workflow_id": saved["workflow_id"]},
        )

        assert [
            request.function_name for request in manager.requests
        ] == [
            "run_ingest_disk_job",
            "run_ingest_remote_html_job",
            "run_conversion_job",
            "run_metadata_identify_job",
            "run_metadata_cover_job",
            "run_persisted_backup_job",
        ]
        assert all(
            request.module_name == "LiuXin_alpha.core.workflow_jobs"
            for request in manager.requests
        )
        assert manager.requests[-1].kwargs["workflow_id"] == (
            saved["workflow_id"]
        )
    finally:
        runtime.shutdown()


def test_metadata_file_read_and_write_stay_inside_core(db, tmp_path) -> None:
    book_path = tmp_path / "core-metadata.epub"
    container_xml = b"""<?xml version="1.0"?>
<container version="1.0"
 xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf"
      media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    opf = b"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:opf="http://www.idpf.org/2007/opf"
 version="2.0" unique-identifier="book-id">
  <metadata>
    <dc:identifier id="book-id">core-test</dc:identifier>
    <dc:title>Before Core</dc:title>
    <dc:creator opf:role="aut">Test Author</dc:creator>
    <dc:language>en</dc:language>
  </metadata>
  <manifest/>
  <spine/>
</package>
"""
    with zipfile.ZipFile(book_path, "w") as archive:
        archive.writestr(
            "mimetype",
            "application/epub+zip",
            compress_type=zipfile.ZIP_STORED,
        )
        archive.writestr("META-INF/container.xml", container_xml)
        archive.writestr("content.opf", opf)

    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    try:
        before = runtime.query(
            "metadata.file.inspect",
            {"path": str(book_path)},
        )
        assert before["metadata"]["title"] == "Before Core"

        written = runtime.command(
            "metadata.file.write",
            {
                "path": str(book_path),
                "metadata": {
                    "title": "After Core",
                    "authors": ["Core Author"],
                },
            },
        )
        assert written["updated"] is True
        assert written["content"] is None

        after = runtime.query(
            "metadata.file.inspect",
            {"path": str(book_path)},
        )
        assert after["metadata"]["title"] == "After Core"
        assert after["metadata"]["authors"] == ["Core Author"]
    finally:
        runtime.shutdown()


def test_schema_policy_and_custom_fields_round_trip_through_core(db) -> None:
    runtime = CoreRuntime(
        library=Library(
            database=db,
            close_database_on_close=False,
        ),
    )
    label = "core_{}".format(uuid.uuid4().hex)
    created_num = None
    try:
        policy = runtime.query(
            "schema.column",
            {"table": "works", "column": "work_title"},
        )
        updated_policy = runtime.command(
            "schema.column.update",
            {
                "table": "works",
                "column": "work_title",
                "policy": {
                    "case_sensitive": policy["case_sensitive"],
                },
            },
        )
        assert updated_policy["updated"] is True

        created = runtime.command(
            "custom-fields.create",
            {
                "name": "Core custom field",
                "label": label,
                "datatype": "text",
                "table": "works",
            },
        )
        created_num = int(created["num"])
        assert created["schema_changed"] is True
        assert created_num in {
            int(field["num"])
            for field in runtime.query("custom-fields.list")["fields"]
            if field.get("num") is not None
        }

        changed = runtime.command(
            "custom-fields.update",
            {
                "num": created_num,
                "changes": {"name": "Core renamed field"},
            },
        )
        assert changed["updated"] is True
    finally:
        if created_num is not None:
            try:
                runtime.command(
                    "custom-fields.delete",
                    {"num": created_num},
                )
            except Exception:
                pass
        runtime.shutdown()
