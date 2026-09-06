"""Contract tests for the packaged operational command families."""

from __future__ import annotations

import base64
import json

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.surfaces.cli import capabilities as capabilities_cli
from LiuXin_alpha.surfaces.cli import catalogue as catalogue_cli
from LiuXin_alpha.surfaces.cli import common as common_cli
from LiuXin_alpha.surfaces.cli import jobs as jobs_cli
from LiuXin_alpha.surfaces.cli.storage_commands import (
    administration, core_access, integrity, store_add, store_wizard,
)
from LiuXin_alpha.surfaces.cli import workflows as workflows_cli
from LiuXin_alpha.surfaces.cli.app import build_parser, main as cli_main


def _wire(content: bytes) -> dict[str, str]:
    return {
        "$type": "bytes",
        "base64": base64.b64encode(content).decode("ascii"),
    }


class _Core:
    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def query(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.queries.append((name, values))
        if name == "jobs.list":
            return {"jobs": [], "states": values.get("states", []), "total": 0}
        if name in {"jobs.get", "jobs.wait"}:
            return {"job": {"job_id": values["job_id"], "state": "succeeded"}}
        if name == "jobs.result":
            return {
                "job_id": values["job_id"],
                "execution": {"ok": True, "result": {"finished": True}},
            }
        if name == "jobs.log.read":
            return {
                "job_id": values["job_id"],
                "text": "worker output\n",
                "offset": values["offset"],
                "next_offset": values["offset"] + 14,
                "eof": True,
                "available": True,
            }
        if name == "storage.file.read":
            return {"location": {"key": "book.epub"}, "content": _wire(b"book")}
        if name == "storage.stores.list":
            return {"stores": [], "count": 0}
        if name == "storage.backends.list":
            return {"backends": [{"kind": "filesystem"}], "count": 1}
        if name == "storage.sources.supported":
            return {"kinds": [{"kind": "unmanaged_disk"}]}
        if name == "storage.resources.describe":
            return {"resources": []}
        if name == "storage.policy.plan":
            return {"asset_id": values["asset_id"], "placements": []}
        if name == "search.global":
            return {"text": values["text"], "items": [], "total": 0}
        if name == "browse.categories":
            return {"categories": []}
        if name == "acquisition.read":
            return {"resource": {"kind": values["kind"]}, "content": _wire(b"cover")}
        if name == "backup.plan":
            return {
                "packs": [],
                "count": 0,
                "destination_store": values["destination_store"],
            }
        if name.startswith("database."):
            return {"operation": name}
        if name == "maintenance.status":
            return {"service": "MaintenanceManager", "plugins": []}
        if name in {
            "health",
            "capabilities.list",
            "metadata.file.formats",
            "metadata.online.sources",
            "conversion.formats",
            "ingest.formats",
        }:
            return {"operation": name}
        raise AssertionError("Unexpected query: {}".format(name))

    def command(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.commands.append((name, values))
        if name.endswith(".start"):
            return {"job_id": "job-1", "label": values.get("label", "")}
        if name == "jobs.cancel":
            return {"job_id": values["job_id"], "cancelled": True, "state": "cancelled"}
        if name == "jobs.retry":
            return {
                "job_id": "job-2",
                "retry_of_job_id": values["job_id"],
                "job": {"state": "pending"},
            }
        if name == "storage.file.put":
            return {"asset": {"digital_asset_id": 8}, "size": 4}
        if name == "storage.file.delete":
            return {"replica_id": values["replica_id"], "deleted": True}
        if name.startswith("maintenance."):
            return {"operation": name, **values}
        if name.startswith("database."):
            return {"operation": name}
        raise AssertionError("Unexpected command: {}".format(name))


@contextmanager
def _session(core: _Core, *_args: object, **_kwargs: object):
    yield core


@pytest.fixture
def fake_core(monkeypatch: pytest.MonkeyPatch) -> _Core:
    core = _Core()
    opener = lambda *args, **kwargs: _session(core, *args, **kwargs)
    for module in (
        capabilities_cli,
        catalogue_cli,
        jobs_cli,
        administration,
        core_access,
        integrity,
        store_add,
        store_wizard,
        workflows_cli,
    ):
        monkeypatch.setattr(module, "open_cli_core", opener)
    monkeypatch.setattr(common_cli, "open_cli_core", opener)
    return core


def _connection() -> list[str]:
    return ["--database", "catalogue.sqlite"]


def test_installed_parser_exposes_complete_operational_families() -> None:
    parser = build_parser()
    action = next(
        item
        for item in parser._actions
        if getattr(item, "dest", None) == "surface"
    )
    assert {
        "core",
        "init",
        "connect",
        "disconnect",
        "config",
        "status",
        "completion",
        "jobs",
        "catalog",
        "acquire",
        "metadata",
        "storage",
        "ingest",
        "convert",
        "backup",
        "database",
        "maintenance",
        "serve",
        "plugins",
    }.issubset(action.choices)


def test_jobs_list_logs_and_cancel_use_named_job_operations(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_main(["jobs", "list", *_connection(), "--state", "failed"]) == 0
    assert json.loads(capsys.readouterr().out)["states"] == ["failed"]
    assert fake_core.queries[-1][0] == "jobs.list"

    assert cli_main(["jobs", "logs", *_connection(), "job-1"]) == 0
    assert json.loads(capsys.readouterr().out)["text"] == "worker output\n"
    assert fake_core.queries[-1][0] == "jobs.log.read"

    assert cli_main(["jobs", "cancel", *_connection(), "job-1"]) == 0
    assert json.loads(capsys.readouterr().out)["cancelled"] is True
    assert fake_core.commands[-1] == ("jobs.cancel", {"job_id": "job-1"})

    assert cli_main(
        [
            "jobs",
            "retry",
            *_connection(),
            "job-1",
            "--label",
            "again",
            "--allow-succeeded",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["retry_of_job_id"] == "job-1"
    assert fake_core.commands[-1] == (
        "jobs.retry",
        {
            "job_id": "job-1",
            "allow_succeeded": True,
            "label": "again",
        },
    )


def test_backup_plan_names_configured_source_and_destination_stores(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_main(
        [
            "backup",
            "plan",
            *_connection(),
            "source-store",
            "sealed-store",
            "--target-pack-mib",
            "8",
            "--output-key-prefix",
            "monthly/naïve #1",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0
    assert fake_core.queries[-1] == (
        "backup.plan",
        {
            "source_store": "source-store",
            "destination_store": "sealed-store",
            "target_pack_size_bytes": 8 * 1024 * 1024,
            "output_key_prefix": "monthly/naïve #1",
        },
    )


def test_storage_transfers_cli_host_bytes_and_rich_hints(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "tortured-\udcff.epub"
    source.write_bytes(b"book")
    hints = tmp_path / "hints.json"
    hints.write_text('{"derivation": {"tool": "converter"}}', encoding="utf-8")

    assert (
        cli_main(
            [
                "storage",
                "files",
                "put",
                *_connection(),
                str(source),
                "--metadata-file",
                str(hints),
                "--store",
                "primary",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["size"] == 4
    name, payload = fake_core.commands[-1]
    assert name == "storage.file.put"
    assert base64.b64decode(payload["content_base64"], validate=True) == b"book"
    assert payload["metadata"]["derivation"]["tool"] == "converter"
    assert payload["original_name"].endswith(".epub")

    target = tmp_path / "download.epub"
    assert (
        cli_main(
            ["storage", "files", "get", *_connection(), "8", str(target)]
        )
        == 0
    )
    assert target.read_bytes() == b"book"
    assert "book.epub" in capsys.readouterr().err


def test_catalogue_search_and_acquisition_download_are_core_backed(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        cli_main(
            ["catalog", "search", *_connection(), "snowman \u2603", "--table", "works"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["text"] == "snowman \u2603"
    assert fake_core.queries[-1] == (
        "search.global",
        {"text": "snowman \u2603", "limit": 100, "offset": 0, "tables": ["works"]},
    )

    output = tmp_path / "cover.jpg"
    assert (
        cli_main(
            ["acquire", "get", *_connection(), "image", "2", str(output)]
        )
        == 0
    )
    assert output.read_bytes() == b"cover"
    assert fake_core.queries[-1][0] == "acquisition.read"
    assert "size" in capsys.readouterr().err


def test_managed_workflows_preserve_core_host_paths_and_detach(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        cli_main(
            [
                "ingest",
                "disk",
                *_connection(),
                "/srv/incoming",
                "--source-label",
                "drive-7",
                "--detach",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["job_id"] == "job-1"
    name, payload = fake_core.commands[-1]
    assert name == "ingest.disk.start"
    assert payload["disk_path"] == "/srv/incoming"
    assert payload["source_label"] == "drive-7"

    assert (
        cli_main(
            [
                "convert",
                "run",
                *_connection(),
                "/srv/a.epub",
                "/srv/a.mobi",
                "--detach",
            ]
        )
        == 0
    )
    _ = capsys.readouterr()
    assert fake_core.commands[-1][0] == "conversion.start"
    assert fake_core.commands[-1][1]["output_path"] == "/srv/a.mobi"


def test_maintenance_mutations_preview_until_explicitly_confirmed(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = len(fake_core.commands)
    argv = ["maintenance", "merge", *_connection(), "tags", "4", "9"]
    assert cli_main(argv) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["preview"] is True
    assert len(fake_core.commands) == before

    assert cli_main([*argv, "--yes"]) == 0
    assert json.loads(capsys.readouterr().out)["operation"] == "maintenance.merge"
    assert fake_core.commands[-1] == (
        "maintenance.merge",
        {"table": "tags", "retained_id": 4, "merged_id": 9},
    )


def test_plugins_probe_public_capabilities_without_generic_dispatch(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_main(["plugins", "inspect", *_connection()]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True
    assert "storage_backends" in result["sections"]
    assert "storage_sources" in result["sections"]
    assert {name for name, _payload in fake_core.queries} >= {
        "capabilities.list",
        "storage.backends.list",
        "storage.sources.supported",
        "conversion.formats",
        "ingest.formats",
    }


def test_core_health_uses_named_operation_and_core_serve_refuses_remote_bind(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_main(["core", "health", *_connection()]) == 0
    assert json.loads(capsys.readouterr().out)["operation"] == "health"
    assert fake_core.queries[-1] == ("health", {})

    rc = cli_main(
        [
            "core",
            "serve",
            "--database",
            "catalogue.sqlite",
            "--host",
            "0.0.0.0",
            "--stop-after",
            "0",
        ]
    )
    assert rc == 2
    assert "no TLS or authentication" in capsys.readouterr().err


def test_serve_refuses_unauthenticated_remote_bind_before_import(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_main(
        [
            "serve",
            "web-write",
            *_connection(),
            "--host",
            "0.0.0.0",
        ]
    )
    assert rc == 2
    assert "no built-in authentication or TLS" in capsys.readouterr().err
