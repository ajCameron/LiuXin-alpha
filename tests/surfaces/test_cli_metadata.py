"""Operational contract tests for ``liuxin metadata``."""

from __future__ import annotations

import base64
import json
import os
import zipfile

from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.surfaces.cli import metadata as metadata_cli
from LiuXin_alpha.surfaces.cli.app import main as cli_main


def _wire(content: bytes) -> dict[str, str]:
    return {
        "$type": "bytes",
        "base64": base64.b64encode(content).decode("ascii"),
    }


class _Core:
    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.commands: list[tuple[str, dict[str, Any]]] = []
        self.item_ids = [3, 7]
        self.updated_file = b"updated-book-bytes"

    def query(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.queries.append((name, values))
        if name == "metadata.get":
            item_id = int(values["item_id"])
            return {
                "database_ids": {"item_id": item_id},
                "liuxin": {"title": "Book {}".format(item_id)},
                "tortured": "snowman \u2603 / lone-surrogate \udcff",
            }
        if name == "rows.query":
            offset = int(values["offset"])
            limit = int(values["limit"])
            selected = self.item_ids[offset : offset + limit]
            return {
                "records": [
                    {
                        "table": "items",
                        "row_id": item_id,
                        "values": {"item_id": item_id},
                    }
                    for item_id in selected
                ],
                "total_count": len(self.item_ids),
            }
        if name == "metadata.opf.export":
            return {"item_id": values["item_id"], "content": _wire(b"<package/>")}
        if name == "metadata.file.formats":
            return {"readable": ["epub", "mobi"], "writable": ["epub"]}
        if name == "metadata.file.inspect":
            content = base64.b64decode(values["base64"], validate=True)
            return {
                "file_type": values["file_type"],
                "metadata": {
                    "title": "Updated" if content == self.updated_file else "Original",
                    "authors": ["CLI Author"],
                },
            }
        if name == "metadata.online.sources":
            return {"sources": [{"name": "Example", "capabilities": ["identify"]}]}
        if name == "jobs.get":
            return {"job": {"job_id": values["job_id"], "state": "succeeded"}}
        if name == "jobs.result":
            if self.commands and self.commands[-1][0] == "metadata.covers.start":
                return {
                    "execution": {
                        "ok": True,
                        "result": {
                            "found": True,
                            "cover": {
                                "source": "Example",
                                "width": 600,
                                "height": 900,
                                "format": "jpeg",
                                "content": _wire(b"cover-bytes"),
                            },
                        },
                    }
                }
            return {
                "execution": {
                    "ok": True,
                    "result": {
                        "count": 1,
                        "results": [{"metadata": {"title": "Found"}}],
                    },
                }
            }
        raise AssertionError("Unexpected query: {}".format(name))

    def command(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        values = dict(payload or {})
        self.commands.append((name, values))
        if name == "metadata.write":
            return {
                "item_id": values["item_id"],
                "fields": values["fields"],
                "replace": values["replace"],
                "changed": True,
            }
        if name == "metadata.file.write":
            assert "path" not in values
            return {
                "updated": True,
                "file_type": values["file_type"],
                "content": _wire(self.updated_file),
                "size": len(self.updated_file),
            }
        if name in {"metadata.identify.start", "metadata.covers.start"}:
            return {"job_id": "job-1", "state": "pending"}
        raise AssertionError("Unexpected command: {}".format(name))


class _Session:
    def __init__(self, core: _Core) -> None:
        self.client = core

    def __enter__(self) -> "_Session":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


@pytest.fixture
def fake_core(monkeypatch: pytest.MonkeyPatch) -> _Core:
    core = _Core()
    monkeypatch.setattr(
        metadata_cli,
        "open_surface_core_from_args",
        lambda _args, **_kwargs: _Session(core),
    )
    return core


def _connection() -> list[str]:
    return ["--database", "catalogue.sqlite"]


def test_show_reads_one_record_as_interoperable_json(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_main(["metadata", "show", *_connection(), "7", "--no-related"])

    assert rc == 0
    raw = capsys.readouterr().out
    assert "\\udcff" in raw
    payload = json.loads(raw)
    assert payload["database_ids"]["item_id"] == 7
    assert fake_core.queries[-1] == (
        "metadata.get",
        {"item_id": 7, "include_related": False, "include_legacy": True},
    )


def test_show_accepts_remote_core_endpoint(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_main(
        [
            "metadata",
            "get",
            "--core-endpoint",
            "http://127.0.0.1:8765",
            "3",
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["database_ids"]["item_id"] == 3


def test_dump_all_pages_ids_and_atomically_writes_deterministic_document(
    fake_core: _Core,
    tmp_path: Path,
) -> None:
    output = tmp_path / "metadata.json"

    rc = cli_main(
        [
            "metadata",
            "dump-json",
            *_connection(),
            "--all",
            "--page-size",
            "1",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["format"] == "liuxin.metadata.dump"
    assert payload["version"] == 1
    assert payload["item_count"] == 2
    assert [item["database_ids"]["item_id"] for item in payload["items"]] == [3, 7]
    assert len([name for name, _payload in fake_core.queries if name == "rows.query"]) == 2
    assert not tuple(tmp_path.glob(".metadata.json.*.tmp"))


def test_dump_supports_json_lines_and_utf8_bom_item_id_file(
    fake_core: _Core,
    tmp_path: Path,
) -> None:
    ids = tmp_path / "ids.txt"
    ids.write_text("\ufeff# selected\n7\n3\n7\n", encoding="utf-8")
    output = tmp_path / "metadata.jsonl"

    rc = cli_main(
        [
            "metadata",
            "dump",
            *_connection(),
            "--item-ids-file",
            str(ids),
            "--json-lines",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [record["database_ids"]["item_id"] for record in records] == [7, 3]


def test_dump_refuses_existing_output_without_core_queries(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "owned.json"
    output.write_text("operator-owned\n", encoding="utf-8")

    rc = cli_main(
        [
            "metadata",
            "dump-json",
            *_connection(),
            "--item-id",
            "3",
            "--output",
            str(output),
        ]
    )

    assert rc == 2
    assert output.read_text(encoding="utf-8") == "operator-owned\n"
    assert "--replace option" in capsys.readouterr().err


def test_set_accepts_hydrated_dump_and_convenience_values(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    values = tmp_path / "values.json"
    values.write_text(
        json.dumps(
            {
                "format": "liuxin.metadata.dump",
                "item_count": 1,
                "items": [
                    {
                        "database_ids": {"item_id": 7},
                        "liuxin": {
                            "tags": ["from dump"],
                            "genre": ["History"],
                        },
                    }
                ],
                "version": 1,
            }
        ),
        encoding="utf-8",
    )

    rc = cli_main(
        [
            "metadata",
            "set",
            *_connection(),
            "7",
            "--values-file",
            str(values),
            "--tag",
            "CLI tag",
            "--identifier",
            "doi=10.1/example",
            "--replace",
            "--target-level",
            "expression",
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["changed"] is True
    operation, payload = fake_core.commands[-1]
    assert operation == "metadata.write"
    assert payload["values"] == {
        "tags": ["CLI tag"],
        "genre": ["History"],
        "identifiers": {"doi": "10.1/example"},
    }
    assert payload["fields"] == ["tags", "genre", "identifiers"]
    assert payload["replace"] is True
    assert payload["target_level"] == "expression"


def test_clear_requires_authoritative_replace(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_main(["metadata", "set", *_connection(), "7", "--clear", "tags"])

    assert rc == 2
    assert not fake_core.commands
    assert "--clear requires --replace" in capsys.readouterr().err


def test_catalogue_write_values_accept_file_inspect_report() -> None:
    assert metadata_cli._extract_write_values(
        {
            "file_type": "epub",
            "metadata": {
                "title": "not currently writable here",
                "tags": ["embedded tag"],
                "identifiers": {"isbn": "9780000000000"},
            },
        }
    ) == {
        "tags": ["embedded tag"],
        "identifiers": {"isbn": "9780000000000"},
    }


def test_set_refuses_owned_report_before_catalogue_mutation(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "write-report.json"
    output.write_text("operator-owned\n", encoding="utf-8")

    rc = cli_main(
        [
            "metadata",
            "set",
            *_connection(),
            "7",
            "--tag",
            "would mutate",
            "--output",
            str(output),
        ]
    )

    assert rc == 2
    assert fake_core.commands == []
    assert output.read_text(encoding="utf-8") == "operator-owned\n"
    assert "--replace option" in capsys.readouterr().err


def test_set_treats_broken_output_symlink_as_owned_before_mutation(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "write-report.json"
    output.symlink_to(tmp_path / "missing-target.json")

    rc = cli_main(
        [
            "metadata",
            "set",
            *_connection(),
            "7",
            "--tag",
            "would mutate",
            "--output",
            str(output),
        ]
    )

    assert rc == 2
    assert fake_core.commands == []
    assert output.is_symlink()
    assert "--replace option" in capsys.readouterr().err


def test_export_opf_decodes_wire_bytes_and_never_clobbers(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "book.opf"
    assert cli_main(
        ["metadata", "export-opf", *_connection(), "3", "--output", str(output)]
    ) == 0
    assert output.read_bytes() == b"<package/>"

    assert cli_main(
        ["metadata", "export-opf", *_connection(), "3", "--output", str(output)]
    ) == 2
    assert output.read_bytes() == b"<package/>"
    assert "--replace option" in capsys.readouterr().err


def test_file_inspect_transfers_tortured_client_path_as_bytes(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_path = os.fsencode(tmp_path) + b"/bad-name-\xff.epub"
    descriptor = os.open(raw_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(descriptor, b"book-bytes")
    os.close(descriptor)
    path = Path(os.fsdecode(raw_path))

    rc = cli_main(["metadata", "file", "inspect", *_connection(), str(path)])

    assert rc == 0
    raw = capsys.readouterr().out
    assert "\\udcff" in raw
    result = json.loads(raw)
    assert result["metadata"]["title"] == "Original"
    operation, payload = fake_core.queries[-1]
    assert operation == "metadata.file.inspect"
    assert "path" not in payload
    assert base64.b64decode(payload["base64"]) == b"book-bytes"


def test_file_write_creates_verified_artifact_without_changing_input(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "book.epub"
    source.write_bytes(b"original-book")
    output = tmp_path / "updated.epub"

    rc = cli_main(
        [
            "metadata",
            "file",
            "write",
            *_connection(),
            str(source),
            "--output",
            str(output),
            "--metadata-json",
            '{"file_type":"epub","metadata":{"title":"Updated","authors":["CLI Author"]}}',
        ]
    )

    assert rc == 0
    assert source.read_bytes() == b"original-book"
    assert output.read_bytes() == fake_core.updated_file
    report = json.loads(capsys.readouterr().out)
    assert report["unmanaged_in_place"] is False
    assert report["verified"] is True
    operation, payload = fake_core.commands[-1]
    assert operation == "metadata.file.write"
    assert "path" not in payload
    assert base64.b64decode(payload["base64"]) == b"original-book"
    assert payload["metadata"] == {
        "title": "Updated",
        "authors": ["CLI Author"],
    }


def test_file_write_in_place_is_atomic_and_keeps_backup(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "book.epub"
    source.write_bytes(b"original-book")

    rc = cli_main(
        [
            "metadata",
            "file",
            "write",
            *_connection(),
            str(source),
            "--in-place",
            "--item-id",
            "7",
        ]
    )

    assert rc == 0
    assert source.read_bytes() == fake_core.updated_file
    assert (tmp_path / "book.epub.bak").read_bytes() == b"original-book"
    report = json.loads(capsys.readouterr().out)
    assert report["unmanaged_in_place"] is True
    assert report["backup_path"].endswith("book.epub.bak")


def test_file_write_in_place_refuses_owned_backup_before_core_mutation(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "book.epub"
    backup = tmp_path / "book.epub.bak"
    source.write_bytes(b"original-book")
    backup.write_bytes(b"operator-owned")

    rc = cli_main(
        [
            "metadata",
            "file",
            "write",
            *_connection(),
            str(source),
            "--in-place",
            "--item-id",
            "7",
        ]
    )

    assert rc == 2
    assert source.read_bytes() == b"original-book"
    assert backup.read_bytes() == b"operator-owned"
    assert fake_core.commands == []
    assert "--replace option" in capsys.readouterr().err


def test_file_write_in_place_refuses_concurrent_input_change(
    fake_core: _Core,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "book.epub"
    source.write_bytes(b"original-book")
    original_command = fake_core.command

    def racing_command(name: str, payload: dict[str, Any] | None = None) -> Any:
        result = original_command(name, payload)
        if name == "metadata.file.write":
            source.write_bytes(b"concurrent-writer")
        return result

    monkeypatch.setattr(fake_core, "command", racing_command)

    rc = cli_main(
        [
            "metadata",
            "file",
            "write",
            *_connection(),
            str(source),
            "--in-place",
            "--item-id",
            "7",
        ]
    )

    assert rc == 2
    assert source.read_bytes() == b"concurrent-writer"
    assert not (tmp_path / "book.epub.bak").exists()
    assert "Input changed" in capsys.readouterr().err


def test_file_write_refuses_existing_output_without_modifying_either_file(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "book.epub"
    output = tmp_path / "updated.epub"
    source.write_bytes(b"original-book")
    output.write_bytes(b"operator-owned")

    rc = cli_main(
        [
            "metadata",
            "file",
            "write",
            *_connection(),
            str(source),
            "--output",
            str(output),
            "--metadata-json",
            '{"title":"Updated"}',
        ]
    )

    assert rc == 2
    assert source.read_bytes() == b"original-book"
    assert output.read_bytes() == b"operator-owned"
    assert "--replace option" in capsys.readouterr().err


def test_online_sources_and_detached_identify_are_json(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_main(["metadata", "online", "sources", *_connection()]) == 0
    assert json.loads(capsys.readouterr().out)["sources"][0]["name"] == "Example"

    assert cli_main(
        [
            "metadata",
            "online",
            "identify",
            *_connection(),
            "--identifier",
            "isbn=9780000000000",
            "--plugin",
            "Example",
            "--detach",
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["detached"] is True
    operation, payload = fake_core.commands[-1]
    assert operation == "metadata.identify.start"
    assert payload["identifiers"] == {"isbn": "9780000000000"}
    assert payload["allowed_plugins"] == ["Example"]


def test_online_identify_waits_for_and_returns_job_result(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_main(
        ["metadata", "online", "identify", *_connection(), "--title", "Found"]
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "succeeded"
    assert result["result"]["results"][0]["metadata"]["title"] == "Found"


def test_online_cover_can_publish_binary_separately_from_json_report(
    fake_core: _Core,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cover = tmp_path / "cover.jpg"

    rc = cli_main(
        [
            "metadata",
            "online",
            "cover",
            *_connection(),
            "--title",
            "Found",
            "--cover-output",
            str(cover),
        ]
    )

    assert rc == 0
    assert cover.read_bytes() == b"cover-bytes"
    report = json.loads(capsys.readouterr().out)
    cover_report = report["result"]["cover"]
    assert cover_report["content_path"] == str(cover)
    assert cover_report["size"] == len(b"cover-bytes")
    assert "content" not in cover_report


def test_metadata_help_is_available_from_packaged_and_compatibility_parsers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as packaged:
        cli_main(["metadata", "--help"])
    assert packaged.value.code == 0
    assert "dump-json" in capsys.readouterr().out

    from LiuXin_alpha.surfaces.cli.squashfs import main as compatibility_main

    with pytest.raises(SystemExit) as compatibility:
        compatibility_main(["metadata", "--help"])
    assert compatibility.value.code == 0
    assert "dump-json" in capsys.readouterr().out


def test_catalogue_commands_round_trip_through_a_real_local_core(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from LiuXin_alpha.core import create_core

    database = tmp_path / "catalogue.sqlite"
    runtime = create_core(
        database_path=database,
        create=True,
        backup=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    try:
        created = runtime.command(
            "catalog.wemi.create",
            {
                "work": {"title": "CLI integration title"},
                "expression": {"label": "CLI integration expression"},
                "manifestation": {"subtitle": "CLI integration manifestation"},
                "items": [{"inventory_code": "cli-metadata-item"}],
                "origin": "cli-metadata-test",
            },
        )
        item_id = int(created["item_ids"][0])
    finally:
        runtime.shutdown()
    capsys.readouterr()

    shown = tmp_path / "shown.json"
    assert cli_main(
        [
            "metadata",
            "show",
            "--database",
            str(database),
            str(item_id),
            "--output",
            str(shown),
        ]
    ) == 0
    assert json.loads(shown.read_text(encoding="utf-8"))["database_ids"]["item_id"] == item_id

    assert cli_main(
        [
            "metadata",
            "set",
            "--database",
            str(database),
            str(item_id),
            "--tag",
            "CLI integration tag",
        ]
    ) == 0
    write_report = json.loads(capsys.readouterr().out)
    assert write_report["changed"] is True

    dump = tmp_path / "dump.json"
    assert cli_main(
        [
            "metadata",
            "dump-json",
            "--database",
            str(database),
            "--all",
            "--output",
            str(dump),
        ]
    ) == 0
    dumped = json.loads(dump.read_text(encoding="utf-8"))
    assert dumped["item_count"] == 1
    assert "CLI integration tag" in str(dumped["items"][0])

    opf = tmp_path / "metadata.opf"
    assert cli_main(
        [
            "metadata",
            "export-opf",
            "--database",
            str(database),
            str(item_id),
            "--output",
            str(opf),
        ]
    ) == 0
    assert b"<package" in opf.read_bytes()

    epub = tmp_path / "source.epub"
    with zipfile.ZipFile(epub, "w") as archive:
        archive.writestr(
            "mimetype",
            "application/epub+zip",
            compress_type=zipfile.ZIP_STORED,
        )
        archive.writestr(
            "META-INF/container.xml",
            """<?xml version="1.0"?>
<container version="1.0"
 xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles><rootfile full-path="content.opf"
   media-type="application/oebps-package+xml"/></rootfiles>
</container>
""",
        )
        archive.writestr(
            "content.opf",
            """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:opf="http://www.idpf.org/2007/opf"
 version="2.0" unique-identifier="book-id">
 <metadata>
  <dc:identifier id="book-id">cli-source</dc:identifier>
  <dc:title>Before CLI</dc:title>
  <dc:creator opf:role="aut">Before Author</dc:creator>
  <dc:language>en</dc:language>
 </metadata><manifest/><spine/>
</package>
""",
        )

    embedded_before = tmp_path / "embedded-before.json"
    assert cli_main(
        [
            "metadata",
            "file",
            "inspect",
            "--database",
            str(database),
            str(epub),
            "--output",
            str(embedded_before),
        ]
    ) == 0
    assert json.loads(embedded_before.read_text(encoding="utf-8"))["metadata"]["title"] == "Before CLI"

    rewritten = tmp_path / "rewritten.epub"
    write_report = tmp_path / "embedded-write.json"
    assert cli_main(
        [
            "metadata",
            "file",
            "write",
            "--database",
            str(database),
            str(epub),
            "--output",
            str(rewritten),
            "--item-id",
            str(item_id),
            "--report-output",
            str(write_report),
        ]
    ) == 0
    assert epub.read_bytes() != rewritten.read_bytes()
    assert json.loads(write_report.read_text(encoding="utf-8"))["verified"] is True

    embedded_after = tmp_path / "embedded-after.json"
    assert cli_main(
        [
            "metadata",
            "file",
            "inspect",
            "--database",
            str(database),
            str(rewritten),
            "--output",
            str(embedded_after),
        ]
    ) == 0
    assert json.loads(embedded_after.read_text(encoding="utf-8"))["metadata"]["title"] == "CLI integration title"
