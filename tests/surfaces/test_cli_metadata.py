"""
Exercise metadata CLI request shaping, byte publication, and catalogue integration.

Most cases use a recording Core with fixed catalogue, rewritten-file, and job
receipts; they do not contact online providers or verify real ebook metadata.
Temporary filesystem writes exercise output ownership and selected race checks.
The final integration case uses a real local SQLite Core and minimal EPUB to
check catalogue writes, OPF export, and title rewriting through the CLI.
"""

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
    """
    Encode fixture bytes in the tagged Core wire representation.

    Example:
        >>> _wire(b"book")
        {'$type': 'bytes', 'base64': 'Ym9vaw=='}


    :param content: Exact bytes to expose through a JSON-compatible fake receipt.
    :return: New bytes-tag/base64 dictionary with ASCII payload text.
    """
    return {
        "$type": "bytes",
        "base64": base64.b64encode(content).decode("ascii"),
    }


class _Core:
    """
    Record metadata/job calls and return deterministic, deliberately narrow fixtures.

    Queries expose two pageable item IDs, hostile Unicode, and immediate job
    completion. File inspection distinguishes only the fixed rewritten bytes;
    file writes do not parse ebooks. Cover results depend on the most recent
    recorded command. Unexpected operation names fail after being recorded.

    Example:
        >>> core = _Core()
        >>> core.query("metadata.get", {"item_id": 7})["liuxin"]
        {'title': 'Book 7'}
    """

    def __init__(self) -> None:
        """
        Create independent call logs and the default catalogue/rewrite fixtures.

        Example:
            >>> core = _Core()
            >>> core.item_ids, core.commands
            ([3, 7], [])


        :return: None; initialize two item IDs, fixed output bytes, and empty call lists.
        """
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.commands: list[tuple[str, dict[str, Any]]] = []
        self.item_ids = [3, 7]
        self.updated_file = b"updated-book-bytes"

    def query(self, name: str, payload: dict[str, Any] | None = None) -> Any:
        """
        Record a shallow payload copy and serve one supported metadata/job query.

        Paginate item_ids by offset/limit; decode inspection bytes strictly but
        do not parse a book. jobs.get is immediately succeeded; jobs.result returns
        a cover only when the last recorded command started cover discovery.

        Example:
            >>> core = _Core()
            >>> core.query("metadata.file.formats")["writable"]
            ['epub']


        :param name: Exact metadata, rows.query, jobs.get, or jobs.result operation.
        :param payload: Request values shallow-copied for recording, or None for empty.
        :return: Fresh operation-specific fixture receipt, not a live database result.
        :raises AssertionError: The recorded operation name has no fake implementation.
        """
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
        """
        Record a request and fabricate a metadata-write or online-job submission receipt.

        Catalogue writes echo selected fields without persistence. File writes
        require no path key and return updated_file without modifying any input.
        Both online operations share the fixed pending job ID job-1.

        Example:
            >>> _Core().command("metadata.identify.start")
            {'job_id': 'job-1', 'state': 'pending'}


        :param name: Supported catalogue/file write or identify/cover start operation.
        :param payload: Request values shallow-copied into the command log; None means empty.
        :return: Synthetic write or job receipt; no catalogue, file, or job is created.
        :raises AssertionError: A file request contains path or the operation is unsupported.
        """
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
    """
    Expose a fake Core through the session context expected by metadata handlers.

    Enter and exit do not acquire, close, or reset anything; exceptions propagate.

    Example:
        >>> core = _Core()
        >>> with _Session(core) as session:
        ...     session.client is core
        True
    """

    def __init__(self, core: _Core) -> None:
        """
        Retain the supplied client without copying its observations or taking ownership.

        Example:
            >>> core = _Core()
            >>> _Session(core).client is core
            True


        :param core: Recording fake shared with the test making assertions.
        :return: None; expose core as the client attribute.
        """
        self.client = core

    def __enter__(self) -> "_Session":
        """
        Return this already-constructed session without opening a resource.

        Example:
            >>> session = _Session(_Core())
            >>> session.__enter__() is session
            True


        :return: This session, preserving its original client reference.
        """
        return self

    def __exit__(self, *_args: object) -> None:
        """
        Leave context without cleanup or exception suppression.

        Example:
            >>> _Session(_Core()).__exit__(None, None, None) is None
            True


        :param _args: Ignored exception type, value, and traceback supplied by with.
        :return: None, allowing any body exception to propagate.
        """
        return None


@pytest.fixture
def fake_core(monkeypatch: pytest.MonkeyPatch) -> _Core:
    """
    Route every metadata Core opening in a test to one shared recording client.

    Parsed local/remote selectors and startup flags are ignored by the opener;
    using an endpoint in these tests does not open a socket.

    Example:
        >>> core = fake_core.__wrapped__(monkeypatch)  # doctest: +SKIP


    :param monkeypatch: Pytest patcher restoring the real session opener after the test.
    :return: Fresh fake Core shared by all sessions opened during this fixture's lifetime.
    """
    core = _Core()
    monkeypatch.setattr(
        metadata_cli,
        "open_surface_core_from_args",
        lambda _args, **_kwargs: _Session(core),
    )
    return core


def _connection() -> list[str]:
    """
    Supply a dummy catalogue selector accepted by the CLI and ignored by the fake opener.

    Example:
        >>> _connection()
        ['--database', 'catalogue.sqlite']


    :return: Fresh two-token argument list; no database file is created here.
    """
    return ["--database", "catalogue.sqlite"]


def test_show_reads_one_record_as_interoperable_json(
    fake_core: _Core,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Escape a lone surrogate in JSON and forward the selected metadata shape flags.

    Example:
        >>> test_show_reads_one_record_as_interoperable_json(fake_core, capsys)  # doctest: +SKIP


    :param fake_core: Recording client returning the hostile-text item fixture.
    :param capsys: Capture used to inspect escaped text and decode the resulting JSON.
    :return: None; assert item identity, valid JSON, and the exact metadata.get payload.
    """
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
    """
    Accept the get alias and remote selector while using a mocked session opener.

    Example:
        >>> test_show_accepts_remote_core_endpoint(fake_core, capsys)  # doctest: +SKIP


    :param fake_core: Fixture redirecting even endpoint-based sessions to the fake client.
    :param capsys: Capture for decoding the selected record's JSON.
    :return: None; assert successful parsing/dispatch and item identity, not HTTP transport.
    """
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
    """
    Page both fake IDs into a complete ordered dump and leave no staging file behind.

    This observes successful publication, not visibility during a crash or rename.

    Example:
        >>> test_dump_all_pages_ids_and_atomically_writes_deterministic_document(fake_core, tmp_path)  # doctest: +SKIP


    :param fake_core: Two-item recording client exposing one row per requested page.
    :param tmp_path: Directory receiving the dump and checked for staging residue.
    :return: None; assert envelope version/count/order, two page queries, and cleanup.
    """
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
    """
    Read BOM-prefixed, commented ID lines and preserve first-seen order in JSONL.

    Example:
        >>> test_dump_supports_json_lines_and_utf8_bom_item_id_file(fake_core, tmp_path)  # doctest: +SKIP


    :param fake_core: Fake client hydrating each selected item ID.
    :param tmp_path: Directory for the input ID list and generated JSONL file.
    :return: None; assert IDs 7 then 3 occur once each in the published records.
    """
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
    """
    Refuse a pre-existing dump destination and preserve its operator-owned contents.

    This case asserts status, content, and guidance; it does not inspect the query log.

    Example:
        >>> test_dump_refuses_existing_output_without_core_queries(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Installed fake session fixture for any attempted Core access.
    :param tmp_path: Directory containing the pre-owned report destination.
    :param capsys: Capture for replacement-option guidance on stderr.
    :return: None; assert status two, retained content, and the refusal message.
    """
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
    """
    Merge a single-item dump with CLI overrides into an ordered expression-level write.

    Example:
        >>> test_set_accepts_hydrated_dump_and_convenience_values(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Recorder for metadata.write and its synthetic changed receipt.
    :param tmp_path: Directory holding the hydrated JSON values fixture.
    :param capsys: Capture for decoding the write report.
    :return: None; assert override values, field order, replacement policy, and target level.
    """
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
    """
    Reject a field-clear request lacking --replace before dispatching any command.

    Example:
        >>> test_clear_requires_authoritative_replace(fake_core, capsys)  # doctest: +SKIP


    :param fake_core: Recorder whose command list must remain empty.
    :param capsys: Capture for the authoritative-replacement requirement message.
    :return: None; assert status two, no mutation request, and explanatory stderr.
    """
    rc = cli_main(["metadata", "set", *_connection(), "7", "--clear", "tags"])

    assert rc == 2
    assert not fake_core.commands
    assert "--clear requires --replace" in capsys.readouterr().err


def test_catalogue_write_values_accept_file_inspect_report() -> None:
    """
    Extract supported catalogue fields from an inspection report while dropping title.

    Example:
        >>> test_catalogue_write_values_accept_file_inspect_report()


    :return: None; assert only tags and identifiers survive the writable-field projection.
    """
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
    """
    Preserve an occupied JSON report and avoid sending the proposed catalogue write.

    Example:
        >>> test_set_refuses_owned_report_before_catalogue_mutation(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Command recorder checked for absence of mutation requests.
    :param tmp_path: Directory containing the pre-existing write report.
    :param capsys: Capture for the output replacement refusal.
    :return: None; assert failure status, empty command log, retained content, and guidance.
    """
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
    """
    Treat a dangling report symlink as occupied rather than publishing through it.

    Example:
        >>> test_set_treats_broken_output_symlink_as_owned_before_mutation(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Recorder whose mutation log must remain empty.
    :param tmp_path: Temporary directory supporting the dangling symlink fixture.
    :param capsys: Capture for the replacement-option refusal.
    :return: None; assert status two, no command, and preservation of the symlink.
    """
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
    """
    Publish the fake wire-encoded OPF once and refuse a second non-replacing export.

    Example:
        >>> test_export_opf_decodes_wire_bytes_and_never_clobbers(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Fixture supplying the fixed package-element wire bytes.
    :param tmp_path: Directory for the exported OPF file.
    :param capsys: Capture for the repeated-export refusal message.
    :return: None; assert exact bytes survive the successful then refused publications.
    """
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
    """
    Send file contents, not a hostile local filename, to Core inspection.

    The filename contains an undecodable byte represented by a surrogate after
    filesystem decoding; JSON must escape it. The fake does not parse an EPUB.

    Example:
        >>> test_file_inspect_transfers_tortured_client_path_as_bytes(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Recorder for the final byte-based inspection request.
    :param tmp_path: Directory supporting the raw-byte filename fixture.
    :param capsys: Capture for escaped-path JSON and the synthetic metadata result.
    :return: None; assert valid output, absent remote path, and the original transferred bytes.
    """
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
    """
    Publish rewritten fixture bytes separately and report successful re-inspection.

    Verification here uses the fake inspector, not an ebook parser or independent
    comparison of every requested metadata value.

    Example:
        >>> test_file_write_creates_verified_artifact_without_changing_input(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Recorder returning fixed rewritten bytes and inspection metadata.
    :param tmp_path: Directory for independent source and output files.
    :param capsys: Capture for the unmanaged/verified result flags.
    :return: None; assert preserved input, published bytes, and unwrapped byte-based request.
    """
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
    """
    Observe successful in-place replacement with original bytes retained in a backup.

    The assertions check final files and report fields, not crash-time atomicity.

    Example:
        >>> test_file_write_in_place_is_atomic_and_keeps_backup(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Fixture returning the fixed rewritten file bytes.
    :param tmp_path: Directory for the unmanaged source and default .bak file.
    :param capsys: Capture for in-place and backup-path report fields.
    :return: None; assert final source/backup contents and successful in-place reporting.
    """
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
    """
    Refuse an occupied default backup before requesting the metadata rewrite.

    Example:
        >>> test_file_write_in_place_refuses_owned_backup_before_core_mutation(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Recorder checked for absence of rewrite commands.
    :param tmp_path: Directory containing source and operator-owned backup fixtures.
    :param capsys: Capture for backup replacement guidance.
    :return: None; assert failure status, unchanged files, and an empty command log.
    """
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
    """
    Detect a source rewrite injected during the Core command before backup publication.

    This covers one explicit timing boundary, not every possible filesystem race.

    Example:
        >>> test_file_write_in_place_refuses_concurrent_input_change(fake_core, tmp_path, monkeypatch, capsys)  # doctest: +SKIP


    :param fake_core: Original command implementation wrapped by the injected race.
    :param tmp_path: Directory holding the concurrently changed source file.
    :param monkeypatch: Patcher installing and restoring the command wrapper.
    :param capsys: Capture for the changed-input diagnostic.
    :return: None; assert refusal preserves concurrent bytes and creates no backup.
    """
    source = tmp_path / "book.epub"
    source.write_bytes(b"original-book")
    original_command = fake_core.command

    def racing_command(name: str, payload: dict[str, Any] | None = None) -> Any:
        """
        Delegate to the fake and change the source after a metadata-file write receipt.

        Example:
            >>> receipt = racing_command("metadata.file.write", {"file_type": "epub"})  # doctest: +SKIP


        :param name: Operation delegated unchanged to the captured original command.
        :param payload: Optional request mapping forwarded without modification.
        :return: Original receipt after injecting concurrent-writer bytes for file writes.
        """
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
    """
    Reject a new-artifact destination that already contains operator-owned bytes.

    Example:
        >>> test_file_write_refuses_existing_output_without_modifying_either_file(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Installed fake Core fixture for the attempted write workflow.
    :param tmp_path: Directory containing the independent source and occupied output.
    :param capsys: Capture for replacement-option guidance.
    :return: None; assert status two and byte-for-byte preservation of both files.
    """
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
    """
    Render source capabilities and submit an identifier/plugin-selected detached job.

    Example:
        >>> test_online_sources_and_detached_identify_are_json(fake_core, capsys)  # doctest: +SKIP


    :param fake_core: Recorder supplying capabilities and a pending job receipt.
    :param capsys: Capture for the separate source and detached-submission JSON reports.
    :return: None; assert routing/selectors and detach reporting without contacting providers.
    """
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
    """
    Project the fake job's immediate successful completion into the identify report.

    No nonterminal poll, timeout, or live source lookup is exercised here.

    Example:
        >>> test_online_identify_waits_for_and_returns_job_result(fake_core, capsys)  # doctest: +SKIP


    :param fake_core: Fixture returning succeeded on the first job-status query.
    :param capsys: Capture for the completion state and synthetic metadata title.
    :return: None; assert success status and the nested identification result.
    """
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
    """
    Publish fake cover bytes and replace their wire payload with report path/size fields.

    The fixture bytes are not a validated JPEG despite the destination suffix.

    Example:
        >>> test_online_cover_can_publish_binary_separately_from_json_report(fake_core, tmp_path, capsys)  # doctest: +SKIP


    :param fake_core: Fixture returning the synthetic completed cover job.
    :param tmp_path: Directory receiving the binary cover artifact.
    :param capsys: Capture for the JSON report, kept separate from cover bytes.
    :return: None; assert exact artifact bytes and path/size replacing embedded content.
    """
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


def test_metadata_help_is_available_from_the_application_parser(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Expose metadata help through the application entry point.

    Example:
        >>> test_metadata_help_is_available_from_the_application_parser(capsys)  # doctest: +SKIP


    :param capsys: Capture for the parser help invocation.
    :return: None; assert clean argparse exits and advertised dump-json support.
    """
    with pytest.raises(SystemExit) as packaged:
        cli_main(["metadata", "--help"])
    assert packaged.value.code == 0
    assert "dump-json" in capsys.readouterr().out



def test_catalogue_commands_round_trip_through_a_real_local_core(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Persist catalogue metadata and rewrite a minimal EPUB through a real SQLite Core.

    Create a WEMI item, read/tag/dump it through fresh CLI sessions, and export OPF.
    Build an EPUB locally, inspect its original title, rewrite it from the selected
    item, then re-inspect the rewritten title. Storage and maintenance managers
    are disabled on initial Core creation; no online source or PostgreSQL is used.

    Example:
        >>> test_catalogue_commands_round_trip_through_a_real_local_core(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated directory for SQLite, JSON reports, OPF, and EPUB artifacts.
    :param capsys: Capture isolating startup chatter and the catalogue write receipt.
    :return: None; assert persisted tags, export content, changed EPUB bytes, and final title.
    """
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
