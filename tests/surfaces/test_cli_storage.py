"""
Check storage-ingest discovery, control paths, reports, locks, and cancellation.

Temporary filesystem fixtures exercise the real CLI discovery/preflight boundary,
not successful managed catalogue ingestion. Several ebook-named fixtures contain
arbitrary bytes; the SquashFS fixture tests detection plus missing-tool reporting,
not valid image extraction. Lock ownership is real, signal delivery is a direct
handler call, and the subprocess smoke test imports from checkout rather than an
installed wheel. Final-file assertions do not prove crash-time publication safety.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import zipfile
from argparse import Namespace
from pathlib import Path
from typing import cast

import pytest

import LiuXin_alpha.surfaces.cli.storage_commands.constants as storage_cli_constants
import LiuXin_alpha.surfaces.cli.storage_commands.ingest_paths as storage_cli_ingest_paths
import LiuXin_alpha.surfaces.cli.storage_commands.ingest_reporting as storage_cli_ingest_reporting
import LiuXin_alpha.surfaces.cli.storage_commands.signals as storage_cli_signals
from LiuXin_alpha.surfaces.cli.app import main as cli_main
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_preflight
from LiuXin_alpha.utils.lock import ExclusiveFile

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "12345678-1234-5678-9234-567812345678"


def _base_arguments(tmp_path: Path, source: Path) -> list[str]:
    """
    Build shared ingest argv using a fixed run UUID and isolated log/report paths.

    Paths are only stringified; no directory or output is created by this helper.
    Console progress is disabled, but ordinary report/status output remains enabled.

    Example:
        >>> arguments = _base_arguments(Path("scratch"), Path("books"))
        >>> arguments[:2], arguments[-1]
        (['storage', 'ingest'], '--no-console-progress')


    :param tmp_path: Parent under which logs/ and report.json arguments are constructed.
    :param source: Discovery/ingest source directory supplied to --source-root.
    :return: Fresh mutable argument list that callers extend with scenario-specific flags.
    """
    return [
        "storage",
        "ingest",
        "--source-root",
        str(source),
        "--log-directory",
        str(tmp_path / "logs"),
        "--report-file",
        str(tmp_path / "report.json"),
        "--run-id",
        RUN_ID,
        "--no-console-progress",
    ]


def _events(path: str) -> list[dict[str, object]]:
    """
    Decode every UTF-8 JSONL event line in a generated test log.

    Blank/malformed lines raise rather than being skipped. The cast is a typing
    aid, not mapping validation; the file is read entirely without a size limit.

    Example:
        >>> events = _events(str(event_log_path))  # doctest: +SKIP


    :param path: Generated event-log path, used directly without user expansion.
    :return: Decoded line values in file order, expected by callers to be dictionaries.
    """
    return [
        cast(dict[str, object], json.loads(line))
        for line in Path(path).read_text(encoding="utf-8").splitlines()
    ]


def _event_names(events: list[dict[str, object]]) -> list[object]:
    """
    Project event names from each log record's required context mapping.

    Example:
        >>> _event_names([{"context": {"event": "cli_started"}}, {"context": {}}])
        ['cli_started', None]


    :param events: Ordered decoded records whose context values support .get().
    :return: Names in input order, with None for a context lacking an event key.
    :raises KeyError: A record lacks the required context key.
    """
    return [cast(dict[str, object], event["context"]).get("event") for event in events]


def test_discovery_writes_full_correlated_atomic_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Discover loose/archive fixtures and correlate stdout, the final report, and JSONL events.

    Observe completed publication and absent staging residue, not intermediate
    visibility or crash recovery. No managed database ingest is requested.

    Example:
        >>> test_discovery_writes_full_correlated_atomic_report(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary parent for a loose file, real ZIP, logs, and report.
    :param capsys: Capture for comparing stdout JSON with disk and checking run-ID stderr.
    :return: None; assert discovery totals, terminal status, publication, and event correlation.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "loose.mobi").write_bytes(b"book")
    with zipfile.ZipFile(source / "pack.zip", "w") as archive:
        archive.writestr("nested/book.txt", b"member")

    report_path = tmp_path / "report.json"
    rc = cli_main([*_base_arguments(tmp_path, source), "--discover-only"])

    assert rc == storage_cli_constants.EXIT_OK
    captured = capsys.readouterr()
    payload = cast(dict[str, object], json.loads(captured.out))
    assert payload == json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["command"] == "storage ingest"
    assert payload["run_id"] == RUN_ID
    assert payload["mode"] == "discovery"
    assert payload["status"] == "complete"
    assert payload["exit_code"] == 0
    assert payload["lock_file"] is None
    assert "Run ID: " + RUN_ID in captured.err
    assert not tuple(report_path.parent.glob(".report.json.*.tmp"))

    ingest_report = cast(dict[str, object], payload["report"])
    assert ingest_report["files_examined"] == 2
    assert ingest_report["top_level_containers"] == 1
    events = _events(cast(str, payload["event_log"]))
    assert _event_names(events)[0] == "cli_started"
    assert _event_names(events)[-1] == "cli_complete"
    assert all(
        cast(dict[str, object], event["context"]).get("details", {}).get("run_id")
        == RUN_ID
        for event in events
        if cast(dict[str, object], event["context"]).get("event")
        not in {None, "captured_output"}
    )


def test_preflight_reports_missing_required_squashfs_reader_without_writes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Report a missing required SquashFS reader without creating the proposed database.

    Preflight still publishes its report/logs. The file has SquashFS magic but is
    not a valid image, and the executable-discovery fake hides only the chosen tool.

    Example:
        >>> test_preflight_reports_missing_required_squashfs_reader_without_writes(tmp_path, capsys, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Directory for the magic-byte source, report/logs, and absent catalogue.
    :param capsys: Capture for the structured preflight result.
    :param monkeypatch: Patcher forcing the configured unsquashfs executable to be unavailable.
    :return: None; assert issue status, failed reader readiness, and no database creation.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "unknown.bin").write_bytes(b"hsqs" + b"not-a-real-image")
    database = tmp_path / "catalogue.sqlite"
    original_which = ingest_preflight.shutil.which
    monkeypatch.setattr(
        ingest_preflight.shutil,
        "which",
        lambda command: (
            None
            if os.fspath(command) == "definitely-missing-unsquashfs"
            else original_which(command)
        ),
    )

    rc = cli_main(
        [
            *_base_arguments(tmp_path, source),
            "--database",
            str(database),
            "--preflight-only",
            "--unsquashfs-exe",
            "definitely-missing-unsquashfs",
        ]
    )

    assert rc == storage_cli_constants.EXIT_ISSUES
    payload = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert payload["mode"] == "preflight"
    assert payload["status"] == "issues"
    assert payload["ok"] is False
    preflight = cast(dict[str, object], payload["preflight"])
    assert preflight["ready"] is False
    checks = cast(list[dict[str, object]], preflight["checks"])
    squashfs = next(check for check in checks if check["name"] == "squashfs_reader")
    assert squashfs["ok"] is False
    assert squashfs["severity"] == "error"
    assert not database.exists()


def test_database_inside_source_is_configuration_error_with_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Reject a catalogue path inside the discovery tree and publish the configuration failure.

    Example:
        >>> test_database_inside_source_is_configuration_error_with_report(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary source and external run-output directory.
    :param capsys: Capture for inspecting the reported usage error and artifact paths.
    :return: None; assert error status/traceback, durable report, and the sole failure event.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")

    rc = cli_main(
        [
            *_base_arguments(tmp_path, source),
            "--database",
            str(source / "catalogue.sqlite"),
        ]
    )

    assert rc == storage_cli_constants.EXIT_USAGE
    payload = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert payload["status"] == "configuration_error"
    assert payload["exit_code"] == storage_cli_constants.EXIT_USAGE
    error = cast(dict[str, object], payload["error"])
    assert "--database must be outside --source-root" in cast(str, error["message"])
    assert "CLIUsageError" in cast(str, error["traceback"])
    assert Path(cast(str, payload["report_file"])).is_file()
    assert _event_names(_events(cast(str, payload["event_log"]))) == [
        "cli_configuration_error"
    ]


def test_no_stdout_report_keeps_result_in_durable_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Suppress stdout JSON while retaining a successful discovery report on disk.

    Example:
        >>> test_no_stdout_report_keeps_result_in_durable_file(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Temporary source, logs, and report location.
    :param capsys: Capture used to require empty stdout after discovery.
    :return: None; assert zero status and an on-disk success receipt with stdout_report false.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")

    rc = cli_main(
        [*_base_arguments(tmp_path, source), "--discover-only", "--no-stdout-report"]
    )

    assert rc == storage_cli_constants.EXIT_OK
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert payload["stdout_report"] is False
    assert payload["ok"] is True


def test_require_existing_database_fails_before_creating_it(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Refuse a missing required catalogue and leave the database path absent.

    Example:
        >>> test_require_existing_database_fails_before_creating_it(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Directory containing an empty source and nonexistent database target.
    :param capsys: Capture for the configuration-error report.
    :return: None; assert usage status, missing-database guidance, and no catalogue creation.
    """
    source = tmp_path / "source"
    source.mkdir()
    database = tmp_path / "missing.sqlite"

    rc = cli_main(
        [
            *_base_arguments(tmp_path, source),
            "--database",
            str(database),
            "--require-existing-database",
        ]
    )

    assert rc == storage_cli_constants.EXIT_USAGE
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "configuration_error"
    assert "database does not exist" in payload["error"]["message"]
    assert not database.exists()


@pytest.mark.parametrize(
    ("path_kind", "expected_message"),
    [
        ("report", "--report-file must be outside --source-root"),
        ("lock", "--lock-file must be outside --source-root"),
        (
            "materialization",
            "--materialization-root must be outside --source-root",
        ),
    ],
)
def test_path_validation_rejects_run_outputs_inside_source_root(
    tmp_path: Path,
    path_kind: str,
    expected_message: str,
) -> None:
    """
    Reject each parametrized report, lock, or materialization path inside the source.

    Call path validation directly; no CLI execution, catalogue creation, or report
    publication is needed to exercise these lexical/resolved-path safeguards.

    Example:
        >>> test_path_validation_rejects_run_outputs_inside_source_root(tmp_path, "report", "--report-file must be outside --source-root")  # doctest: +SKIP


    :param tmp_path: Temporary parent used to separate permitted outputs from the source.
    :param path_kind: Parametrized report, lock, or materialization selector moved inside source.
    :param expected_message: Regex expected in the resulting CLIUsageError.
    :return: None; assert the selected control path is rejected with specific guidance.
    """
    source = tmp_path / "source"
    source.mkdir()
    report = (
        source / "report.json" if path_kind == "report" else tmp_path / "report.json"
    )
    lock = source / "ingest.lock" if path_kind == "lock" else tmp_path / "ingest.lock"
    materialization = (
        source / "materialized" if path_kind == "materialization" else None
    )
    args = Namespace(
        replace_report=False,
        database=None,
        require_existing_database=False,
        materialization_root=materialization,
    )

    with pytest.raises(storage_cli_constants.CLIUsageError, match=expected_message):
        storage_cli_ingest_paths._validate_paths(
            args,
            source_root=source,
            report_path=report,
            lock_path=lock,
        )


def test_existing_report_is_never_overwritten_without_explicit_opt_in(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Preserve an operator-owned report while emitting an overwrite refusal on stdout.

    Example:
        >>> test_existing_report_is_never_overwritten_without_explicit_opt_in(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Parent for the empty source and pre-existing report fixture.
    :param capsys: Capture for the configuration-error JSON and replacement guidance.
    :return: None; assert usage status and unchanged operator-owned report contents.
    """
    source = tmp_path / "source"
    source.mkdir()
    report = tmp_path / "report.json"
    report.write_text("operator-owned\n", encoding="utf-8")

    rc = cli_main([*_base_arguments(tmp_path, source), "--discover-only"])

    assert rc == storage_cli_constants.EXIT_USAGE
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "configuration_error"
    assert "pass --replace-report" in payload["error"]["message"]
    assert report.read_text(encoding="utf-8") == "operator-owned\n"


def test_atomic_report_publisher_cannot_clobber_a_racing_writer(
    tmp_path: Path,
) -> None:
    """
    Refuse a destination already occupied at publication and remove temporary staging.

    The competing file exists before the call; this simulates the publisher's
    collision boundary without a concurrent process or timed race injection.

    Example:
        >>> test_atomic_report_publisher_cannot_clobber_a_racing_writer(tmp_path)  # doctest: +SKIP


    :param tmp_path: Directory for the competing report and any temporary publication file.
    :return: None; assert CLIUsageError, retained competing bytes, and no staging residue.
    """
    report = tmp_path / "report.json"
    report.write_text("racing-writer\n", encoding="utf-8")

    with pytest.raises(storage_cli_constants.CLIUsageError, match="--replace-report"):
        storage_cli_ingest_reporting._write_report(
            report,
            {"ok": True},
            replace=False,
            compact=True,
        )

    assert report.read_text(encoding="utf-8") == "racing-writer\n"
    assert not tuple(tmp_path.glob(".report.json.*.tmp"))


def test_real_run_refuses_an_already_owned_explicit_lock(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Hold a real exclusive lock and require ingest refusal before catalogue creation.

    Example:
        >>> test_real_run_refuses_an_already_owned_explicit_lock(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Directory for source, held lock, proposed database, and reports.
    :param capsys: Capture for the configuration failure and explicit lock-path receipt.
    :return: None; assert ownership guidance, usage status, and an absent database file.
    """
    source = tmp_path / "source"
    source.mkdir()
    (source / "book.epub").write_bytes(b"book")
    database = tmp_path / "catalogue.sqlite"
    lock = tmp_path / "ingest.lock"

    with ExclusiveFile(str(lock), timeout=0):
        rc = cli_main(
            [
                *_base_arguments(tmp_path, source),
                "--database",
                str(database),
                "--lock-file",
                str(lock),
            ]
        )

    assert rc == storage_cli_constants.EXIT_USAGE
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "configuration_error"
    assert "another ingest owns run lock" in payload["error"]["message"]
    assert payload["lock_file"] == str(lock)
    assert not database.exists()


def test_signal_cancellation_requires_a_second_signal_to_force_unwind() -> None:
    """
    Latch termination on the first direct delivery and raise on a second interrupt.

    Call the handler directly; no process signal is sent and no handler is installed.

    Example:
        >>> test_signal_cancellation_requires_a_second_signal_to_force_unwind()


    :return: None; assert requested state, first-signal identity, and second-signal unwinding.
    """
    cancellation = storage_cli_signals.SignalCancellation()

    cancellation._receive(signal.SIGTERM, None)

    assert cancellation.requested() is True
    assert cancellation.signal_number == signal.SIGTERM
    with pytest.raises(KeyboardInterrupt):
        cancellation._receive(signal.SIGINT, None)


def test_module_invocation_exposes_storage_surface_from_checkout(
    tmp_path: Path,
) -> None:
    """
    Start the module CLI from outside the checkout and inspect storage-ingest help.

    Prepend checkout src to the child PYTHONPATH while retaining any existing
    entries. This is a checkout-import smoke test, not installed-package verification.

    Example:
        >>> test_module_invocation_exposes_storage_surface_from_checkout(tmp_path)  # doctest: +SKIP


    :param tmp_path: Child process working directory, separate from the repository.
    :return: None; assert clean exit and advertised preflight/report/signal behavior.
    """
    environment = dict(os.environ)
    source_path = str(REPO_ROOT / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (source_path, environment.get("PYTHONPATH", "")) if part
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "LiuXin_alpha.surfaces.cli",
            "storage",
            "ingest",
            "--help",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "--preflight-only" in completed.stdout
    assert "--report-file" in completed.stdout
    assert "130 SIGINT; 143 SIGTERM" in completed.stdout
