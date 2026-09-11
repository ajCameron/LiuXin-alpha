"""
Inspect CLI-host mixed-ingest report/event files and reconstruct later attempts.

Runs are grouped by UUID and ordered by filesystem modification time, not event
timestamps or a transactional history store. Invalid reports may be skipped;
unmatched event logs become incomplete attempts. Inspection output is not redacted.
Resume reuses recorded arguments with selected fields reset, then delegates to
normal ingest; it does not restore an execution snapshot or guarantee unchanged
source bytes. Logs/reports are trusted local operational inputs, not confined or
authenticated declarations from an untrusted archive.
"""

from __future__ import annotations

import argparse
import json
import re

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.surfaces.cli.common import add_json_output, emit_json, load_json_file
from LiuXin_alpha.surfaces.cli.storage import (
    add_storage_ingest_arguments,
    cmd_storage_ingest,
)
from LiuXin_alpha.surfaces.system_profile import load_system_profile


_RUN_ID_PATTERN = re.compile(
    r"(?P<run>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)
_MAX_REPORT_BYTES = 128 * 1024 * 1024
_MAX_EVENT_LINE_BYTES = 4 * 1024 * 1024


def _log_directory(args: argparse.Namespace) -> Path:
    """
    Resolve an explicit ingest-log directory or require one from the selected profile.

    A truthy explicit directory bypasses profile loading. Otherwise normal explicit,
    environment, and persisted selection apply. Expand/resolve the chosen path and
    require an existing directory without creating it or restricting it to a root.

    Example:
        >>> _log_directory(argparse.Namespace(log_directory=str(Path.cwd()))) == Path.cwd().resolve()
        True


    :param args: Optional log_directory, system_root, and profile selectors.
    :return: Absolute resolved CLI-host directory to scan for run files.
    :raises ValueError: Profile loading fails or its log_directory is None/empty.
    :raises FileNotFoundError: The selected path is not an existing directory.
    """
    explicit = getattr(args, "log_directory", None)
    if explicit:
        path = Path(str(explicit)).expanduser().resolve(strict=False)
    else:
        resolved = load_system_profile(
            system_root=getattr(args, "system_root", None),
            profile=getattr(args, "profile", None),
            use_environment=True,
            required=True,
        )
        assert resolved is not None
        value = resolved.values.get("log_directory")
        if value in (None, ""):
            raise ValueError(
                "Selected LiuXin manifest has no log_directory; use --log-directory."
            )
        path = Path(str(value)).expanduser().resolve(strict=False)
    if not path.is_dir():
        raise FileNotFoundError("Ingest log directory does not exist: {!s}".format(path))
    return path


def _load_report(path: Path) -> dict[str, Any]:
    """
    Load one bounded JSON report and require a top-level mapping.

    The shared loader imposes a 128-MiB per-file limit. Stringify keys into a new
    shallow dictionary without validating report fields, status, or referenced paths.

    Example:
        >>> report = _load_report(Path("attempt.report.json"))  # doctest: +SKIP


    :param path: CLI-host report selector passed to the shared JSON file loader.
    :return: New string-key dictionary retaining decoded values by reference.
    :raises ValueError: JSON decoding/size checks fail or the value is not a mapping.
    """
    value = load_json_file(path, max_bytes=_MAX_REPORT_BYTES)
    if not isinstance(value, Mapping):
        raise ValueError("Ingest report must contain an object: {!s}".format(path))
    return {str(key): item for key, item in value.items()}


def _run_id_from_path(path: Path) -> str | None:
    """
    Extract the first UUID-shaped substring from a filename and canonicalize it.

    Match only the basename, without requiring boundaries, a particular prefix,
    extension, UUID version, or a corresponding report.

    Example:
        >>> _run_id_from_path(Path("attempt-AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA.jsonl"))
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        >>> _run_id_from_path(Path("ordinary.report.json")) is None
        True


    :param path: Path whose name may contain a run UUID.
    :return: Canonical lowercase hyphenated UUID, or None if no valid match exists.
    """
    match = _RUN_ID_PATTERN.search(path.name)
    if match is None:
        return None
    try:
        return str(UUID(match.group("run")))
    except ValueError:
        return None


def _attempts(directory: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Group direct-child report files and unmatched event logs into ordered attempts.

    Scan *.report.json first, skipping any load Exception and unparseable run IDs.
    A truthy report run_id takes precedence over filename extraction, even if that
    value is invalid. Keep decoded reports and selected status/path fields; stat
    failures after loading are not suppressed. No aggregate count/byte cap is added.

    Then scan *.jsonl whose resolved paths were not claimed by any loaded report,
    inferring incomplete attempts from filename UUIDs without reading their events.
    Report event-log paths resolve relative to the process directory, not this
    scan directory; they can refer outside it. Claimed paths suppress discovery
    globally across run groups. Each group's newest mtime_ns sorts first; ties
    retain discovery order. This is not an atomic or authenticated history snapshot.

    Example:
        >>> grouped = _attempts(ingest_log_directory)  # doctest: +SKIP


    :param directory: CLI-host directory to glob nonrecursively for attempts.
    :return: Canonical run UUIDs mapped to newest-first mutable attempt dictionaries,
        including raw report and internal modified_ns fields.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for report_path in directory.glob("*.report.json"):
        try:
            report = _load_report(report_path)
        except Exception:
            continue
        raw_run = report.get("run_id") or _run_id_from_path(report_path)
        try:
            run_id = str(UUID(str(raw_run)))
        except (TypeError, ValueError):
            continue
        grouped.setdefault(run_id, []).append(
            {
                "run_id": run_id,
                "status": report.get("status", "unknown"),
                "ok": report.get("ok"),
                "exit_code": report.get("exit_code"),
                "mode": report.get("mode"),
                "report_file": str(report_path.resolve(strict=False)),
                "event_log": report.get("event_log"),
                "human_log": report.get("human_log"),
                "modified_ns": report_path.stat().st_mtime_ns,
                "report": report,
            }
        )
    known_logs = {
        str(Path(str(attempt.get("event_log"))).resolve(strict=False))
        for values in grouped.values()
        for attempt in values
        if attempt.get("event_log")
    }
    for event_path in directory.glob("*.jsonl"):
        resolved_path = str(event_path.resolve(strict=False))
        if resolved_path in known_logs:
            continue
        run_id = _run_id_from_path(event_path)
        if run_id is None:
            continue
        grouped.setdefault(run_id, []).append(
            {
                "run_id": run_id,
                "status": "incomplete",
                "ok": False,
                "exit_code": None,
                "mode": None,
                "report_file": None,
                "event_log": resolved_path,
                "human_log": str(event_path.with_suffix(".log")),
                "modified_ns": event_path.stat().st_mtime_ns,
                "report": None,
            }
        )
    for values in grouped.values():
        values.sort(key=lambda item: int(item["modified_ns"]), reverse=True)
    return grouped


def _public_attempt(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """
    Shallow-copy an attempt while dropping only raw report and ordering timestamp.

    Other fields and nested values are retained; this is not credential redaction.

    Example:
        >>> _public_attempt({"status": "incomplete", "report": {}, "modified_ns": 7})
        {'status': 'incomplete'}


    :param attempt: Attempt mapping normally produced by the local history scanner.
    :return: New dict excluding exactly report and modified_ns keys.
    """
    return {
        key: value
        for key, value in attempt.items()
        if key not in {"report", "modified_ns"}
    }


def _find_run(args: argparse.Namespace) -> tuple[Path, list[dict[str, Any]]]:
    """
    Resolve the log directory and find all attempts for one canonicalized UUID.

    Directory/profile validation precedes run-ID parsing, and the whole directory
    is rescanned rather than looking up a single filename.

    Example:
        >>> directory, attempts = _find_run(parsed_run_show_args)  # doctest: +SKIP


    :param args: Location/profile selectors and run_id to stringify and parse as UUID.
    :return: Resolved directory and the selected newest-first attempt list.
    :raises ValueError: The supplied run ID is not a UUID, or selection is invalid.
    :raises FileNotFoundError: The log directory or a discoverable matching run is absent.
    """
    directory = _log_directory(args)
    try:
        run_id = str(UUID(str(args.run_id)))
    except ValueError as error:
        raise ValueError("RUN_ID must be a UUID.") from error
    attempts = _attempts(directory).get(run_id)
    if not attempts:
        raise FileNotFoundError(
            "No mixed-ingest run {} was found in {!s}.".format(run_id, directory)
        )
    return directory, attempts


def cmd_ingest_runs_list(args: argparse.Namespace) -> int:
    """
    Publish a paged newest-attempt projection for every discovered run.

    Discover/load all attempts before paging. Sort runs by their greatest observed
    mtime_ns, attach attempt counts, clamp offset to at least zero and limit to
    1..10,000, and omit raw reports/internal timestamps. Failed or incomplete runs
    do not turn listing into a failing command.

    Example:
        >>> cmd_ingest_runs_list(parsed_run_list_args)  # doctest: +SKIP


    :param args: Location selectors, integer-convertible offset/limit, and JSON controls.
    :return: Zero after publishing runs, paging metadata, and total run count.
    """
    directory = _log_directory(args)
    grouped = _attempts(directory)
    runs = []
    for run_id, attempts in grouped.items():
        latest = _public_attempt(attempts[0])
        latest["attempt_count"] = len(attempts)
        latest["run_id"] = run_id
        runs.append(latest)
    runs.sort(
        key=lambda value: max(
            int(item["modified_ns"]) for item in grouped[str(value["run_id"])]
        ),
        reverse=True,
    )
    offset = max(0, int(args.offset))
    limit = max(1, min(int(args.limit), 10000))
    selected = runs[offset : offset + limit]
    emit_json(
        {
            "log_directory": str(directory),
            "runs": selected,
            "count": len(selected),
            "total": len(runs),
            "offset": offset,
            "limit": limit,
            "has_more": offset + len(selected) < len(runs),
        },
        args,
    )
    return 0


def cmd_ingest_runs_show(args: argparse.Namespace) -> int:
    """
    Publish one run's ordered attempt summaries and the newest attempt's raw report.

    An incomplete newest event-log attempt can make report None even if an older
    attempt has a report. Raw report data is neither redacted nor success-filtered.

    Example:
        >>> cmd_ingest_runs_show(parsed_run_show_args)  # doctest: +SKIP


    :param args: Location selectors, run_id, and JSON output controls.
    :return: Zero after publication regardless of the run's recorded success/status.
    """
    directory, attempts = _find_run(args)
    latest = attempts[0]
    emit_json(
        {
            "log_directory": str(directory),
            "run_id": str(UUID(str(args.run_id))),
            "attempts": [_public_attempt(value) for value in attempts],
            "report": latest.get("report"),
        },
        args,
    )
    return 0


def _event_issues(path: Path, *, limit: int) -> list[dict[str, Any]]:
    """
    Read JSONL from the beginning and collect warning-level or named issue events.

    Limit each physical line, including its newline, to 4 MiB; an oversized line
    contributes a synthetic error record and stops the scan. Skip malformed UTF-8,
    JSON, and non-mapping events. Numeric/string levels are int-coerced, with
    TypeError/ValueError treated as zero; other errors such as overflow propagate.
    Recognize level >=30 or exact ingest_issue/cli_failed/cli_interrupted names.
    Values are not redacted, and neither total scanned bytes nor ordinary-line
    count is capped. A nonpositive limit still opens the file but reads no lines.

    Example:
        >>> issues = _event_issues(Path("attempt.jsonl"), limit=20)  # doctest: +SKIP


    :param path: CLI-host event log opened in binary mode.
    :param limit: Maximum returned issue records, including a synthetic size error.
    :return: New list of shallow event dictionaries and/or one oversized-line error.
    """
    issues: list[dict[str, Any]] = []
    with path.open("rb") as stream:
        while len(issues) < limit:
            line = stream.readline(_MAX_EVENT_LINE_BYTES + 1)
            if not line:
                break
            if len(line) > _MAX_EVENT_LINE_BYTES:
                issues.append(
                    {"error": "event line exceeds 4 MiB", "event_log": str(path)}
                )
                break
            try:
                event = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(event, Mapping):
                continue
            try:
                level_value = event.get("level", 0)
                if not isinstance(level_value, (str, int, float)):
                    raise TypeError
                level = int(level_value)
            except (TypeError, ValueError):
                level = 0
            context = event.get("context")
            event_name = context.get("event") if isinstance(context, Mapping) else None
            if level >= 30 or event_name in {"ingest_issue", "cli_failed", "cli_interrupted"}:
                issues.append(dict(event))
    return issues


def cmd_ingest_runs_issues(args: argparse.Namespace) -> int:
    """
    Collect bounded report and event-log issues from newest attempts first.

    Clamp limit to 1..10,000 and collect at most one extra item to report truncation.
    Within an attempt, report.error precedes nested report.issues, then event-log
    issues. Duplicate issues are not deduplicated. Existing referenced event logs
    are read as recorded, potentially outside the discovery directory; missing
    files are skipped, but read failures propagate. Issue contents are not redacted.

    Example:
        >>> cmd_ingest_runs_issues(parsed_run_issues_args)  # doctest: +SKIP


    :param args: Location selectors, run_id, integer-convertible limit, and JSON controls.
    :return: Zero if no issue was collected, otherwise one, after output.
    """
    _directory, attempts = _find_run(args)
    limit = max(1, min(int(args.limit), 10000))
    collection_limit = limit + 1
    collected: list[dict[str, Any]] = []
    for attempt in attempts:
        report = attempt.get("report")
        if isinstance(report, Mapping):
            if report.get("error") is not None:
                collected.append({"source": "report", "error": report["error"]})
            nested = report.get("report")
            if isinstance(nested, Mapping):
                raw_issues = nested.get("issues")
                if isinstance(raw_issues, list):
                    remaining = max(0, collection_limit - len(collected))
                    collected.extend(
                        {"source": "report", "issue": value}
                        for value in raw_issues[:remaining]
                    )
        event_log = attempt.get("event_log")
        if event_log and len(collected) < collection_limit:
            path = Path(str(event_log))
            if path.is_file():
                collected.extend(
                    {"source": "event_log", "event": value}
                    for value in _event_issues(
                        path, limit=collection_limit - len(collected)
                    )
                )
        if len(collected) >= collection_limit:
            break
    emit_json(
        {
            "run_id": str(UUID(str(args.run_id))),
            "issues": collected[:limit],
            "count": min(len(collected), limit),
            "truncated": len(collected) > limit,
        },
        args,
    )
    return 0 if not collected else 1


def _starting_event(path: Path) -> dict[str, Any]:
    """
    Find the first usable cli_started details mapping within 100 physical lines.

    Read from the beginning with a 4-MiB per-line limit. Invalid encoding/JSON,
    non-mappings, and start events without mapping details consume a line but are
    skipped. Return a shallow copy without validating the recorded resume fields.

    Example:
        >>> details = _starting_event(Path("attempt.jsonl"))  # doctest: +SKIP


    :param path: Existing CLI-host event log to inspect for original start details.
    :return: New dictionary from the first qualifying context.details mapping.
    :raises ValueError: A scanned line exceeds 4 MiB or no usable start appears
        before EOF/the 100-line boundary.
    """
    with path.open("rb") as stream:
        for _index in range(100):
            line = stream.readline(_MAX_EVENT_LINE_BYTES + 1)
            if not line:
                break
            if len(line) > _MAX_EVENT_LINE_BYTES:
                raise ValueError("Ingest event line exceeds the 4 MiB safety limit.")
            try:
                event = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(event, Mapping):
                continue
            context = event.get("context")
            if isinstance(context, Mapping) and context.get("event") == "cli_started":
                details = context.get("details")
                if isinstance(details, Mapping):
                    return dict(details)
    raise ValueError("Run has no usable cli_started event; it cannot be resumed safely.")


def _resume_namespace(args: argparse.Namespace, attempt: Mapping[str, Any]) -> argparse.Namespace:
    """
    Reconstruct an ingest namespace from a prior start event with output/state resets.

    Require an existing event log, a None/'ingest' start mode, and a truthy source
    root. Parse fresh ingest defaults, then replay known recorded argument keys
    except discovery/preflight/report destination/replacement/output selectors.
    Recorded values are assigned directly, without argparse conversion or schema
    validation. Unknown keys are ignored rather than made into new attributes.

    Force source/database/materialization values from top-level start details,
    log_directory from the recorded event path's parent, and the requested run UUID.
    Clear report destination/replacement, discovery/preflight, and root/profile;
    enable stdout reports. Other known recorded options remain. Referenced paths
    are not confined, source contents are not checked here, and this reconstructs
    a request rather than restoring an exact interrupted execution state.

    Example:
        >>> resumed = _resume_namespace(parsed_resume_args, latest_attempt)  # doctest: +SKIP


    :param args: Resume namespace supplying run_id; location discovery is already done.
    :param attempt: Selected attempt mapping with its event_log reference.
    :return: Fresh mutable ingest Namespace ready for cmd_storage_ingest validation.
    :raises FileNotFoundError: The selected attempt's event log is unavailable.
    :raises ValueError: Start details, mode, source, or run UUID are unusable.
    """
    event_log = attempt.get("event_log")
    if not event_log or not Path(str(event_log)).is_file():
        raise FileNotFoundError("Run event log is unavailable; exact resume is not possible.")
    started = _starting_event(Path(str(event_log)))
    if started.get("mode") not in {None, "ingest"}:
        raise ValueError(
            "Only real ingest attempts can be resumed; start event mode is {!r}."
            .format(started.get("mode"))
        )
    source_root = started.get("source_root")
    if not source_root:
        raise ValueError("Run start event does not record source_root.")
    parser = argparse.ArgumentParser(add_help=False)
    add_storage_ingest_arguments(parser)
    namespace = parser.parse_args(["--source-root", str(source_root)])
    recorded_args = started.get("arguments")
    if isinstance(recorded_args, Mapping):
        for key, value in recorded_args.items():
            if hasattr(namespace, str(key)) and key not in {
                "discover_only",
                "preflight_only",
                "report_file",
                "replace_report",
                "output",
            }:
                setattr(namespace, str(key), value)
    namespace.source_root = str(source_root)
    namespace.database = started.get("database")
    namespace.materialization_root = started.get("materialization_root")
    namespace.log_directory = str(Path(str(event_log)).parent)
    namespace.run_id = UUID(str(args.run_id))
    namespace.report_file = None
    namespace.replace_report = False
    namespace.no_stdout_report = False
    namespace.discover_only = False
    namespace.preflight_only = False
    namespace.system_root = None
    namespace.profile = None
    return namespace


def cmd_ingest_runs_resume(args: argparse.Namespace) -> int:
    """
    Reconstruct and run the newest attempt while guarding deliberate successful reruns.

    Reject recorded discovery/preflight modes before reconstruction. Only an exact
    complete status with truthy ok requires yes; failed/incomplete attempts do not.
    There is no fallback to an older resumable attempt if the newest is unusable.
    Delegate all execution and publication to normal storage ingest rather than
    emitting an additional JSON wrapper or taking a separate history lock here.

    Example:
        >>> cmd_ingest_runs_resume(parsed_resume_args)  # doctest: +SKIP


    :param args: Location selectors, run_id, and yes to allow a clean completed rerun.
    :return: The delegated cmd_storage_ingest exit status, with exceptions propagating.
    :raises ValueError: The latest attempt is not ingest, or clean completion lacks yes.
    """
    _directory, attempts = _find_run(args)
    latest = attempts[0]
    if latest.get("mode") not in {None, "ingest"}:
        raise ValueError(
            "Only real ingest attempts can be resumed; this run was {!r}."
            .format(latest.get("mode"))
        )
    if bool(latest.get("ok")) and str(latest.get("status")) == "complete" and not args.yes:
        raise ValueError(
            "The latest attempt completed cleanly; pass --yes to deliberately rerun it."
        )
    namespace = _resume_namespace(args, latest)
    return cmd_storage_ingest(namespace)


def _location_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Declare optional exclusive root, profile, or direct log-directory selectors.

    No explicit selection is required because profile fallback occurs at execution.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _location_arguments(parser)
        >>> parser.parse_args(["--log-directory", "logs"]).log_directory
        'logs'


    :param parser: Mutable run-command parser receiving the selector group.
    :return: None; no directory or profile is accessed during construction.
    """
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--system-root")
    group.add_argument("--profile")
    group.add_argument("--log-directory")


def build_ingest_runs_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register local run listing, inspection, issue collection, and resume leaves.

    List/show/issues receive JSON output controls; resume delegates normal ingest
    output and has only selectors, run ID, and completed-rerun confirmation here.
    UUID validation, paging clamps, discovery, and execution occur in handlers.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_ingest_runs_parser(parser.add_subparsers())
        >>> args = parser.parse_args(["runs", "list"])
        >>> args.limit, args.offset
        (100, 0)


    :param commands: Ingest-family subparser collection receiving the runs subtree.
    :return: None; four required action choices and handlers are installed in place.
    """
    runs = commands.add_parser(
        "runs", help="List, inspect, diagnose, or resume durable mixed-ingest runs."
    )
    subcommands = runs.add_subparsers(dest="ingest_runs_command", required=True)
    list_parser = subcommands.add_parser("list")
    _location_arguments(list_parser)
    list_parser.add_argument("--limit", type=int, default=100)
    list_parser.add_argument("--offset", type=int, default=0)
    add_json_output(list_parser)
    list_parser.set_defaults(handler=cmd_ingest_runs_list)

    show = subcommands.add_parser("show")
    _location_arguments(show)
    show.add_argument("run_id")
    add_json_output(show)
    show.set_defaults(handler=cmd_ingest_runs_show)

    issues = subcommands.add_parser("issues")
    _location_arguments(issues)
    issues.add_argument("run_id")
    issues.add_argument("--limit", type=int, default=1000)
    add_json_output(issues)
    issues.set_defaults(handler=cmd_ingest_runs_issues)

    resume = subcommands.add_parser("resume")
    _location_arguments(resume)
    resume.add_argument("run_id")
    resume.add_argument(
        "--yes",
        action="store_true",
        help="Allow an already-successful run to be deliberately rerun.",
    )
    resume.set_defaults(handler=cmd_ingest_runs_resume)


__all__ = [
    "build_ingest_runs_parser",
    "cmd_ingest_runs_issues",
    "cmd_ingest_runs_list",
    "cmd_ingest_runs_resume",
    "cmd_ingest_runs_show",
]
