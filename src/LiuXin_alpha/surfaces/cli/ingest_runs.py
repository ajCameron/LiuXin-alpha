"""First-class inspection and resumption of local mixed-ingest runs."""

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
    value = load_json_file(path, max_bytes=_MAX_REPORT_BYTES)
    if not isinstance(value, Mapping):
        raise ValueError("Ingest report must contain an object: {!s}".format(path))
    return {str(key): item for key, item in value.items()}


def _run_id_from_path(path: Path) -> str | None:
    match = _RUN_ID_PATTERN.search(path.name)
    if match is None:
        return None
    try:
        return str(UUID(match.group("run")))
    except ValueError:
        return None


def _attempts(directory: Path) -> dict[str, list[dict[str, Any]]]:
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
    return {
        key: value
        for key, value in attempt.items()
        if key not in {"report", "modified_ns"}
    }


def _find_run(args: argparse.Namespace) -> tuple[Path, list[dict[str, Any]]]:
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
    Execute the `ingest runs list` CLI command.


    :param args:
    :return:
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
    Execute the `ingest runs show` CLI command.


    :param args:
    :return:
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
    Execute the `ingest runs issues` CLI command.


    :param args:
    :return:
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
    Execute the `ingest runs resume` CLI command.


    :param args:
    :return:
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
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--system-root")
    group.add_argument("--profile")
    group.add_argument("--log-directory")


def build_ingest_runs_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Build the `ingest runs` command-line parser.


    :param commands:
    :return:
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
