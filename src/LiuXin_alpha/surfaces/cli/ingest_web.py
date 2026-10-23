"""Expose web registration, acquisition, inspection and exact saved-run replay."""

from __future__ import annotations

import argparse
from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_job_execution_arguments,
    add_json_output,
    emit_json,
    execution_exit_code,
    open_cli_core,
    submit_job,
)


def cmd_ingest_web(args: argparse.Namespace) -> int:
    """Route inspection to queries and acquisition/registration to the shared Core job owner."""
    payload = _request(args)
    with open_cli_core(args, enable_storage_manager=True) as core:
        if args.status:
            result = core.query("ingest.web.status", payload)
        elif args.action == "list":
            result = core.query("ingest.web.files", payload)
        else:
            result = submit_job(core, "ingest.web.start", payload, args)
    emit_json(result, args)
    code = execution_exit_code(result)
    execution = result.get("execution") if isinstance(result, dict) else None
    report = execution.get("result") if isinstance(execution, dict) else None
    if isinstance(report, dict) and report.get("status") in {
        "partial",
        "failed",
        "limited",
        "interrupted",
    }:
        return 1
    return code


_OPTION_NAMES = {
    "crawler": "crawler",
    "destination": "destination",
    "state_directory": "state_directory",
    "registered_only": "registered_only",
    "dry_run": "dry_run",
    "extension": "extensions",
    "include": "include",
    "max_depth": "max_depth",
    "max_pages": "max_pages",
    "max_files": "max_files",
    "requests_per_hour": "requests_per_hour",
    "request_timeout": "timeout_s",
    "max_seconds": "max_seconds",
}
_BYTE_OPTIONS = {
    "max_file_mib": "max_file_bytes",
    "max_total_mib": "max_total_bytes",
    "min_free_mib": "min_free_bytes",
}


def _request(args: argparse.Namespace) -> dict[str, Any]:
    """Validate routing before opening a local Core or remote connection."""
    if args.status or args.action == "list":
        if _controls(args):
            raise ValueError("inspection does not accept crawl/download controls")
        if args.status:
            if args.url:
                raise ValueError("--status takes a run ID, not a source URL")
            return {"run_id": args.status}
        if not args.url:
            raise ValueError("--list requires the source URL")
        return {"root_url": args.url}
    return _payload(args)


def _controls(args: argparse.Namespace) -> dict[str, Any]:
    """Pass only supplied options; the application owns every default."""
    values = {
        name: getattr(args, flag)
        for flag, name in _OPTION_NAMES.items()
        if getattr(args, flag) is not None and getattr(args, flag) is not False
    }
    values.update(
        {
            name: getattr(args, flag) * 1024 * 1024
            for flag, name in _BYTE_OPTIONS.items()
            if getattr(args, flag) is not None
        }
    )
    return values


def _payload(args: argparse.Namespace) -> dict[str, Any]:
    """Keep CLI spelling separate from backend-neutral persisted workflow controls."""
    controls = _controls(args)
    if args.resume:
        if args.url or controls:
            raise ValueError(
                "--resume replays saved options; omit the URL and crawl/download controls"
            )
        return {"resume": args.resume}
    if not args.url:
        raise ValueError("provide a web source URL or --resume RUN_ID")
    return {"options": {"root_url": args.url, "mode": args.action, **controls}}


def build_web_ingest_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Default to remote registration; retaining bytes is an explicit download action."""
    parser = subparsers.add_parser(
        "web",
        help="Register remote files or acquire selected copies into managed storage.",
        description="Walk a bounded HTTP(S) source. Registration records links without retaining ebook bytes; --download acquires verified local copies.",
        epilog="Examples: liuxin ingest web https://example.org/books/ --register-only\n"
        "          liuxin ingest web https://example.org/books/ --download --registered-only\n"
        "          liuxin ingest web --resume RUN_ID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_connection_arguments(parser)
    add_job_execution_arguments(parser)
    add_json_output(parser)
    parser.add_argument("url", nargs="?", help="Source URL on the Core host's network.")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument(
        "--register-only",
        dest="action",
        action="store_const",
        const="register",
        help="Record remote candidates without downloading their payloads (default).",
    )
    actions.add_argument(
        "--download",
        dest="action",
        action="store_const",
        const="download",
        help="Acquire selected files into the configured managed Store.",
    )
    actions.add_argument(
        "--list",
        dest="action",
        action="store_const",
        const="list",
        help="List recorded candidates, captures and saved runs without network access.",
    )
    actions.add_argument(
        "--resume",
        metavar="RUN_ID",
        help="Replay a saved run's exact options and skip completed captures.",
    )
    actions.add_argument(
        "--status",
        metavar="RUN_ID",
        help="Inspect saved run and per-file state without network access.",
    )
    parser.add_argument("--crawler", choices=("native", "wget"))
    parser.add_argument(
        "--destination",
        help="Writable managed Store UUID; otherwise use its configured default.",
    )
    parser.add_argument(
        "--state-directory",
        help="Core-host staging parent; defaults beside a SQLite catalogue.",
    )
    parser.add_argument(
        "--registered-only",
        action="store_true",
        help="Select from stored candidates without re-crawling the site.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover candidates without recording observations or acquiring files.",
    )
    parser.add_argument(
        "--extension",
        action="append",
        help="Accepted suffix; repeat as needed (default: known ebook suffixes excluding HTML navigation).",
    )
    parser.add_argument(
        "--include",
        action="append",
        help="Selected URL glob; quote it to avoid shell expansion. Repeat for alternatives.",
    )
    parser.add_argument("--max-depth", type=int)
    parser.add_argument("--max-pages", type=int, help="Native crawler page ceiling.")
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--max-file-mib", type=int)
    parser.add_argument(
        "--max-total-mib",
        type=int,
        help="Payload transfer ceiling across this run, including retries.",
    )
    parser.add_argument(
        "--min-free-mib",
        type=int,
        help="Required staging and destination headroom.",
    )
    parser.add_argument(
        "--requests-per-hour",
        type=float,
        help="Request pacing; defaults to the shared crawler preference.",
    )
    parser.add_argument("--request-timeout", type=float)
    parser.add_argument("--max-seconds", type=float)
    parser.set_defaults(action="register", handler=cmd_ingest_web)
