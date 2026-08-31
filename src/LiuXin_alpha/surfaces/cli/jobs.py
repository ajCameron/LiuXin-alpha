"""Operator-facing managed-job commands."""

from __future__ import annotations

import argparse
import sys
import time

from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    TERMINAL_JOB_STATES,
    add_connection_arguments,
    add_json_output,
    emit_bytes,
    emit_json,
    execution_exit_code,
    open_cli_core,
    wait_for_job,
)


def _core(args: argparse.Namespace):
    return open_cli_core(args, enable_storage_manager=False)


def cmd_jobs_list(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"offset": int(args.offset)}
    if args.state:
        payload["states"] = list(args.state)
    if args.limit is not None:
        payload["limit"] = int(args.limit)
    with _core(args) as core:
        result = core.query("jobs.list", payload)
    emit_json(result, args)
    return 0


def cmd_jobs_show(args: argparse.Namespace) -> int:
    with _core(args) as core:
        result = core.query("jobs.get", {"job_id": args.job_id})
    emit_json(result, args)
    return 0


def cmd_jobs_wait(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"job_id": args.job_id}
    if args.timeout is not None:
        payload["timeout_s"] = float(args.timeout)
    with _core(args) as core:
        result = core.query("jobs.wait", payload)
    emit_json(result, args)
    job = result.get("job", {}) if isinstance(result, dict) else {}
    state = str(job.get("state", "")).lower() if isinstance(job, dict) else ""
    return 0 if state == "succeeded" else (1 if state in TERMINAL_JOB_STATES else 0)


def cmd_jobs_watch(args: argparse.Namespace) -> int:
    with _core(args) as core:
        result = wait_for_job(
            core,
            args.job_id,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
        )
    emit_json(result, args)
    return execution_exit_code(result)


def cmd_jobs_result(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"job_id": args.job_id}
    if args.timeout is not None:
        payload["timeout_s"] = float(args.timeout)
    with _core(args) as core:
        result = core.query("jobs.result", payload)
    emit_json(result, args)
    return execution_exit_code(result)


def _read_log(core: Any, args: argparse.Namespace) -> dict[str, Any]:
    offset = int(args.offset)
    chunks: list[str] = []
    last: dict[str, Any] = {}
    started = time.monotonic()
    while True:
        last = dict(
            core.query(
                "jobs.log.read",
                {
                    "job_id": args.job_id,
                    "offset": offset,
                    "max_bytes": int(args.max_bytes),
                },
            )
        )
        text = str(last.get("text", ""))
        if text:
            chunks.append(text)
            if args.raw:
                sys.stdout.write(text)
                sys.stdout.flush()
        offset = int(last.get("next_offset", offset))
        if not args.follow:
            break
        status = core.query("jobs.get", {"job_id": args.job_id})
        job = status.get("job", {}) if isinstance(status, dict) else {}
        state = str(job.get("state", "")).lower() if isinstance(job, dict) else ""
        if bool(last.get("eof", False)) and state in TERMINAL_JOB_STATES:
            break
        if args.timeout is not None and time.monotonic() - started >= args.timeout:
            last["follow_timed_out"] = True
            break
        time.sleep(max(0.01, float(args.poll_interval)))
    return {
        "job_id": args.job_id,
        "text": "".join(chunks),
        "offset": int(args.offset),
        "next_offset": offset,
        "eof": bool(last.get("eof", True)),
        "available": bool(last.get("available", False)),
        **(
            {"follow_timed_out": True}
            if bool(last.get("follow_timed_out", False))
            else {}
        ),
    }


def cmd_jobs_logs(args: argparse.Namespace) -> int:
    if args.raw and args.output != "-":
        raise ValueError("--raw writes to stdout and cannot be combined with --output.")
    with _core(args) as core:
        result = _read_log(core, args)
    if not args.raw:
        emit_json(result, args)
    return 1 if bool(result.get("follow_timed_out", False)) else 0


def cmd_jobs_cancel(args: argparse.Namespace) -> int:
    with _core(args) as core:
        result = core.command("jobs.cancel", {"job_id": args.job_id})
    emit_json(result, args)
    return 0 if bool(result.get("cancelled", False)) else 1


def cmd_jobs_retry(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "job_id": args.job_id,
        "allow_succeeded": bool(args.allow_succeeded),
    }
    if args.label:
        payload["label"] = args.label
    with _core(args) as core:
        result = core.command("jobs.retry", payload)
    emit_json(result, args)
    return 0


def _connection_json(parser: argparse.ArgumentParser) -> None:
    add_connection_arguments(parser)
    add_json_output(parser)


def build_jobs_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "jobs",
        help="List, inspect, follow, retrieve, retry, and cancel managed jobs.",
    )
    commands = parser.add_subparsers(dest="jobs_command", required=True)

    list_parser = commands.add_parser("list", help="List managed jobs.")
    _connection_json(list_parser)
    list_parser.add_argument(
        "--state",
        action="append",
        help="Filter by state; repeat for more than one state.",
    )
    list_parser.add_argument("--limit", type=int, default=100)
    list_parser.add_argument("--offset", type=int, default=0)
    list_parser.set_defaults(handler=cmd_jobs_list)

    show = commands.add_parser("show", aliases=["get"], help="Show one job.")
    _connection_json(show)
    show.add_argument("job_id")
    show.set_defaults(handler=cmd_jobs_show)

    wait = commands.add_parser(
        "wait", help="Wait through Core for a job to become terminal."
    )
    _connection_json(wait)
    wait.add_argument("job_id")
    wait.add_argument("--timeout", type=float)
    wait.set_defaults(handler=cmd_jobs_wait)

    watch = commands.add_parser(
        "watch", help="Poll a job and return its terminal execution result."
    )
    _connection_json(watch)
    watch.add_argument("job_id")
    watch.add_argument("--timeout", type=float)
    watch.add_argument("--poll-interval", type=float, default=0.25)
    watch.set_defaults(handler=cmd_jobs_watch)

    result = commands.add_parser("result", help="Retrieve a job execution result.")
    _connection_json(result)
    result.add_argument("job_id")
    result.add_argument("--timeout", type=float)
    result.set_defaults(handler=cmd_jobs_result)

    logs = commands.add_parser(
        "logs", aliases=["log"], help="Read or follow captured job output."
    )
    _connection_json(logs)
    logs.add_argument("job_id")
    logs.add_argument("--offset", type=int, default=0)
    logs.add_argument("--max-bytes", type=int, default=64 * 1024)
    logs.add_argument("--follow", action="store_true")
    logs.add_argument("--timeout", type=float)
    logs.add_argument("--poll-interval", type=float, default=0.25)
    logs.add_argument(
        "--raw", action="store_true", help="Write only decoded log text to stdout."
    )
    logs.set_defaults(handler=cmd_jobs_logs)

    cancel = commands.add_parser("cancel", help="Request cancellation of one job.")
    _connection_json(cancel)
    cancel.add_argument("job_id")
    cancel.set_defaults(handler=cmd_jobs_cancel)

    retry = commands.add_parser(
        "retry", help="Replay one terminal job as a new linked run."
    )
    _connection_json(retry)
    retry.add_argument("job_id")
    retry.add_argument("--label", help="Optional label for the new run.")
    retry.add_argument(
        "--allow-succeeded",
        action="store_true",
        help="Permit deliberate replay of a job that already succeeded.",
    )
    retry.set_defaults(handler=cmd_jobs_retry)


__all__ = ["build_jobs_parser"]
