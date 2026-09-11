"""
Expose managed-job inspection, waiting, log following, cancellation, and retry.

Commands use named Core operations with storage composition disabled. Wait
delegates one blocking query, watch polls for an execution result, and log
following polls both output and state. Their status policies are intentionally
different. JSON output is published after session exit; raw log output is emitted
incrementally and cannot be rolled back after a later failure.
"""

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
    """
    Compose a common CLI client context without requesting a storage manager.

    Example:
        >>> with _core(args) as core:  # doctest: +SKIP
        ...     jobs = core.query('jobs.list', {})


    :param args: Parsed database/profile/remote connection selectors.
    :return: Context manager yielding the selected Core client.
    """
    return open_cli_core(args, enable_storage_manager=False)


def cmd_jobs_list(args: argparse.Namespace) -> int:
    """
    Query a job page with integer paging and optional ordered state filters.

    Offset is always sent; limit is omitted only for None. State values are
    list-copied when truthy, without normalization/deduplication. Negative paging
    values are forwarded for Core to handle. Emit JSON after the session exits.

    Example:
        >>> status = cmd_jobs_list(args)  # doctest: +SKIP


    :param args: Connection/output namespace plus offset, optional limit, and state values.
    :return: Zero after a successful query and output, regardless of result count.
    """
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
    """
    Fetch one job by its unchanged selector and publish the returned metadata.

    A missing or failed job is not assigned a special status here; Core errors
    propagate and any successfully emitted response returns zero.

    Example:
        >>> status = cmd_jobs_show(args)  # doctest: +SKIP


    :param args: Connection/output namespace with job_id passed directly to jobs.get.
    :return: Zero after session exit and successful JSON publication.
    """
    with _core(args) as core:
        result = core.query("jobs.get", {"job_id": args.job_id})
    emit_json(result, args)
    return 0


def cmd_jobs_wait(args: argparse.Namespace) -> int:
    """
    Make one Core wait query and derive status from a returned dictionary job state.

    Forward a non-None timeout as float seconds without local polling. After
    output, inspect only dict-shaped result/job values and lowercase the state
    without stripping. Succeeded and unrecognized/nonterminal states return
    zero; other recognized terminal states return one. Zero therefore does not
    prove the job finished successfully when the Core wait expires.

    Example:
        >>> status = cmd_jobs_wait(args)  # doctest: +SKIP


    :param args: Connection/output options plus job_id and optional Core-side wait timeout.
    :return: One for a recognized nonsuccess terminal state, otherwise zero.
    """
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
    """
    Poll to terminal execution or local wait timeout, then emit the common result.

    Delegate polling and non-cancelling timeout reporting to wait_for_job. Exit
    the session before output and apply the common narrow execution-failure
    projection; this does not send a separate cancellation command.

    Example:
        >>> status = cmd_jobs_watch(args)  # doctest: +SKIP


    :param args: Connection/output options with job_id, timeout seconds, and poll_interval seconds.
    :return: Common execution_exit_code result after JSON publication.
    """
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
    """
    Request a job execution result with an optional Core-side timeout and publish it.

    This is one jobs.result query, not the watch polling loop. Timeout values
    are float-coerced without positivity checks; output/query errors propagate.

    Example:
        >>> status = cmd_jobs_result(args)  # doctest: +SKIP


    :param args: Connection/output namespace with job_id and optional timeout seconds.
    :return: Common narrow execution-failure status after the response is emitted.
    """
    payload: dict[str, Any] = {"job_id": args.job_id}
    if args.timeout is not None:
        payload["timeout_s"] = float(args.timeout)
    with _core(args) as core:
        result = core.query("jobs.result", payload)
    emit_json(result, args)
    return execution_exit_code(result)


def _read_log(core: Any, args: argparse.Namespace) -> dict[str, Any]:
    """
    Read one log page or accumulate pages while following job state and EOF.

    Start at integer offset and forward a per-query max_bytes, not a total
    output cap. Stringify text, retain all chunks in memory, and optionally
    write/flush each chunk to stdout immediately. Even raw mode accumulates the
    full returned text; a later error cannot retract already printed output.
    next_offset is trusted without monotonicity checks.

    Without follow, stop after the first log query and ignore timeout. Following
    reads job state after each page and stops for truthy EOF plus a recognized
    lowercased, untrimmed terminal state. Only dict status/job shapes are examined.
    That condition precedes the local monotonic deadline; timeout never cancels
    work or bounds an individual query. Poll sleeps are floored at 0.01 seconds.
    Returned EOF defaults true when absent, although absent EOF does not stop
    the follow loop; availability comes only from the last page.

    Example:
        >>> report = _read_log(core, args)  # doctest: +SKIP


    :param core: Client exposing jobs.log.read and, when following, jobs.get.
    :param args: Namespace with job_id, offset/max_bytes, raw/follow flags, timeout, and poll_interval.
    :return: Combined text and original/next offsets, final EOF/availability, and optional follow_timed_out.
    """
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
    """
    Read/follow logs, emitting raw chunks in-session or one JSON result afterward.

    Reject raw output with any destination other than the exact '-' spelling
    before opening Core. Raw mode bypasses JSON/output replacement settings;
    partial text can already be visible when a later query fails. Report status
    depends on local follow timeout, not whether the underlying job succeeded.

    Example:
        >>> status = cmd_jobs_logs(args)  # doctest: +SKIP


    :param args: Connection/output namespace with log paging, following, raw, and wait options.
    :return: One for a truthy follow_timed_out report flag, otherwise zero.
    :raises ValueError: Raw log text is combined with a non-stdout output destination.
    """
    if args.raw and args.output != "-":
        raise ValueError("--raw writes to stdout and cannot be combined with --output.")
    with _core(args) as core:
        result = _read_log(core, args)
    if not args.raw:
        emit_json(result, args)
    return 1 if bool(result.get("follow_timed_out", False)) else 0


def cmd_jobs_cancel(args: argparse.Namespace) -> int:
    """
    Request cancellation, publish its receipt, and inspect the cancelled flag.

    This handler does not wait for worker termination. Status projection occurs
    after output and requires a truthy cancelled value; absent values mean one.

    Example:
        >>> status = cmd_jobs_cancel(args)  # doctest: +SKIP


    :param args: Connection/output namespace with the target job_id.
    :return: Zero for a truthy cancellation receipt flag, otherwise one.
    """
    with _core(args) as core:
        result = core.command("jobs.cancel", {"job_id": args.job_id})
    emit_json(result, args)
    return 0 if bool(result.get("cancelled", False)) else 1


def cmd_jobs_retry(args: argparse.Namespace) -> int:
    """
    Request a linked retry with optional label and succeeded-job acknowledgement.

    Forward a truthy label unchanged and always include boolean allow_succeeded.
    Emit the receipt without validating a new ID or waiting for its execution.

    Example:
        >>> status = cmd_jobs_retry(args)  # doctest: +SKIP


    :param args: Connection/output options plus job_id, allow_succeeded, and optional label.
    :return: Zero after the retry receipt is published; errors propagate.
    """
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
    """
    Add common Core connection and JSON output options to a job-command parser.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _connection_json(parser)
        >>> parser.parse_args(['--database', 'library.sqlite']).output
        '-'


    :param parser: Leaf command parser to mutate without opening a connection.
    :return: None after registering both shared option groups.
    """
    add_connection_arguments(parser)
    add_json_output(parser)


def build_jobs_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register job list/show/wait/watch/result/log/cancel/retry commands and aliases.

    Every leaf receives connection and JSON controls. Show/get and logs/log share
    parser objects. Paging and timeout arguments are numerically parsed without
    range checks; raw/output incompatibility and operation-specific status policy
    are enforced only by handlers. No client or worker is started here.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_jobs_parser(root.add_subparsers())
        >>> args = root.parse_args(['jobs', 'log', '--database', 'library.sqlite', 'job-1'])
        >>> (args.max_bytes, args.follow, args.raw)
        (65536, False, False)


    :param subparsers: Parent argparse collection receiving the required jobs command family.
    :return: None after registering eight command parsers and their handler defaults.
    """
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
