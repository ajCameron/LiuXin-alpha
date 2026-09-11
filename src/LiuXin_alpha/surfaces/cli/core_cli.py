"""
Expose Core health/contracts and a guarded, locally owned HTTP daemon through the CLI.

Inspection commands borrow a composed client and publish JSON after session exit.
Daemon serving requires a local owned runtime and refuses non-loopback binding
unless explicitly acknowledged; that override adds neither authentication nor
TLS. Readiness output, signals, and shutdown belong to the serve handler, whose
cleanup guarantees begin after daemon construction.
"""

from __future__ import annotations

import argparse
import ipaddress
import signal
import sys
import threading

from contextlib import redirect_stdout
from pathlib import Path
from types import FrameType
from typing import Any

from LiuXin_alpha.core.transport.http import CoreHttpDaemon
from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    emit_bytes,
    emit_json,
    json_bytes,
)
from LiuXin_alpha.surfaces.core import open_surface_core_from_args


def _open(args: argparse.Namespace):
    """
    Lazily obtain the common client context with storage composition enabled.

    Example:
        >>> with _open(args) as core:  # doctest: +SKIP
        ...     result = core.query('health', {})


    :param args: Core inspection namespace containing source/profile/transport selectors.
    :return: Common CLI context manager yielding the selected Core client.
    """
    from LiuXin_alpha.surfaces.cli.common import open_cli_core

    return open_cli_core(args, enable_storage_manager=True)


def cmd_core_health(args: argparse.Namespace) -> int:
    """
    Query Core health, publish the response, and project its optional ok flag.

    Exit the client context before output. A missing ok key defaults to success;
    other values use Python truthiness. A malformed response can fail after JSON
    is already emitted because status projection follows publication.

    Example:
        >>> status = cmd_core_health(args)  # doctest: +SKIP


    :param args: Connection and JSON-output options for the health command.
    :return: Zero for truthy or absent ok, otherwise one.
    """
    with _open(args) as core:
        result = core.query("health", {})
    emit_json(result, args)
    return 0 if bool(result.get("ok", True)) else 1


def cmd_core_capabilities(args: argparse.Namespace) -> int:
    """
    Query named operation-family availability and emit the unmodified response.

    No response-specific success flag is interpreted; query/output errors propagate.

    Example:
        >>> status = cmd_core_capabilities(args)  # doctest: +SKIP


    :param args: Connection and JSON-output options for capability inspection.
    :return: Zero after session exit and successful response publication.
    """
    with _open(args) as core:
        result = core.query("capabilities.list", {})
    emit_json(result, args)
    return 0


def cmd_core_api(args: argparse.Namespace) -> int:
    """
    Request the machine-readable API contract with optional target selection.

    Always forward the truthiness of include_targets; include target only when
    truthy, without stripping or validating its name. Emit after session cleanup.

    Example:
        >>> status = cmd_core_api(args)  # doctest: +SKIP


    :param args: Connection/output namespace plus include_targets and optional target.
    :return: Zero after the api.describe response is successfully published.
    """
    payload: dict[str, Any] = {"include_targets": bool(args.include_targets)}
    if args.target:
        payload["target"] = args.target
    with _open(args) as core:
        result = core.query("api.describe", payload)
    emit_json(result, args)
    return 0


def _is_loopback_bind(host: str) -> bool:
    """
    Recognize localhost or an IP literal classified as loopback without DNS lookup.

    Strip and lowercase only for validation; the serving caller still forwards
    its original host string to the daemon. Other hostnames are not resolved.

    Example:
        >>> [_is_loopback_bind(host) for host in [' localhost ', '127.0.0.2', '::1', '0.0.0.0', 'example.invalid']]
        [True, True, True, False, False]


    :param host: Bind-host value stringified for hostname/IP classification.
    :return: True for localhost or a parsed loopback address, otherwise False.
    """
    token = str(host).strip().lower()
    if token == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(token).is_loopback)
    except ValueError:
        return False


def cmd_core_serve(args: argparse.Namespace) -> int:
    """
    Serve an owned local Core runtime until a stop signal or optional wait expires.

    Reject unacknowledged non-loopback binds and nonpositive request-size limits
    before composition. Redirect factory stdout to stderr, enable storage, and
    select maintenance from the flag. A borrowed/remote session is closed then
    rejected. MiB converts to integer bytes; no separate finite-number validation
    occurs. Daemon construction happens before the cleanup try block, so a
    construction failure is not covered by its session cleanup.

    After starting, emit readiness JSON and optionally a separately published
    ready file before installing signal handlers. Output failure can occur while
    the daemon is already running. SIGINT/SIGTERM handlers are installed only on
    the main thread and merely set the stop event. No stop_after means repeated
    half-second waits; otherwise wait once for a nonnegative duration or a signal.

    Finally restore recorded signal handlers, stop the daemon, and close the
    session in sequence. An earlier cleanup exception can prevent later cleanup
    and mask the body error. The remote-bind override never adds auth or TLS.

    Example:
        >>> status = cmd_core_serve(args)  # doctest: +SKIP


    :param args: Parsed serve connection, bind/namespace, size, readiness/output, signal-wait, and maintenance options.
    :return: Zero after normal stop waiting and successful cleanup; failures propagate.
    :raises ValueError: A non-loopback bind lacks acknowledgement or the size limit is nonpositive.
    :raises RuntimeError: The composed session lacks a locally owned runtime.
    """
    if not _is_loopback_bind(args.host) and not args.allow_unsafe_remote_bind:
        raise ValueError(
            "Refusing a non-loopback Core bind: this transport has no TLS or "
            "authentication. Use an SSH tunnel, or pass "
            "--allow-unsafe-remote-bind after protecting the network boundary."
        )
    if args.max_request_mib <= 0:
        raise ValueError("--max-request-mib must be greater than zero.")

    # A served Core must own its runtime. It cannot proxy another endpoint.
    with redirect_stdout(sys.stderr):
        session = open_surface_core_from_args(
            args,
            enable_storage_manager=True,
            enable_maintenance=bool(args.enable_maintenance),
        )
    if session.runtime is None or not session.owns_runtime:
        session.close()
        raise RuntimeError("Core serving requires a locally owned runtime.")

    daemon = CoreHttpDaemon(
        session.runtime,
        host=args.host,
        port=args.port,
        endpoint_namespace=args.namespace,
        max_request_bytes=int(float(args.max_request_mib) * 1024 * 1024),
    )
    stopped = threading.Event()
    previous: dict[int, Any] = {}

    def request_stop(_number: int, _frame: FrameType | None) -> None:
        """
        Notify the serving loop that shutdown was requested by a handled signal.

        Do not stop the daemon inside the signal callback; the outer finally
        block performs ordered cleanup after the wait returns.

        Example:
            >>> request_stop(signal.SIGTERM, None)  # doctest: +SKIP


        :param _number: Delivered signal number, deliberately unused.
        :param _frame: Interrupted execution frame, deliberately unused.
        :return: None after setting the enclosing stop event.
        """
        stopped.set()

    try:
        daemon.start()
        readiness = {
            "endpoint": daemon.base_url,
            "health_url": daemon.health_url,
            "bind": {"host": daemon.server_address[0], "port": daemon.server_address[1]},
            "authentication": False,
            "tls": False,
            "max_request_bytes": daemon.max_request_bytes,
        }
        emit_json(readiness, args)
        if args.ready_file:
            emit_bytes(
                json_bytes(readiness),
                output=Path(args.ready_file).expanduser(),
                replace=bool(args.replace_ready_file),
            )
        print(
            "Core transport has no authentication or TLS; keep it loopback-only "
            "or behind a protected tunnel.",
            file=sys.stderr,
        )
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGINT, signal.SIGTERM):
                previous[int(number)] = signal.getsignal(number)
                signal.signal(number, request_stop)
        if args.stop_after is None:
            while not stopped.wait(0.5):
                pass
        else:
            stopped.wait(max(0.0, float(args.stop_after)))
        return 0
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
        daemon.stop()
        session.close()


def _connection_json(parser: argparse.ArgumentParser) -> None:
    """
    Add common Core selectors and JSON destination controls to an inspection parser.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _connection_json(parser)
        >>> parser.parse_args(['--database', 'library.sqlite']).output
        '-'


    :param parser: Inspection-command parser to mutate without opening a session.
    :return: None after registering both shared argument groups.
    """
    add_connection_arguments(parser)
    add_json_output(parser)


def build_core_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register Core health, capabilities, API inspection, and guarded serving commands.

    Inspection leaves use common connection/JSON options. Serve requires an
    explicit local --database, exposes no remote-endpoint selector, and declares
    bind/namespace, request-size, maintenance, readiness, and stop-wait controls.
    Runtime ownership and unsafe-bind checks occur in the handler, not here.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_core_parser(root.add_subparsers())
        >>> args = root.parse_args(['core', 'serve', '--database', 'library.sqlite'])
        >>> (args.host, args.port, args.max_request_mib)
        ('127.0.0.1', 8765, 1024.0)


    :param subparsers: Parent argparse collection receiving the required core command family.
    :return: None; add four leaf parsers and bind their command handlers.
    """
    parser = subparsers.add_parser(
        "core", help="Inspect Core health/contracts or serve a guarded local daemon."
    )
    commands = parser.add_subparsers(dest="core_command", required=True)

    health = commands.add_parser("health", help="Check Core and database health.")
    _connection_json(health)
    health.set_defaults(handler=cmd_core_health)

    capabilities = commands.add_parser(
        "capabilities", help="Show stable operation-family availability."
    )
    _connection_json(capabilities)
    capabilities.set_defaults(handler=cmd_core_capabilities)

    api = commands.add_parser("api", help="Describe the stable Core API contract.")
    _connection_json(api)
    api.add_argument("--target", help="Describe one named operation.")
    api.add_argument(
        "--include-targets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include per-operation targets. Default: true",
    )
    api.set_defaults(handler=cmd_core_api)

    serve = commands.add_parser(
        "serve",
        help="Serve one local database through the Core HTTP transport.",
        description=(
            "Serve Core HTTP. The transport has no TLS or authentication and "
            "therefore defaults to loopback; use SSH tunnelling for remote use."
        ),
    )
    serve.add_argument("--database", required=True, help="Database path on this host.")
    serve.add_argument("--db-type", default="SQLite")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--namespace", default="")
    serve.add_argument("--max-request-mib", type=float, default=1024.0)
    serve.add_argument("--enable-maintenance", action="store_true")
    serve.add_argument(
        "--allow-unsafe-remote-bind",
        action="store_true",
        help="Acknowledge that a non-loopback bind has no built-in auth/TLS.",
    )
    serve.add_argument("--ready-file", help="Also atomically write readiness JSON here.")
    serve.add_argument("--replace-ready-file", action="store_true")
    serve.add_argument(
        "--stop-after",
        type=float,
        help="Stop after this many seconds (useful for probes and service tests).",
    )
    add_json_output(serve)
    serve.set_defaults(handler=cmd_core_serve)


__all__ = ["build_core_parser"]
