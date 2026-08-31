"""Core health, contract inspection, and guarded daemon serving."""

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
    from LiuXin_alpha.surfaces.cli.common import open_cli_core

    return open_cli_core(args, enable_storage_manager=True)


def cmd_core_health(args: argparse.Namespace) -> int:
    with _open(args) as core:
        result = core.query("health", {})
    emit_json(result, args)
    return 0 if bool(result.get("ok", True)) else 1


def cmd_core_capabilities(args: argparse.Namespace) -> int:
    with _open(args) as core:
        result = core.query("capabilities.list", {})
    emit_json(result, args)
    return 0


def cmd_core_api(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"include_targets": bool(args.include_targets)}
    if args.target:
        payload["target"] = args.target
    with _open(args) as core:
        result = core.query("api.describe", payload)
    emit_json(result, args)
    return 0


def _is_loopback_bind(host: str) -> bool:
    token = str(host).strip().lower()
    if token == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(token).is_loopback)
    except ValueError:
        return False


def cmd_core_serve(args: argparse.Namespace) -> int:
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
    add_connection_arguments(parser)
    add_json_output(parser)


def build_core_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
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
