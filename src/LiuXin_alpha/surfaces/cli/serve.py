"""
Launch packaged HTTP surfaces through a shared, loopback-guarded CLI facade.

Application modules are imported only after bind validation and argument
translation. They own server startup, shutdown, and further validation. The
unsafe-bind override supplies neither authentication nor TLS; Core RPC serving
is a separate command family. This facade forwards explicit database/endpoint
selectors, not system-root/profile selectors.
"""

from __future__ import annotations

import argparse
import importlib
import ipaddress
import sys

from LiuXin_alpha.surfaces.cli.common import add_connection_arguments


_SURFACES = {
    "web": "LiuXin_alpha.surfaces.web_readonly.app",
    "web-write": "LiuXin_alpha.surfaces.web_readwrite.app",
    "api": "LiuXin_alpha.surfaces.api_readonly.app",
    "opds": "LiuXin_alpha.surfaces.opds_readonly.app",
    "calibre": "LiuXin_alpha.surfaces.web_calibre_readonly.app",
}


def _is_loopback(host: str) -> bool:
    """
    Recognize localhost or a literal loopback address without DNS resolution.

    Normalization is used for classification only, not for the eventual bind.

    Example:
        >>> [_is_loopback(value) for value in (" LOCALHOST ", "::1", "0.0.0.0")]
        [True, True, False]


    :param host: Host value to stringify, strip, lowercase, and classify.
    :return: Whether the normalized value is localhost or a loopback IP literal.
    """
    token = str(host).strip().lower()
    if token == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(token).is_loopback)
    except ValueError:
        return False


def cmd_serve(args: argparse.Namespace) -> int:
    """
    Validate a surface bind and delegate to that application's CLI entrypoint.

    A truthy database takes precedence over the endpoint. System-root/profile
    selectors are neither resolved nor forwarded here. The original host is
    forwarded even though bind classification uses a normalized spelling.
    Database-path exposure applies only to web, web-write, and calibre; cache
    options omit web-write; the OPDS grouping limit applies only to opds/calibre.
    Other accepted but inapplicable flags are silently omitted.

    Print the security warning before importing the application. Import,
    application, output, and integer-conversion failures propagate, including
    the application's SystemExit; this adapter does not manage a server itself.

    Example:
        >>> cmd_serve(parsed_loopback_web_args)  # doctest: +SKIP


    :param args: Parsed serve leaf with explicit connection, bind, paging,
        cache, download, and unsafe-bind options.
    :return: The selected application's main result converted to an integer.
    :raises ValueError: A non-loopback bind lacks the explicit unsafe override.
    :raises KeyError: The supplied surface name is not registered.
    """
    if not _is_loopback(args.host) and not args.allow_unsafe_remote_bind:
        raise ValueError(
            "Refusing a non-loopback bind: this packaged surface has no "
            "built-in authentication or TLS. Use a protected reverse proxy, "
            "SSH tunnel, or pass --allow-unsafe-remote-bind after securing the boundary."
        )
    module_name = _SURFACES[args.serve_surface]
    argv: list[str] = []
    if args.database:
        argv.extend(("--database", args.database))
    else:
        argv.extend(("--core-endpoint", args.core_endpoint))
    argv.extend(("--core-timeout", str(args.core_timeout)))
    argv.extend(("--db-type", args.db_type))
    argv.extend(("--host", args.host))
    if args.port is not None:
        argv.extend(("--port", str(args.port)))
    if args.page_size is not None:
        argv.extend(("--page-size", str(args.page_size)))
    if args.max_page_size is not None:
        argv.extend(("--max-page-size", str(args.max_page_size)))
    if args.title:
        argv.extend(("--title", args.title))
    if args.no_file_downloads:
        argv.append("--no-file-downloads")
    if args.expose_database_path and args.serve_surface in {"web", "web-write", "calibre"}:
        argv.append("--expose-database-path")
    if args.serve_surface != "web-write":
        argv.extend(("--metadata-read-source", args.metadata_read_source))
        argv.extend(("--cache-type", args.cache_type))
        if args.no_cache_db_fallback:
            argv.append("--no-cache-db-fallback")
    if args.opds_max_ungrouped_items is not None and args.serve_surface in {"opds", "calibre"}:
        argv.extend(
            ("--opds-max-ungrouped-items", str(args.opds_max_ungrouped_items))
        )
    print(
        "This surface has no built-in authentication or TLS; keep it "
        "loopback-only or behind a protected boundary.",
        file=sys.stderr,
    )
    module = importlib.import_module(module_name)
    return int(module.main(argv))


def _surface_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    *,
    help_text: str,
) -> None:
    """
    Register one HTTP surface with common connection and presentation options.

    All surfaces accept the same flags, even those ignored for that application
    by cmd_serve. Defaults select loopback and database-backed metadata; numeric
    ranges, paths, authentication, and server availability are not checked here.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> _surface_parser(root.add_subparsers(), "web", help_text="Web UI")
        >>> root.parse_args(["web", "--database", "catalogue.sqlite"]).host
        '127.0.0.1'


    :param commands: Subparser collection to mutate with the named leaf.
    :param name: Surface token stored for cmd_serve's module lookup.
    :param help_text: Description displayed in the parent command's help.
    :return: None; the parser and its handler defaults are registered in place.
    """
    parser = commands.add_parser(name, help=help_text)
    add_connection_arguments(parser)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--page-size", type=int)
    parser.add_argument("--max-page-size", type=int)
    parser.add_argument("--title")
    parser.add_argument("--no-file-downloads", action="store_true")
    parser.add_argument("--expose-database-path", action="store_true")
    parser.add_argument(
        "--metadata-read-source", choices=("database", "cache"), default="database"
    )
    parser.add_argument("--cache-type", default="schema_backed")
    parser.add_argument("--no-cache-db-fallback", action="store_true")
    parser.add_argument("--opds-max-ungrouped-items", type=int)
    parser.add_argument(
        "--allow-unsafe-remote-bind",
        action="store_true",
        help="Acknowledge the lack of built-in authentication and TLS.",
    )
    parser.set_defaults(handler=cmd_serve, serve_surface=name)


def build_serve_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register required web, web-write, api, opds, and calibre serve subcommands.

    Parser construction imports no application and opens no listener. Core RPC
    is deliberately outside this family and is served through ``core serve``.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_serve_parser(root.add_subparsers())
        >>> root.parse_args(["serve", "opds", "--database", "db.sqlite"]).serve_surface
        'opds'


    :param subparsers: Root CLI subparser collection receiving the serve family.
    :return: None; all five leaves are installed with cmd_serve as their handler.
    """
    parser = subparsers.add_parser(
        "serve",
        help="Run packaged web, API, and OPDS surfaces with guarded binding.",
        description=(
            "Run packaged HTTP surfaces. They have no built-in authentication "
            "or TLS and default to loopback. Use `liuxin core serve` for the "
            "Core RPC transport."
        ),
    )
    commands = parser.add_subparsers(dest="serve_command", required=True)
    _surface_parser(commands, "web", help_text="Read-only LiuXin web UI.")
    _surface_parser(commands, "web-write", help_text="Read-write web UI (high trust only).")
    _surface_parser(commands, "api", help_text="Read-only JSON API.")
    _surface_parser(commands, "opds", help_text="Read-only OPDS feed.")
    _surface_parser(commands, "calibre", help_text="Calibre-compatible read-only web UI.")


__all__ = ["build_serve_parser"]
