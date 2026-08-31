"""First-run catalogue and local storage initialisation."""

from __future__ import annotations

import argparse
import os
import sys

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_json_output,
    emit_bytes,
    emit_json,
    json_bytes,
    open_cli_core,
)
from LiuXin_alpha.surfaces.cli.postgres import (
    POSTGRES_DRIVER_INSTALL_HINT,
    cmd_postgres_init,
    configured_postgres_schema,
    configured_postgres_target,
    is_postgres_service_name,
    is_postgres_url,
    postgres_driver_is_available,
    redact_postgres_target,
    write_postgres_env_file,
)
from LiuXin_alpha.surfaces.system_profile import (
    SYSTEM_MANIFEST_FORMAT,
    SYSTEM_MANIFEST_NAME,
    SYSTEM_MANIFEST_VERSION,
)


class _WizardCancelled(Exception):
    """The operator deliberately left the interactive initializer."""


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _stdin_is_interactive() -> bool:
    try:
        return bool(sys.stdin.isatty())
    except Exception:
        return False


def _prompt_text(
    label: str,
    *,
    default: str | None = None,
    display_default: str | None = None,
) -> str:
    """Read one non-empty wizard value without ever displaying hidden defaults."""

    shown = display_default if display_default is not None else default
    suffix = "" if shown in (None, "") else " [{}]".format(shown)
    while True:
        try:
            value = input("{}{}: ".format(label, suffix)).strip()
        except (EOFError, KeyboardInterrupt) as error:
            raise _WizardCancelled from error
        if value:
            return value
        if default not in (None, ""):
            return str(default)
        print("A value is required.")


def _prompt_choice(
    label: str,
    choices: tuple[tuple[str, str], ...],
    *,
    default: int = 1,
) -> str:
    print(label)
    for index, (choice_label, _value) in enumerate(choices, start=1):
        marker = " (recommended)" if index == default else ""
        print("  {}) {}{}".format(index, choice_label, marker))
    aliases = {
        choice_label.casefold(): value
        for choice_label, value in choices
    }
    aliases.update({value.casefold(): value for _label, value in choices})
    while True:
        answer = _prompt_text("Choice", default=str(default))
        try:
            selected = int(answer)
        except ValueError:
            value = aliases.get(answer.casefold())
            if value is not None:
                return value
        else:
            if 1 <= selected <= len(choices):
                return choices[selected - 1][1]
        print("Choose a number from 1 to {}.".format(len(choices)))


def _prompt_yes_no(label: str, *, default: bool) -> bool:
    default_text = "Y/n" if default else "y/N"
    while True:
        try:
            answer = input("{} [{}]: ".format(label, default_text)).strip().casefold()
        except (EOFError, KeyboardInterrupt) as error:
            raise _WizardCancelled from error
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Answer yes or no.")


def _print_plan(title: str, entries: tuple[tuple[str, object], ...]) -> None:
    print("\n{}".format(title))
    for label, value in entries:
        print("  {}: {}".format(label, value))


def _run_path_wizard(args: argparse.Namespace, backend: str) -> int:
    system_root_text = _prompt_text(
        "LiuXin system root",
        default="./liuxin-system",
    )
    system_root = _resolved(system_root_text)
    configure_store = _prompt_yes_no(
        "Create and configure the primary filesystem Store?",
        default=True,
    )
    _print_plan(
        "Initialization plan",
        (
            ("database backend", backend),
            ("system root", system_root),
            ("catalogue", system_root / "catalogue.sqlite"),
            (
                "primary Store",
                system_root / "store" if configure_store else "not configured",
            ),
            ("existing data", "preserved"),
        ),
    )
    if not _prompt_yes_no("Apply this plan?", default=False):
        raise _WizardCancelled

    args.wizard = False
    args.system_root = str(system_root)
    args.database = None
    args.db_type = backend
    args.no_store = not configure_store
    args.store_root = None
    return cmd_init(args)


def _run_postgres_wizard(args: argparse.Namespace) -> int:
    if not postgres_driver_is_available():
        raise ValueError(POSTGRES_DRIVER_INSTALL_HINT)
    configured_target = configured_postgres_target()
    default_kind = 2 if configured_target.kind == "service" else 1
    target_kind = _prompt_choice(
        "How should LiuXin connect to PostgreSQL?",
        (
            ("PostgreSQL URL", "url"),
            ("PGSERVICE profile", "service"),
        ),
        default=default_kind,
    )
    url: str | None = None
    service: str | None = None
    if target_kind == "url":
        configured_url = (
            configured_target.value if configured_target.kind == "url" else None
        )
        while True:
            url = _prompt_text(
                "PostgreSQL URL (prefer .pgpass or a password manager)",
                default=configured_url or "postgresql://localhost/liuxin",
                display_default=(
                    None
                    if not configured_url
                    else redact_postgres_target(configured_target)
                ),
            )
            if is_postgres_url(url):
                break
            print("Enter a postgresql:// or postgres:// URL.")
    else:
        configured_service = (
            configured_target.value
            if configured_target.kind == "service"
            else None
        )
        while True:
            service = _prompt_text(
                "PGSERVICE profile name",
                default=configured_service or "liuxin",
            )
            if is_postgres_service_name(service):
                break
            print(
                "Use a PGSERVICE profile name containing only letters, numbers, "
                "dot, underscore, or hyphen."
            )
    schema = _prompt_text(
        "PostgreSQL schema",
        default=configured_postgres_schema(),
    )
    system_root = _resolved(
        _prompt_text(
            "LiuXin system root",
            default="./liuxin-system",
        )
    )
    save_environment = _prompt_yes_no(
        "Write a reusable mode-0600 connection environment file?",
        default=False,
    )
    environment_file: Path | None = None
    if save_environment:
        while True:
            environment_file = _resolved(
                _prompt_text(
                    "Environment file",
                    default="./liuxin-postgres.env",
                )
            )
            if not environment_file.exists():
                break
            if environment_file.is_dir():
                print("The environment-file path is a directory; choose a file.")
                continue
            if _prompt_yes_no(
                "Replace the existing environment file?",
                default=False,
            ):
                break

    redacted_target = redact_postgres_target(
        configured_postgres_target(
            explicit_url=url,
            explicit_service=service,
        )
    )
    _print_plan(
        "PostgreSQL initialization plan",
        (
            ("target", redacted_target),
            ("schema", schema),
            ("system root", system_root),
            ("system profile", system_root / SYSTEM_MANIFEST_NAME),
            ("action", "create/upgrade LiuXin tables, then run full readiness checks"),
            (
                "connection file",
                environment_file if environment_file is not None else "not written",
            ),
            ("password storage", "not added by the wizard"),
        ),
    )
    print(
        "  Note: the PostgreSQL database and login role must already exist; "
        "use `liuxin postgres setup-sql` when server-level provisioning is needed."
    )
    if not _prompt_yes_no("Apply this plan?", default=False):
        raise _WizardCancelled

    postgres_args = argparse.Namespace(
        url=url,
        service=service,
        schema=schema,
        password=None,
        no_password_prompt=False,
        check=True,
        json=False,
        connect_only=False,
        skip_core=False,
        skip_storage=False,
        skip_helpers=False,
        store_env_file=None,
        store_password=False,
        system_root=str(system_root),
    )
    result = cmd_postgres_init(postgres_args)
    if result != 0:
        return int(result)
    if environment_file is not None:
        written = write_postgres_env_file(
            environment_file,
            url=url,
            service=service,
            schema=schema,
            include_password=False,
        )
        print(
            "PostgreSQL connection environment written: {}".format(written),
            file=sys.stderr,
        )
    return 0


def cmd_init_wizard(args: argparse.Namespace) -> int:
    """Guide one interactive operator through a safe first initialization."""

    if args.system_root or args.database:
        raise ValueError("Do not combine --wizard with SYSTEM_ROOT or --database.")
    if not _stdin_is_interactive():
        raise ValueError(
            "The initialization wizard requires an interactive terminal. "
            "For automation, provide SYSTEM_ROOT/--database or use the "
            "explicit `liuxin postgres init` command."
        )

    print("LiuXin initialization wizard")
    print("No changes are made until you confirm the displayed plan.\n")
    try:
        backend = _prompt_choice(
            "Choose the catalogue database backend:",
            (
                ("SQLite (embedded, simplest)", "SQLite"),
                ("APSW SQLite (embedded, when installed)", "APSW"),
                ("PostgreSQL (server database)", "PostgreSQL"),
            ),
            default=1,
        )
        if backend == "PostgreSQL":
            return _run_postgres_wizard(args)
        return _run_path_wizard(args, backend)
    except _WizardCancelled:
        print("Initialization cancelled; no further changes were made.")
        return 1


def _ensure_directory(
    path: Path,
    *,
    create: bool,
    created: list[str],
) -> None:
    if path.exists():
        if not path.is_dir():
            raise ValueError("Expected a directory: {!s}".format(path))
        return
    if not create:
        raise FileNotFoundError(
            "Directory does not exist: {!s}; omit --no-create-directories."
            .format(path)
        )
    path.mkdir(parents=True, exist_ok=False)
    created.append(str(path))


def _layout(args: argparse.Namespace) -> dict[str, Any]:
    if args.system_root and args.database:
        raise ValueError("Provide SYSTEM_ROOT or --database, not both.")
    if not args.system_root and not args.database:
        raise ValueError(
            "Provide SYSTEM_ROOT or --database, or run `liuxin init --wizard` "
            "from an interactive terminal."
        )
    if args.no_store and args.store_root:
        raise ValueError("--no-store cannot be combined with --store-root.")
    if args.no_manifest and args.manifest:
        raise ValueError("--no-manifest cannot be combined with --manifest.")
    if str(args.db_type).strip().lower() not in {"sqlite", "apsw"}:
        raise ValueError(
            "`liuxin init` currently supports SQLite or APSW path-backed "
            "catalogues; use `liuxin init --wizard` or `liuxin postgres init` "
            "for PostgreSQL."
        )

    system_root = None if not args.system_root else _resolved(args.system_root)
    if system_root is not None:
        database = system_root / "catalogue.sqlite"
        store_root = (
            None
            if args.no_store
            else _resolved(args.store_root or (system_root / "store"))
        )
        materialization_root = system_root / "ingest-materialized"
        log_directory = system_root / "logs" / "ingest"
        manifest = _resolved(args.manifest or (system_root / SYSTEM_MANIFEST_NAME))
    else:
        database = _resolved(args.database)
        store_root = None if args.no_store or not args.store_root else _resolved(args.store_root)
        materialization_root = None
        log_directory = None
        manifest = None if not args.manifest else _resolved(args.manifest)

    if database.exists() and database.is_dir():
        raise ValueError("Database path is a directory: {!s}".format(database))
    if manifest is not None and manifest == database:
        raise ValueError("The system manifest and database paths must differ.")
    if getattr(args, "output", "-") != "-":
        output = _resolved(args.output)
        if output == database or (manifest is not None and output == manifest):
            raise ValueError(
                "JSON command output must not replace the database or system manifest."
            )
    if store_root is not None:
        try:
            database.relative_to(store_root)
        except ValueError:
            pass
        else:
            raise ValueError("The database must not be inside the managed Store.")
    return {
        "system_root": system_root,
        "database": database,
        "store_root": store_root,
        "materialization_root": materialization_root,
        "log_directory": log_directory,
        "manifest": manifest,
    }


def cmd_init(args: argparse.Namespace) -> int:
    wants_wizard = bool(args.wizard) or (
        not args.system_root and not args.database and _stdin_is_interactive()
    )
    if wants_wizard:
        return cmd_init_wizard(args)
    layout = _layout(args)
    database: Path = layout["database"]
    created_directories: list[str] = []
    create_directories = not bool(args.no_create_directories)

    if layout["system_root"] is not None:
        _ensure_directory(
            layout["system_root"],
            create=create_directories,
            created=created_directories,
        )
    _ensure_directory(
        database.parent,
        create=create_directories,
        created=created_directories,
    )
    for name in ("store_root", "materialization_root", "log_directory"):
        value = layout[name]
        if value is not None:
            _ensure_directory(
                value,
                create=create_directories,
                created=created_directories,
            )

    database_existed = os.path.isfile(database)
    args.database = str(database)
    args.core_endpoint = None
    # ``system_root`` has already been resolved into explicit init paths. Core
    # must see the resulting database transport, not try to resolve the
    # not-yet-written manifest while initialization is creating it.
    args.system_root = None
    if hasattr(args, "profile"):
        args.profile = None
    store_receipt: Any = None
    refresh_receipt: Any = None
    default_receipt: Any = None
    # The database layer's create path is for first-time schema creation, not
    # for opening an existing catalogue.  Re-entering it during an idempotent
    # ``init`` can rebuild bootstrap tables and discard Store registrations
    # made by later ingest runs.
    with open_cli_core(
        args,
        enable_storage_manager=True,
        create=not database_existed,
    ) as core:
        health = core.query("health", {})
        store_root = layout["store_root"]
        if store_root is not None:
            store_receipt = core.command(
                "storage.store.save",
                {
                    "store": {
                        "store_name": args.store_name,
                        "store_kind": args.store_kind,
                        "store_access_protocol": "file",
                        "store_root_uri": str(store_root),
                        "store_is_read_only": 0,
                        "store_online_status": "online",
                        "store_operational_role": "live",
                    }
                },
            )
            refresh_receipt = core.command(
                "storage.refresh",
                {
                    "startup_on_add": True,
                    "include_offline": False,
                    "clear_existing": True,
                    "strict": True,
                },
            )
            default_receipt = core.command(
                "storage.default.set", {"store": args.store_name}
            )
        stores = core.query("storage.stores.list", {"refresh": False})

    manifest_payload = {
        "format": SYSTEM_MANIFEST_FORMAT,
        "version": SYSTEM_MANIFEST_VERSION,
        "system_root": (
            None if layout["system_root"] is None else str(layout["system_root"])
        ),
        "database": str(database),
        "db_type": str(args.db_type),
        "store_root": (
            None if layout["store_root"] is None else str(layout["store_root"])
        ),
        "store_name": None if layout["store_root"] is None else args.store_name,
        "materialization_root": (
            None
            if layout["materialization_root"] is None
            else str(layout["materialization_root"])
        ),
        "log_directory": (
            None if layout["log_directory"] is None else str(layout["log_directory"])
        ),
    }
    manifest_path = layout["manifest"]
    if manifest_path is not None and not args.no_manifest:
        _ensure_directory(
            manifest_path.parent,
            create=create_directories,
            created=created_directories,
        )
        emit_bytes(
            json_bytes(manifest_payload),
            output=manifest_path,
            replace=True,
            mode=0o600,
        )

    if layout["system_root"] is not None:
        next_ingest = [
            "liuxin",
            "ingest",
            "/path/to/source",
            "--system-root",
            str(layout["system_root"]),
        ]
    else:
        next_ingest = [
            "liuxin",
            "ingest",
            "/path/to/source",
            "--database",
            str(database),
        ]
    health_summary = (
        {
            "ok": not bool(health.get("shutdown", False)),
            "core_uuid": health.get("core_uuid"),
            "core_version": health.get("core_version"),
            "api_version": health.get("api_version"),
        }
        if isinstance(health, Mapping)
        else {"ok": True}
    )
    refresh_report = (
        refresh_receipt.get("report")
        if isinstance(refresh_receipt, Mapping)
        else refresh_receipt
    )
    default_summary = (
        {
            key: default_receipt.get(key)
            for key in ("selected", "store_uuid", "store_name")
            if key in default_receipt
        }
        if isinstance(default_receipt, Mapping)
        else None
    )
    next_actions: dict[str, list[str]] = {"ingest": next_ingest}
    if manifest_path is not None and not args.no_manifest:
        next_actions["connect"] = (
            ["liuxin", "connect", str(layout["system_root"])]
            if layout["system_root"] is not None
            else ["liuxin", "connect", "--profile", str(manifest_path)]
        )
    result = {
        "ok": True,
        "database": str(database),
        "database_created": not database_existed,
        "db_type": str(args.db_type),
        "created_directories": created_directories,
        "manifest": (
            None
            if manifest_path is None or args.no_manifest
            else str(manifest_path)
        ),
        "health": health_summary,
        "store": (
            None
            if layout["store_root"] is None
            else {
                "name": args.store_name,
                "kind": args.store_kind,
                "root": str(layout["store_root"]),
                "saved": store_receipt is not None,
            }
        ),
        "storage_refresh": refresh_report,
        "default_store": default_summary,
        "store_count": (
            stores.get("count") if isinstance(stores, Mapping) else None
        ),
        "next": next_actions,
    }
    emit_json(result, args)
    return 0


def build_init_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "init",
        help="Create and validate a LiuXin catalogue and optional first Store.",
        description=(
            "Initialise a self-contained local system root, or an explicit "
            "path-backed catalogue. With no location in an interactive terminal, "
            "or with --wizard, choose SQLite/APSW/PostgreSQL through a guided "
            "setup. The operation is idempotent and never deletes an existing "
            "Store or catalogue."
        ),
    )
    parser.add_argument(
        "system_root",
        nargs="?",
        help=(
            "Local LiuXin root. Creates catalogue.sqlite, store/, "
            "ingest-materialized/, logs/, and liuxin-system.json."
        ),
    )
    parser.add_argument("--database", help="Explicit path-backed catalogue instead of SYSTEM_ROOT.")
    parser.add_argument("--db-type", default="SQLite")
    parser.add_argument(
        "--wizard",
        action="store_true",
        help=(
            "Interactively choose the database backend and location, display a "
            "redacted plan, then initialize and check it."
        ),
    )
    parser.add_argument("--store-root", help="Managed local Store root.")
    parser.add_argument("--store-name", default="primary")
    parser.add_argument("--store-kind", default="filesystem")
    parser.add_argument(
        "--no-store",
        action="store_true",
        help="Initialise only the catalogue; do not create or configure a Store.",
    )
    parser.add_argument(
        "--no-create-directories",
        action="store_true",
        help="Require every selected directory to exist already.",
    )
    parser.add_argument("--manifest", help="Override the system-manifest path.")
    parser.add_argument("--no-manifest", action="store_true")
    add_json_output(parser)
    parser.set_defaults(handler=cmd_init)


__all__ = [
    "SYSTEM_MANIFEST_FORMAT",
    "SYSTEM_MANIFEST_NAME",
    "SYSTEM_MANIFEST_VERSION",
    "build_init_parser",
    "cmd_init",
    "cmd_init_wizard",
]
