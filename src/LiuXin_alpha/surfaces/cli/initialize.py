"""
Initialize local catalogue layouts or guide an operator through backend setup.

Path-backed initialization composes SQLite/APSW Core, registers an optional Store,
and replaces a mode-0600 manifest after closing the session. Existing database
files use the open path, not schema recreation. Interactive PostgreSQL setup
delegates to its separate initializer. These workflows are not all-or-nothing:
directories, database/Store changes, and manifests can survive later failures.
"""

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
    """
    Signal declined confirmation or interrupted input to the wizard's outer handler.

    The marker carries no rollback behavior; only the outer wizard translates it
    into a cancellation message and status one.

    Example:
        >>> isinstance(_WizardCancelled(), Exception)
        True
    """


def _resolved(path: str | Path) -> Path:
    """
    Expand a user path and resolve it absolutely without requiring existence.

    Existing symlinks and parent traversals are followed; no root confinement or
    file/directory validation is performed.

    Example:
        >>> _resolved(Path.cwd()) == Path.cwd().resolve()
        True


    :param path: String or Path naming a prospective CLI-host location.
    :return: Expanded, resolved absolute Path, possibly not yet present.
    """
    return Path(path).expanduser().resolve(strict=False)


def _stdin_is_interactive() -> bool:
    """
    Test standard input's TTY status, treating ordinary inspection errors as false.

    Example:
        >>> from unittest.mock import patch
        >>> with patch.object(sys, "stdin", object()):
        ...     _stdin_is_interactive()
        False


    :return: Truthiness of stdin.isatty(), or False when that call raises Exception.
    """
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
    """
    Prompt until a nonempty stripped answer or usable default is available.

    display_default replaces only the displayed suffix; the actual default is
    returned unchanged as text on blank input. An empty display_default hides
    the suffix, while None displays the actual default. This uses ordinary input,
    not password masking, and print/input errors other than EOF/interrupt escape.

    Example:
        >>> from unittest.mock import patch
        >>> with patch("builtins.input", return_value="  chosen  "):
        ...     _prompt_text("Location", default="fallback")
        'chosen'


    :param label: Prompt text before the optional default suffix and colon.
    :param default: Value returned on blank input unless None or the empty string.
    :param display_default: Alternate suffix text, or None to display default itself.
    :return: Stripped nonempty answer, or stringified default without stripping it.
    :raises _WizardCancelled: Input reaches EOF or the operator interrupts it.
    """

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
    """
    Display a numbered menu and accept an in-range number or casefolded alias.

    Both labels and values are aliases; later dictionary entries overwrite
    collisions. Integer-looking input uses only numeric selection, not alias
    fallback. The default is displayed as recommended but is not range-validated.

    Example:
        >>> from unittest.mock import patch
        >>> with patch("builtins.input", return_value="alpha"):
        ...     selected = _prompt_choice("Backend", (("Alpha", "a"), ("Beta", "b")))
        Backend
          1) Alpha (recommended)
          2) Beta
        >>> selected
        'a'


    :param label: Heading printed before the numbered choices.
    :param choices: Ordered (display label, returned value) string pairs.
    :param default: One-based choice number proposed on blank input.
    :return: The value associated with an accepted number, label, or value alias.
    :raises _WizardCancelled: The delegated text prompt reaches EOF or interrupt.
    """
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
    """
    Ask for case-insensitive yes/no, repeating invalid answers and honoring blank input.

    Example:
        >>> from unittest.mock import patch
        >>> with patch("builtins.input", return_value=" YES "):
        ...     _prompt_yes_no("Apply?", default=False)
        True


    :param label: Question printed with its Y/n or y/N default hint.
    :param default: Boolean returned unchanged for an empty stripped answer.
    :return: True for y/yes, False for n/no, or default for blank input.
    :raises _WizardCancelled: Input reaches EOF or is interrupted.
    """
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
    """
    Print a heading and ordered label/value plan without redacting its values.

    Callers must prepare safe display values before passing credential-bearing data.

    Example:
        >>> _print_plan("Plan", (("backend", "SQLite"),))
        <BLANKLINE>
        Plan
          backend: SQLite


    :param title: Heading preceded by a blank line.
    :param entries: Ordered labels and values interpolated through normal formatting.
    :return: None after printing the plan to stdout; output errors propagate.
    """
    print("\n{}".format(title))
    for label, value in entries:
        print("  {}: {}".format(label, value))


def _run_path_wizard(args: argparse.Namespace, backend: str) -> int:
    """
    Confirm a system-root layout and hand it to the noninteractive initializer.

    Prompt for root and whether to configure a primary filesystem Store, then show
    the plan. Only after confirmation mutate args to disable the wizard, select
    the resolved root/backend, and reset database/store-root choices. Other options
    remain in args and may still cause later initialization validation failures.

    Example:
        >>> _run_path_wizard(parsed_init_args, "SQLite")  # doctest: +SKIP


    :param args: Mutable init namespace reused by cmd_init after confirmation.
    :param backend: Chosen path-backed driver spelling, normally SQLite or APSW.
    :return: The delegated initializer's exit status.
    :raises _WizardCancelled: Input is interrupted or the operator declines the plan.
    """
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
    """
    Collect PostgreSQL connection choices and delegate confirmed schema initialization.

    Require an installed driver, prompt for URL/service and schema, then select
    system root and an optional environment-file path. Configured URL defaults and
    the plan use redacted displays, but typed input uses ordinary visible input.
    The database/login role must already exist; this does not provision the server.

    Confirmation constructs a separate PostgreSQL namespace with full readiness
    checks enabled and no explicit password. Only a zero initializer result permits
    writing the optional environment file, requesting include_password=False.
    Environment-file output or later stderr failures do not undo initialization.
    This helper itself does not create a layout before confirmation.

    Example:
        >>> _run_postgres_wizard(parsed_init_args)  # doctest: +SKIP


    :param args: Caller init namespace accepted for the wizard interface; its fields
        are not read or modified by this backend-specific path.
    :return: Nonzero initializer result converted to int, or zero after optional
        environment-file publication.
    :raises ValueError: The PostgreSQL driver is unavailable or delegated validation fails.
    :raises _WizardCancelled: Input is interrupted or the final plan is declined.
    """
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
    """
    Choose a backend interactively and translate deliberate wizard cancellation.

    Refuse explicit root/database selectors and non-TTY input before prompting.
    Dispatch PostgreSQL separately from path-backed backends. Catch only the
    wizard's cancellation marker; other initialization failures propagate. The
    cancellation message is not a promise to roll back work already delegated.

    Example:
        >>> cmd_init_wizard(parsed_empty_init_args)  # doctest: +SKIP


    :param args: Init namespace with no explicit system_root/database selection.
    :return: Delegated backend status, or one after cancellation is reported.
    :raises ValueError: An explicit location conflicts or stdin is not interactive.
    """

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
    """
    Accept an existing directory or create it and record only the requested path.

    Parent creation is recursive, but newly made ancestors are not listed separately.
    Existing permissions are not adjusted, and filesystem races/errors propagate.

    Example:
        >>> created = []
        >>> _ensure_directory(Path.cwd(), create=False, created=created)
        >>> created
        []


    :param path: CLI-host directory required by the planned layout.
    :param create: Permit mkdir for a path not observed to exist.
    :param created: Mutable receipt list extended with str(path) only after mkdir succeeds.
    :return: None when a directory already exists or creation succeeds.
    :raises ValueError: An existing path is not a directory.
    :raises FileNotFoundError: The directory is absent and creation is disabled.
    """
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
    """
    Resolve a local initialization layout and reject conflicting/unsafe path choices.

    Require exactly one root/database and SQLite/APSW, rejecting no-store/store-root
    and no-manifest/manifest conflicts. A system root supplies catalogue.sqlite,
    optional store, materialization, log, and default manifest paths. An explicit
    database supplies no auxiliary paths unless a Store or manifest is requested.

    Reject directory-valued databases, manifest/database equality, JSON-output
    equality with either, and databases lexically beneath the resolved Store root.
    These checks do not cover all hard-link aliases or concurrent path changes.
    A root's manifest path is still computed/checked when no_manifest suppresses
    later publication. Nothing is created here.

    Example:
        >>> layout = _layout(parsed_path_init_args)  # doctest: +SKIP
        >>> layout["database"].name  # doctest: +SKIP
        'catalogue.sqlite'


    :param args: Init location, driver, Store, manifest, and JSON output selections.
    :return: Dictionary of system_root/database/store_root/materialization_root/
        log_directory/manifest Paths, with None for unselected optional locations.
    :raises ValueError: Selectors conflict, the driver is unsupported, or protected
        path relationships would place the database at risk.
    """
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
    """
    Initialize a path-backed catalogue/layout, or dispatch interactive backend setup.

    Use the wizard when requested, or when no location is given and stdin is a TTY.
    Otherwise resolve layout and create required directories first. Mutate args to
    select the concrete database, clear Core endpoint/root/profile selectors, and
    compose storage-enabled Core. create is false for an observed existing file,
    avoiding the database schema-creation path on repeat initialization.

    Query health, optionally save/refresh/select a primary Store, and list Stores.
    After session exit, replace the selected manifest with mode-0600 output unless
    disabled, then publish the command report. No transaction spans these steps:
    directory/database/Store effects and manifest writes remain after later failure.
    Existing manifests are replaced, not merged with unknown fields.

    Result ok is always true if execution reaches publication; receipts are not
    generally inspected for failures. Health summary tests shutdown, not receipt.ok,
    and non-mapping health counts as ok. database_created reflects the earlier file
    check, and store.saved only tests that a receipt was non-None. Next-action
    tokens are suggestions, not executed commands.

    Example:
        >>> cmd_init(parsed_path_init_args)  # doctest: +SKIP


    :param args: Mutable init namespace containing layout, driver, wizard, creation,
        Store, manifest, and JSON publication controls.
    :return: Delegated wizard status, or zero after path-backed initialization/output;
        validation, Core, file, cleanup, and output failures otherwise propagate.
    """
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
    """
    Register first-run root/database, optional Store, manifest, and wizard options.

    Location is syntactically optional for interactive routing. Backend/path
    conflicts and suitability are validated by handlers, not during construction;
    default driver is SQLite and primary Store kind is filesystem.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_init_parser(parser.add_subparsers())
        >>> args = parser.parse_args(["init", "--database", "catalogue.sqlite", "--no-store"])
        >>> args.db_type, args.no_store
        ('SQLite', True)


    :param subparsers: Root CLI collection receiving the init leaf and handler.
    :return: None; all initialization options are declared without filesystem writes.
    """
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
