"""
Expose PostgreSQL readiness, schema/bootstrap SQL, grants, and connection exports.

SQL-printing commands do not connect; init and grant-runtime-role delegate actual
database operations. Environment-file exports preserve connection URLs, including
embedded credentials, even when a separate password variable is omitted. System
manifests instead remove URL userinfo passwords and selected secret-like query
keys. Neither diagnostic printing nor later file publication rolls back already
completed database changes or earlier output.
"""

from __future__ import annotations

import argparse
import json
import sys

from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
    format_postgres_self_test,
    run_postgres_self_test,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
    configured_postgres_password,
    configured_postgres_schema,
    configured_postgres_target,
    is_postgres_service_name,
    is_postgres_url,
    redact_postgres_target,
    write_postgres_env_file,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    POSTGRES_DRIVER_INSTALL_HINT,
    PostgresConnectionAdapter,
    connect_postgres,
    postgres_driver_is_available,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.runtime_privileges import (
    build_postgres_setup_statements,
    build_runtime_grant_statements,
    grant_runtime_role_privileges,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import (
    build_schema_statements,
    create_postgres_schema,
)
from LiuXin_alpha.surfaces.cli.common import emit_bytes, json_bytes
from LiuXin_alpha.surfaces.system_profile import (
    SYSTEM_MANIFEST_FORMAT,
    SYSTEM_MANIFEST_NAME,
    SYSTEM_MANIFEST_VERSION,
)


def _metadata_from_args(args: argparse.Namespace) -> dict[str, object]:
    """
    Project truthy URL, service, and schema arguments into backend metadata keys.

    No normalization, conflict resolution, password extraction, or redaction is
    performed; omitted attributes are treated as absent.

    Example:
        >>> _metadata_from_args(argparse.Namespace(service="books", schema="catalogue"))
        {'postgres_service': 'books', 'schema': 'catalogue'}


    :param args: Namespace optionally providing url, service, and schema.
    :return: New dictionary with supplied values stringified under backend keys.
    """
    metadata: dict[str, object] = {}
    if getattr(args, "url", None):
        metadata["postgres_url"] = str(args.url)
    if getattr(args, "service", None):
        metadata["postgres_service"] = str(args.service)
    if getattr(args, "schema", None):
        metadata["schema"] = str(args.schema)
    return metadata


def cmd_postgres_check(args: argparse.Namespace) -> int:
    """
    Run backend readiness checks, publish their report, and optionally export selection.

    Resolve the explicit/environment password through its helper. Connect-only
    disables all schema families; otherwise each skip flag controls one family.
    Print the checker-provided JSON or formatted text without an additional CLI
    sanitizer. If the first named configured check succeeds, optionally write an
    environment file even when overall readiness fails. store_password controls
    a separate password variable, not passwords already embedded in the URL.
    Report output precedes file writing; subsequent failures leave prior effects.

    Example:
        >>> cmd_postgres_check(parsed_postgres_check_args)  # doctest: +SKIP


    :param args: Connection/password selectors, schema-family controls, json,
        optional store_env_file, and store_password export policy.
    :return: Zero for truthy overall result.ok, otherwise two, after requested output.
    """
    check_schema = not bool(getattr(args, "connect_only", False))
    password = configured_postgres_password(getattr(args, "password", None))
    result = run_postgres_self_test(
        _metadata_from_args(args),
        postgres_url=args.url,
        postgres_service=args.service,
        password=password,
        prompt_for_password=not bool(args.no_password_prompt),
        check_core=check_schema and not bool(args.skip_core),
        check_storage=check_schema and not bool(args.skip_storage),
        check_helpers=check_schema and not bool(args.skip_helpers),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(format_postgres_self_test(result))
    if getattr(args, "store_env_file", None) and _check_passed(result, "configured"):
        path = write_postgres_env_file(
            args.store_env_file,
            url=args.url,
            service=args.service,
            password=password,
            include_password=bool(args.store_password),
            schema=configured_postgres_schema(_metadata_from_args(args)),
        )
        print("PostgreSQL env file written: path={}".format(path), file=sys.stderr)
    return 0 if result.get("ok") else 2


def cmd_postgres_schema_sql(args: argparse.Namespace) -> int:
    """
    Print generated schema statements without opening a PostgreSQL connection.

    Strip trailing semicolon characters, append one semicolon, and print each
    statement in generator order. This does not execute or validate server state.

    Example:
        >>> cmd_postgres_schema_sql(argparse.Namespace(schema="catalogue"))  # doctest: +SKIP


    :param args: Namespace with schema stringified for the schema SQL builder.
    :return: Zero after every statement is printed; builder/output failures propagate.
    """
    for statement in build_schema_statements(schema=str(args.schema)):
        print(statement.rstrip(";") + ";")
    return 0


def _check_passed(result: dict[str, object], name: str) -> bool:
    """
    Return the first exact-name check's truthiness status from a list-shaped report.

    Ignore non-dict entries; duplicate names do not override the first match.

    Example:
        >>> _check_passed({"checks": [{"name": "configured", "ok": False}, {"name": "configured", "ok": True}]}, "configured")
        False


    :param result: Report expected to contain checks as a list, not any iterable.
    :param name: Exact case-sensitive check name to find.
    :return: Boolean first-match ok, or False for absence/invalid list shape.
    """
    checks = result.get("checks")
    if not isinstance(checks, list):
        return False
    for check in checks:
        if isinstance(check, dict) and check.get("name") == name:
            return bool(check.get("ok"))
    return False


def cmd_postgres_init(args: argparse.Namespace) -> int:
    """
    Create/update the selected schema, close its connection, and optionally check readiness.

    Resolve schema and credentials, adapt the raw connection, and always attempt
    close after schema creation begins. Connection-construction failure precedes
    that cleanup block. With check enabled, delegate the normal checker and write
    a requested manifest only for status zero. Otherwise print a redacted target
    summary and optionally write the manifest. --json affects the checker path,
    not the unchecked success message. No wrapper rollback spans schema work,
    readiness, output, and manifest publication.

    Example:
        >>> cmd_postgres_init(parsed_postgres_init_args)  # doctest: +SKIP


    :param args: Connection/schema/password settings, check flag, optional system_root,
        and the checker's reporting/skip controls when readiness is requested.
    :return: Delegated checker status when enabled, otherwise zero after publication.
    """
    metadata = _metadata_from_args(args)
    schema = configured_postgres_schema(metadata)
    conn = connect_postgres(
        metadata,
        args.url,
        service=args.service,
        password=configured_postgres_password(getattr(args, "password", None)),
        prompt_for_password=not bool(args.no_password_prompt),
    )
    try:
        create_postgres_schema(PostgresConnectionAdapter(conn), schema=schema)
    finally:
        conn.close()

    if args.check:
        result = cmd_postgres_check(args)
        if result == 0 and getattr(args, "system_root", None):
            _write_postgres_system_manifest(args, schema=schema)
        return result

    target = redact_postgres_target(configured_postgres_target(_metadata_from_args(args)))
    print("PostgreSQL schema initialised: target={}, schema={}".format(target, schema))
    if getattr(args, "system_root", None):
        _write_postgres_system_manifest(args, schema=schema)
    return 0


def _write_postgres_system_manifest(
    args: argparse.Namespace,
    *,
    schema: str,
) -> Path:
    """
    Create a local deployment layout and replace its private PostgreSQL manifest.

    Create root/log/materialization directories before resolving a required target;
    failures can leave those directories. Prefer explicit target resolution, then
    metadata fallback. For URLs remove a userinfo password and query keys containing
    pass/password/token/secret/key, case-insensitively. Preserve username, path,
    fragment, and other query data: this is not comprehensive secret detection.
    Service selectors pass through unchanged.

    Replace the manifest with mode 0600 through shared byte publication, then print
    its path on stderr. Existing manifest fields are not merged; printing failure
    can occur after replacement. This function neither connects nor creates Stores.

    Example:
        >>> path = _write_postgres_system_manifest(args, schema="catalogue")  # doctest: +SKIP


    :param args: system_root and optional URL/service connection selectors.
    :param schema: Schema name stringified into database_metadata without server checks.
    :return: Absolute resolved path of the published system manifest.
    :raises ValueError: No target can be configured, or URL parsing fails.
    """
    root = Path(args.system_root).expanduser().resolve(strict=False)
    root.mkdir(parents=True, exist_ok=True)
    log_directory = root / "logs" / "ingest"
    materialization_root = root / "ingest-materialized"
    log_directory.mkdir(parents=True, exist_ok=True)
    materialization_root.mkdir(parents=True, exist_ok=True)
    target = configured_postgres_target(
        explicit_url=getattr(args, "url", None),
        explicit_service=getattr(args, "service", None),
    )
    if not target.configured:
        target = configured_postgres_target(_metadata_from_args(args))
    if not target.configured:
        raise ValueError("Cannot write a PostgreSQL profile without a connection target.")
    database_target = target.value
    if target.kind == "url":
        parsed = urlsplit(database_target)
        userinfo, separator, hostinfo = parsed.netloc.rpartition("@")
        netloc = parsed.netloc
        if separator and ":" in userinfo:
            username = userinfo.split(":", 1)[0]
            netloc = username + "@" + hostinfo
        safe_query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if not any(
                    marker in key.casefold()
                    for marker in ("pass", "password", "token", "secret", "key")
                )
            ],
            doseq=True,
        )
        database_target = urlunsplit(
            (parsed.scheme, netloc, parsed.path, safe_query, parsed.fragment)
        )
    manifest = {
        "format": SYSTEM_MANIFEST_FORMAT,
        "version": SYSTEM_MANIFEST_VERSION,
        "system_root": str(root),
        "database": database_target,
        "db_type": "PostgreSQL",
        "database_metadata": {"schema": str(schema)},
        "store_root": None,
        "store_name": None,
        "materialization_root": str(materialization_root),
        "log_directory": str(log_directory),
    }
    path = root / SYSTEM_MANIFEST_NAME
    emit_bytes(json_bytes(manifest), output=path, replace=True, mode=0o600)
    print("PostgreSQL LiuXin system manifest written: {}".format(path), file=sys.stderr)
    return path


def cmd_postgres_write_env(args: argparse.Namespace) -> int:
    """
    Write reusable shell connection exports and print a redacted target summary.

    Pass the explicit password or an empty string without independently resolving
    the environment password. include_password controls a separate export only;
    opting in without an explicit password emits an empty assignment even when
    an environment password exists. Credentials inside a URL remain in the file.
    The writer directly writes/replaces contents then chmods to 0600, rather than
    staging an atomic publication. A subsequent target-rendering/output failure does not
    undo the file write, and this command does not test database connectivity.

    Example:
        >>> cmd_postgres_write_env(parsed_postgres_write_env_args)  # doctest: +SKIP


    :param args: output, URL/service, schema, password, and include_password flag.
    :return: Zero after writing exports and printing their location/target summary.
    """
    path = write_postgres_env_file(
        args.output,
        url=args.url,
        service=args.service,
        password=args.password or "",
        include_password=bool(args.include_password),
        schema=args.schema,
    )
    print(
        "PostgreSQL env file written: path={}, target={}, includes_password={}".format(
            path,
            redact_postgres_target(configured_postgres_target(explicit_url=args.url, explicit_service=args.service)),
            bool(args.include_password),
        )
    )
    return 0


def cmd_postgres_grant_sql(args: argparse.Namespace) -> int:
    """
    Print runtime-role grant SQL for a database/schema without applying privileges.

    Role/database/schema are stringified; quoting and statement selection belong
    to the grant builder. Each statement receives a trailing semicolon.

    Example:
        >>> cmd_postgres_grant_sql(parsed_grant_sql_args)  # doctest: +SKIP


    :param args: Runtime role, target database, and schema names for SQL generation.
    :return: Zero after printing all statements, without opening a connection.
    """
    for statement in build_runtime_grant_statements(
        role=str(args.role),
        schema=str(args.schema),
        database=str(args.database),
    ):
        print(statement.rstrip(";") + ";")
    return 0


def cmd_postgres_setup_sql(args: argparse.Namespace) -> int:
    """
    Print administrator bootstrap SQL for selected server/database setup sections.

    Negative creation flags suppress role/database creation in the builder; schema
    and grant selection remain builder-owned. Comment lines use the dedicated
    printer. No SQL is executed, role created, or password provisioned here.

    Example:
        >>> cmd_postgres_setup_sql(parsed_setup_sql_args)  # doctest: +SKIP


    :param args: database, owner_role, runtime_role, schema, section, no_create_database,
        and no_create_roles options.
    :return: Zero after generated statements/comments are printed in order.
    """
    for statement in build_postgres_setup_statements(
        database=str(args.database),
        owner_role=str(args.owner_role),
        runtime_role=str(args.runtime_role),
        schema=str(args.schema),
        create_database=not bool(args.no_create_database),
        create_roles=not bool(args.no_create_roles),
        section=str(args.section),
    ):
        _print_sql_statement(statement)
    return 0


def _print_sql_statement(statement: str) -> None:
    """
    Print a SQL fragment with one terminator, except exact leading comment markers.

    Strip trailing whitespace first. Text beginning '--' remains unterminated;
    leading whitespace is not removed for that test, and empty text prints ';'.

    Example:
        >>> _print_sql_statement("SELECT 1;;; ")
        SELECT 1;
        >>> _print_sql_statement("-- explanatory comment ")
        -- explanatory comment


    :param statement: Fragment stringified and printed without parsing or execution.
    :return: None after stdout publication; output failures propagate.
    """
    text = str(statement).rstrip()
    if text.startswith("--"):
        print(text)
        return
    print(text.rstrip(";") + ";")


def cmd_postgres_grant_runtime_role(args: argparse.Namespace) -> int:
    """
    Apply runtime-role privileges through the backend helper and publish its receipt.

    Forward connection metadata, explicit URL/service, resolved password, and
    prompt policy. Unlike schema init, role/schema arguments are directly stringified
    here, including a None schema from direct/parser calls. The helper owns database
    execution and cleanup; this adapter adds no confirmation or rollback. JSON prints
    the complete receipt, while text requires role/database/schema keys. No receipt
    ok flag is inspected before returning success.

    Example:
        >>> cmd_postgres_grant_runtime_role(parsed_grant_role_args)  # doctest: +SKIP


    :param args: URL/service/schema/password, role, no_password_prompt, and json flag.
    :return: Zero after the delegated grant and output complete; failures raise.
    """
    result = grant_runtime_role_privileges(
        _metadata_from_args(args),
        args.url,
        service=args.service,
        role=str(args.role),
        schema=str(args.schema),
        password=configured_postgres_password(getattr(args, "password", None)),
        prompt_for_password=not bool(args.no_password_prompt),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "PostgreSQL runtime privileges granted: role={role}, database={database}, schema={schema}".format(
                **result
            )
        )
    return 0


def build_postgres_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Register PostgreSQL inspection, initialization, SQL generation, and export leaves.

    Connection-based commands permit selector omission for backend defaults;
    write-env instead requires exactly one explicit URL/service. SQL-only commands
    default schema to public; connection commands leave it None for later policy.
    Optional check environment output uses a fixed /tmp path when its value is
    omitted. Construction itself performs no connection, SQL, or filesystem write.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> build_postgres_parser(parser.add_subparsers())
        >>> args = parser.parse_args(["postgres", "check", "--connect-only"])
        >>> args.connect_only, args.schema
        (True, None)


    :param subparsers: Root CLI collection receiving required PostgreSQL action choices.
    :return: None; all leaves and handler defaults are registered in place.
    """
    parser = subparsers.add_parser(
        "postgres",
        help="PostgreSQL backend setup and readiness checks.",
    )
    postgres_subparsers = parser.add_subparsers(dest="postgres_command", required=True)

    check = postgres_subparsers.add_parser("check", help="Run PostgreSQL backend readiness checks.")
    _add_connection_args(check)
    check.add_argument("--connect-only", action="store_true", help="Only verify driver import, login, and identity.")
    check.add_argument("--skip-core", action="store_true", help="Skip WEMI/core table checks.")
    check.add_argument("--skip-storage", action="store_true", help="Skip storage table checks.")
    check.add_argument("--skip-helpers", action="store_true", help="Skip helper/workflow table checks.")
    check.add_argument(
        "--store-env-file",
        nargs="?",
        const="/tmp/liuxin-postgres.env",
        default=None,
        metavar="PATH",
        help="Write shell exports for later PostgreSQL commands. Default path when omitted: /tmp/liuxin-postgres.env.",
    )
    check.add_argument(
        "--store-password",
        action="store_true",
        help="With --store-env-file, also export LIUXIN_POSTGRES_PASSWORD from the current environment.",
    )
    check.add_argument("--json", action="store_true", help="Print the checker result as JSON.")
    check.set_defaults(handler=cmd_postgres_check)

    schema_sql = postgres_subparsers.add_parser("schema-sql", help="Print generated PostgreSQL schema SQL.")
    schema_sql.add_argument("--schema", default="public", help="PostgreSQL schema name for generated SQL.")
    schema_sql.set_defaults(handler=cmd_postgres_schema_sql)

    init = postgres_subparsers.add_parser("init", help="Create the LiuXin PostgreSQL schema.")
    _add_connection_args(init)
    init.add_argument("--check", action="store_true", help="Run readiness checks after creating schema.")
    init.add_argument(
        "--system-root",
        help="Write a reusable secret-free liuxin-system.json after successful setup.",
    )
    init.add_argument("--json", action="store_true", help="When used with --check, print JSON.")
    init.add_argument("--skip-core", action="store_true", help=argparse.SUPPRESS)
    init.add_argument("--skip-storage", action="store_true", help=argparse.SUPPRESS)
    init.add_argument("--skip-helpers", action="store_true", help=argparse.SUPPRESS)
    init.set_defaults(handler=cmd_postgres_init)

    write_env = postgres_subparsers.add_parser(
        "write-env",
        help="Write a shell env file for LiuXin PostgreSQL commands.",
    )
    write_target = write_env.add_mutually_exclusive_group(required=True)
    write_target.add_argument("--url", default=None, help="PostgreSQL URL to export.")
    write_target.add_argument("--service", default=None, help="PostgreSQL service profile to export.")
    write_env.add_argument("--output", required=True, help="Path to write shell exports to.")
    write_env.add_argument("--schema", default=None, help="Optional PostgreSQL schema to export.")
    write_env.add_argument(
        "--include-password",
        action="store_true",
        help="Also write LIUXIN_POSTGRES_PASSWORD. The file is still mode 0600.",
    )
    write_env.add_argument("--password", default="", help="Password value used only with --include-password.")
    write_env.set_defaults(handler=cmd_postgres_write_env)

    grant_sql = postgres_subparsers.add_parser(
        "grant-sql",
        help="Print SQL for granting LiuXin runtime role privileges.",
    )
    grant_sql.add_argument("--role", required=True, help="Runtime PostgreSQL role name.")
    grant_sql.add_argument("--database", required=True, help="Target database name.")
    grant_sql.add_argument("--schema", default="public", help="Target schema name.")
    grant_sql.set_defaults(handler=cmd_postgres_grant_sql)

    setup_sql = postgres_subparsers.add_parser(
        "setup-sql",
        help="Print admin SQL for creating PostgreSQL roles, database, schema, and runtime grants.",
    )
    setup_sql.add_argument("--database", required=True, help="Target database name.")
    setup_sql.add_argument("--owner-role", required=True, help="Schema/database owner role name.")
    setup_sql.add_argument("--runtime-role", required=True, help="Runtime role used by LiuXin.")
    setup_sql.add_argument("--schema", default="public", help="Target schema name.")
    setup_sql.add_argument(
        "--section",
        choices=("all", "server", "database"),
        default="all",
        help=(
            "Which setup section to print. Use server for role/database creation "
            "and database for schema/grants after connecting to the target database."
        ),
    )
    setup_sql.add_argument(
        "--no-create-database",
        action="store_true",
        help="Do not print CREATE DATABASE; useful when the database already exists.",
    )
    setup_sql.add_argument(
        "--no-create-roles",
        action="store_true",
        help="Do not print role creation blocks; useful when roles already exist.",
    )
    setup_sql.set_defaults(handler=cmd_postgres_setup_sql)

    grant_role = postgres_subparsers.add_parser(
        "grant-runtime-role",
        help="Connect and grant LiuXin runtime privileges to a PostgreSQL role.",
    )
    _add_connection_args(grant_role)
    grant_role.add_argument("--role", required=True, help="Runtime PostgreSQL role name.")
    grant_role.add_argument("--json", action="store_true", help="Print grant result as JSON.")
    grant_role.set_defaults(handler=cmd_postgres_grant_runtime_role)


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    """
    Declare optional exclusive URL/service and schema/password/prompt controls.

    Defaults remain None rather than resolving environment or validating targets
    during parser construction. Ordinary argv passwords are not hidden by argparse.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _add_connection_args(parser)
        >>> parser.parse_args(["--service", "books"]).service
        'books'


    :param parser: Mutable PostgreSQL command leaf receiving the connection options.
    :return: None; no credentials are loaded and no connection is attempted.
    """
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--url",
        default=None,
        help="PostgreSQL URL. Defaults to LIUXIN_POSTGRES_URL/LIUXIN_DATABASE_URL.",
    )
    target.add_argument(
        "--service",
        default=None,
        help="PostgreSQL service profile. Defaults to LIUXIN_POSTGRES_SERVICE/PGSERVICE.",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="PostgreSQL schema name. Defaults to LIUXIN_POSTGRES_SCHEMA or public.",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Password for this command only. Prefer .pgpass, PGSERVICE, or LIUXIN_POSTGRES_PASSWORD.",
    )
    parser.add_argument(
        "--no-password-prompt",
        action="store_true",
        help="Do not prompt interactively for a missing PostgreSQL password.",
    )


__all__ = [
    "build_postgres_parser",
    "cmd_postgres_check",
    "cmd_postgres_grant_runtime_role",
    "cmd_postgres_grant_sql",
    "cmd_postgres_init",
    "cmd_postgres_schema_sql",
    "cmd_postgres_setup_sql",
    "cmd_postgres_write_env",
    "POSTGRES_DRIVER_INSTALL_HINT",
    "configured_postgres_schema",
    "configured_postgres_target",
    "is_postgres_service_name",
    "is_postgres_url",
    "postgres_driver_is_available",
    "redact_postgres_target",
    "write_postgres_env_file",
]
