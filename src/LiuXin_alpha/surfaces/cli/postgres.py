from __future__ import annotations

import argparse
import json
import sys

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (
    format_postgres_self_test,
    run_postgres_self_test,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
    configured_postgres_password,
    configured_postgres_schema,
    configured_postgres_target,
    redact_postgres_target,
    write_postgres_env_file,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
    connect_postgres,
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


def _metadata_from_args(args: argparse.Namespace) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if getattr(args, "url", None):
        metadata["postgres_url"] = str(args.url)
    if getattr(args, "service", None):
        metadata["postgres_service"] = str(args.service)
    if getattr(args, "schema", None):
        metadata["schema"] = str(args.schema)
    return metadata


def cmd_postgres_check(args: argparse.Namespace) -> int:
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
    for statement in build_schema_statements(schema=str(args.schema)):
        print(statement.rstrip(";") + ";")
    return 0


def _check_passed(result: dict[str, object], name: str) -> bool:
    for check in result.get("checks") or []:
        if isinstance(check, dict) and check.get("name") == name:
            return bool(check.get("ok"))
    return False


def cmd_postgres_init(args: argparse.Namespace) -> int:
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
        return cmd_postgres_check(args)

    target = redact_postgres_target(configured_postgres_target(_metadata_from_args(args)))
    print("PostgreSQL schema initialised: target={}, schema={}".format(target, schema))
    return 0


def cmd_postgres_write_env(args: argparse.Namespace) -> int:
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
    for statement in build_runtime_grant_statements(
        role=str(args.role),
        schema=str(args.schema),
        database=str(args.database),
    ):
        print(statement.rstrip(";") + ";")
    return 0


def cmd_postgres_setup_sql(args: argparse.Namespace) -> int:
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
    text = str(statement).rstrip()
    if text.startswith("--"):
        print(text)
        return
    print(text.rstrip(";") + ";")


def cmd_postgres_grant_runtime_role(args: argparse.Namespace) -> int:
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


def build_postgres_parser(subparsers: argparse._SubParsersAction) -> None:
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
]
