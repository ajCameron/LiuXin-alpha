#!/usr/bin/env python3
"""Run a live PostgreSQL smoke test for the LiuXin backend.

PostgreSQL is intentionally external to this script. Provide a DSN with
``--url`` or ``LIUXIN_POSTGRES_URL``. By default the script creates a disposable
schema, initializes LiuXin tables there, runs the strict checker, exercises a
small driver CRUD path, and drops the schema at the end.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
import uuid

from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.checker import (  # noqa: E402
    format_postgres_self_test,
    run_postgres_self_test,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (  # noqa: E402
    configured_postgres_password,
    configured_postgres_schema,
    configured_postgres_target,
    redact_postgres_target,
    store_postgres_password,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (  # noqa: E402
    PostgresConnectionAdapter,
    connect_postgres,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (  # noqa: E402
    DatabaseDriver,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.schema import (  # noqa: E402
    create_postgres_schema,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripts/run_postgres_live_smoke.py",
        description="Run a live LiuXin PostgreSQL backend smoke test against a configured target.",
    )
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
        "--env-file",
        type=Path,
        default=None,
        help="Optional shell env file from `postgres check --store-env-file` or `postgres write-env`.",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema to use. Default: generated disposable schema unless --use-configured-schema is passed.",
    )
    parser.add_argument(
        "--use-configured-schema",
        action="store_true",
        help="Use LIUXIN_POSTGRES_SCHEMA/metadata default instead of generating a disposable schema.",
    )
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not create/update the LiuXin schema.")
    parser.add_argument("--skip-crud", action="store_true", help="Skip the driver CRUD probe after strict checks.")
    parser.add_argument("--keep-schema", action="store_true", help="Keep the generated disposable schema.")
    parser.add_argument(
        "--drop-schema",
        action="store_true",
        help="Drop the named schema at the end. Refuses to drop public.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON summary.")
    parser.add_argument(
        "--password",
        default=None,
        help="Password for this smoke run only. Prefer .pgpass, PGSERVICE, or LIUXIN_POSTGRES_PASSWORD.",
    )
    parser.add_argument(
        "--no-password-prompt",
        action="store_true",
        help="Do not prompt interactively for a missing PostgreSQL password.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file is not None:
        _load_env_file(args.env_file)

    target = configured_postgres_target(explicit_url=args.url, explicit_service=args.service)
    password = _password_for_run(args)
    schema, generated_schema = _resolve_schema(args)
    should_drop_schema = _should_drop_schema(args, schema=schema, generated_schema=generated_schema)
    metadata = _metadata_for_target(target, schema=schema)
    summary: dict[str, Any] = {
        "backend": "postgresql",
        "url": redact_postgres_target(target),
        "target_kind": target.kind,
        "schema": schema,
        "generated_schema": generated_schema,
        "steps": [],
        "ok": False,
    }

    if not target.configured:
        _record(summary, "configured", False, "No PostgreSQL URL or service profile configured.")
        _emit_summary(summary, json_output=args.json)
        return 2

    try:
        login_result = run_postgres_self_test(
            metadata,
            postgres_url=args.url,
            postgres_service=args.service,
            password=password,
            prompt_for_password=not bool(args.no_password_prompt),
            check_core=False,
            check_storage=False,
            check_helpers=False,
        )
        _record(summary, "login", bool(login_result.get("ok")), "login/identity check", login_result)
        if not args.json:
            print(format_postgres_self_test(login_result))
        if not login_result.get("ok"):
            _emit_summary(summary, json_output=args.json)
            return 2

        if not args.skip_init_schema:
            _create_schema(
                metadata,
                password=password,
                prompt_for_password=not bool(args.no_password_prompt),
                schema=schema,
            )
            _record(summary, "schema.init", True, "schema initialized")
        else:
            _record(summary, "schema.init.skipped", True, "schema initialization skipped")

        strict_result = run_postgres_self_test(
            metadata,
            postgres_url=args.url,
            postgres_service=args.service,
            password=password,
            prompt_for_password=not bool(args.no_password_prompt),
        )
        _record(summary, "strict-check", bool(strict_result.get("ok")), "strict readiness check", strict_result)
        if not args.json:
            print(format_postgres_self_test(strict_result))
        if not strict_result.get("ok"):
            _emit_summary(summary, json_output=args.json)
            return 2

        if args.skip_crud:
            _record(summary, "driver-crud.skipped", True, "driver CRUD probe skipped")
        else:
            crud = _run_driver_crud(metadata)
            _record(summary, "driver-crud", True, "driver CRUD probe passed", crud)

        summary["ok"] = True
        _emit_summary(summary, json_output=args.json)
        return 0
    finally:
        if should_drop_schema and target.configured:
            _drop_schema(
                metadata,
                password=password,
                prompt_for_password=not bool(args.no_password_prompt),
                schema=schema,
            )


def _resolve_schema(args: argparse.Namespace) -> tuple[str, bool]:
    if args.schema:
        return str(args.schema), False
    if args.use_configured_schema:
        return configured_postgres_schema(), False
    suffix = "{}_{}".format(int(time.time()), uuid.uuid4().hex[:8])
    return "liuxin_smoke_" + suffix, True


def _should_drop_schema(args: argparse.Namespace, *, schema: str, generated_schema: bool) -> bool:
    if schema.casefold() == "public":
        return False
    if args.drop_schema:
        return True
    return bool(generated_schema and not args.keep_schema)


def _metadata_for_target(target, *, schema: str) -> dict[str, object]:
    metadata: dict[str, object] = {"schema": schema}
    if target.kind == "service":
        metadata["postgres_service"] = target.value
    elif target.kind == "url":
        metadata["postgres_url"] = target.value
    return metadata


def _password_for_run(args: argparse.Namespace) -> str:
    password = configured_postgres_password(getattr(args, "password", None))
    if getattr(args, "password", None):
        store_postgres_password(password)
    return password


def _create_schema(metadata: dict[str, object], *, password: str, prompt_for_password: bool, schema: str) -> None:
    raw = connect_postgres(metadata, password=password, prompt_for_password=prompt_for_password)
    try:
        create_postgres_schema(PostgresConnectionAdapter(raw), schema=schema)
    finally:
        raw.close()


def _drop_schema(metadata: dict[str, object], *, password: str, prompt_for_password: bool, schema: str) -> None:
    raw = connect_postgres(metadata, password=password, prompt_for_password=prompt_for_password)
    try:
        conn = PostgresConnectionAdapter(raw)
        with conn:
            conn.execute("drop schema if exists {} cascade".format(_quote_identifier(schema)))
    finally:
        raw.close()


def _run_driver_crud(metadata: dict[str, object]) -> dict[str, object]:
    driver = DatabaseDriver(metadata, set_conn=True)
    row_id = None
    try:
        row_id = driver.direct_add_simple_row_dict({"rating": 4.0, "rating_out_of": 5})
        row = driver.direct_get_row_dict_from_id("ratings", int(row_id))
        if not row or row.get("rating") != 4.0:
            raise RuntimeError("Inserted rating row could not be read back.")
        updated = dict(row)
        updated["rating"] = 4.5
        driver.direct_update_row_dict(updated)
        updated_row = driver.direct_get_row_dict_from_id("ratings", int(row_id))
        if not updated_row or updated_row.get("rating") != 4.5:
            raise RuntimeError("Updated rating row could not be read back.")
        return {"table": "ratings", "row_id": int(row_id)}
    finally:
        if row_id is not None:
            try:
                driver.direct_execute(
                    'delete from "ratings" where "rating_id" = %s',
                    (int(row_id),),
                )
            except Exception:
                pass
        try:
            driver.close()
        except Exception:
            pass


def _load_env_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit("Env file does not exist: {}".format(path))
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            raise SystemExit("Could not parse env file line {!r}: {}".format(raw_line, exc)) from exc
        if parts and parts[0] == "export":
            parts = parts[1:]
        for part in parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key:
                os.environ[key] = value


def _record(
    summary: dict[str, Any],
    name: str,
    ok: bool,
    message: str,
    details: object | None = None,
) -> None:
    step: dict[str, Any] = {"name": name, "ok": bool(ok), "message": message}
    if details is not None:
        step["details"] = details
    summary["steps"].append(step)


def _emit_summary(summary: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        return
    print("LiuXin PostgreSQL live smoke: {}".format("OK" if summary.get("ok") else "FAILED"))
    print("Target: {}".format(summary.get("url") or "<not configured>"))
    print("Schema: {}".format(summary.get("schema") or "<unknown>"))
    for step in summary.get("steps") or []:
        marker = "ok" if step.get("ok") else "FAIL"
        print("[{}] {}: {}".format(marker, step.get("name"), step.get("message")))


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


if __name__ == "__main__":
    raise SystemExit(main())
