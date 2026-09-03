"""PostgreSQL connection and schema helpers."""

from __future__ import annotations

import importlib.util
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.config import (
    PostgresConnectionTarget,
    PostgresConfigError,
    add_password_to_url,
    configured_postgres_password,
    configured_postgres_schema,
    configured_postgres_target,
    configured_postgres_url,
    prompt_postgres_password,
    redact_postgres_target,
    should_prompt_for_password_error,
    store_postgres_password,
    url_has_password,
)
from LiuXin_alpha.utils.logging import default_log


DEFAULT_POSTGRES_APPLICATION_NAME = "liuxin-alpha"
DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_POSTGRES_WRITE_RETRIES = 4
DEFAULT_POSTGRES_WRITE_RETRY_DELAY = 0.75

POSTGRES_DRIVER_INSTALL_HINT = (
    "PostgreSQL Python support is not installed. From a LiuXin source checkout "
    "run `python -m pip install -e '.[postgres]'`; for an installed package run "
    "`python -m pip install 'liuxin-alpha[postgres]'`."
)
POSTGRES_DATABASE_SETUP_HINT = (
    "The target PostgreSQL database does not exist. Create the database and "
    "login roles as a PostgreSQL administrator; `liuxin postgres setup-sql "
    "--help` generates reviewable server/database setup SQL."
)
POSTGRES_SERVER_HINT = (
    "PostgreSQL is not reachable. For a local target, check that the server is "
    "installed and running (for example with `pg_isready`); for a remote target, "
    "verify its host, port, firewall, and service state."
)
POSTGRES_SERVICE_HINT = (
    "The selected PGSERVICE profile was not found. Check `PGSERVICE`, "
    "`PGSERVICEFILE`, and the PostgreSQL service-file entry, or use a "
    "postgresql:// URL."
)


class PostgresConnectionError(RuntimeError):
    """Raised when a configured PostgreSQL operation cannot connect or run."""


class PostgresSchemaError(PostgresConnectionError):
    """Raised when PostgreSQL is reachable but missing required schema."""


def postgres_driver_is_available() -> bool:
    """Return whether the optional psycopg2 runtime is importable."""

    try:
        return importlib.util.find_spec("psycopg2") is not None
    except (ImportError, ValueError):
        return False


def postgres_connection_hint(exc: BaseException | None) -> str:
    """Return one actionable hint for a common PostgreSQL setup failure."""

    if exc is None:
        return ""
    messages: list[str] = []
    sqlstates: set[str] = set()
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        messages.append(str(current or ""))
        for attribute in ("pgcode", "sqlstate"):
            value = getattr(current, attribute, None)
            if value:
                sqlstates.add(str(value).upper())
        current = current.__cause__ or current.__context__
    message = "\n".join(messages).casefold()

    missing_module = str(getattr(exc, "name", "") or "").casefold()
    if missing_module.startswith("psycopg2") or "no module named 'psycopg2'" in message:
        return POSTGRES_DRIVER_INSTALL_HINT
    if "3D000" in sqlstates or (
        "database" in message and "does not exist" in message
    ):
        return POSTGRES_DATABASE_SETUP_HINT
    if (
        "not found" in message
        and (
            "service definition" in message
            or "definition of service" in message
            or "service file" in message
            or "pgservice" in message
        )
    ):
        return POSTGRES_SERVICE_HINT
    server_markers = (
        "connection refused",
        "could not connect to server",
        "server closed the connection unexpectedly",
        "could not translate host name",
        "name or service not known",
        "temporary failure in name resolution",
    )
    missing_socket = "no such file or directory" in message and (
        "unix domain socket" in message or ".s.pgsql" in message
    )
    if missing_socket or any(marker in message for marker in server_markers):
        return POSTGRES_SERVER_HINT
    return ""


@dataclass(frozen=True)
class PostgresSchemaCheck:
    """Result for checking a set of PostgreSQL tables."""

    required_tables: tuple[str, ...]
    missing_tables: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.missing_tables


def connect_postgres(
    metadata: Mapping[str, object] | None = None,
    url: str | None = None,
    *,
    service: str | None = None,
    password: str | None = None,
    prompt_for_password: bool = True,
    connect_timeout_seconds: int = DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
    application_name: str = DEFAULT_POSTGRES_APPLICATION_NAME,
) -> Any:
    """Connect to a configured PostgreSQL database, prompting once if needed."""

    target = configured_postgres_target(metadata, explicit_url=url, explicit_service=service)
    if not target.configured:
        raise PostgresConfigError("No PostgreSQL URL or service profile configured for LiuXin.")

    configured_password = configured_postgres_password(password)

    try:
        import psycopg2
    except ModuleNotFoundError as exc:
        raise PostgresConnectionError(POSTGRES_DRIVER_INSTALL_HINT) from exc

    try:
        conn = _connect_psycopg2(
            psycopg2,
            target,
            password=configured_password,
            connect_timeout=max(1, int(connect_timeout_seconds)),
            application_name=application_name,
        )
        default_log.log_variables(
            "PostgreSQL connection established.",
            "DEBUG",
            ("database_target", redact_postgres_target(target)),
            ("application_name", application_name),
        )
        return conn
    except Exception as exc:
        if (
            configured_password
            or _target_has_password(target)
            or not prompt_for_password
            or not should_prompt_for_password_error(exc)
        ):
            raise PostgresConnectionError(redact_postgres_error(exc, target.label)) from exc

    prompted_password = store_postgres_password(prompt_postgres_password(target.label))
    try:
        conn = _connect_psycopg2(
            psycopg2,
            target,
            password=prompted_password,
            connect_timeout=max(1, int(connect_timeout_seconds)),
            application_name=application_name,
        )
        default_log.log_variables(
            "PostgreSQL connection established after password prompt.",
            "DEBUG",
            ("database_target", redact_postgres_target(target)),
            ("application_name", application_name),
        )
        return conn
    except Exception as retry_exc:
        raise PostgresConnectionError(redact_postgres_error(retry_exc, target.label)) from retry_exc


def _connect_psycopg2(
    psycopg2_module: Any,
    target: PostgresConnectionTarget,
    *,
    password: str,
    connect_timeout: int,
    application_name: str,
) -> Any:
    if target.kind == "service":
        kwargs: dict[str, object] = {
            "service": target.value,
            "connect_timeout": connect_timeout,
            "application_name": application_name,
        }
        if password:
            kwargs["password"] = password
        return psycopg2_module.connect(**kwargs)
    return psycopg2_module.connect(
        dsn=add_password_to_url(target.value, password),
        connect_timeout=connect_timeout,
        application_name=application_name,
    )


def _target_has_password(target: PostgresConnectionTarget) -> bool:
    return target.kind == "url" and url_has_password(target.value)


def postgres_cursor(conn: Any) -> Any:
    """Return the dict-like cursor used by PostgreSQL readiness checks."""

    from psycopg2.extras import RealDictCursor

    return conn.cursor(cursor_factory=RealDictCursor)


def set_statement_timeout(cur: Any, timeout_ms: int = DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS) -> None:
    cur.execute("set local statement_timeout = %s", (max(0, int(timeout_ms)),))


def table_exists(cur: Any, table_name: str, *, schema: str = "public") -> bool:
    cur.execute("select to_regclass(%s) is not null as exists", (_table_regclass(table_name, schema=schema),))
    row = cur.fetchone()
    return _row_bool(row, "exists")


def check_required_tables(
    cur: Any,
    required_tables: Iterable[str],
    *,
    schema: str = "public",
) -> PostgresSchemaCheck:
    required = tuple(str(table) for table in required_tables if str(table).strip())
    missing = tuple(table for table in required if not table_exists(cur, table, schema=schema))
    return PostgresSchemaCheck(required_tables=required, missing_tables=missing)


def ensure_required_tables(
    cur: Any,
    required_tables: Iterable[str],
    *,
    schema: str = "public",
    create_schema: Callable[[Any], None] | None = None,
    always_create_schema: bool = False,
    operation: str = "PostgreSQL schema check",
) -> PostgresSchemaCheck:
    """Check required tables, optionally creating/upgrading schema once."""

    check = check_required_tables(cur, required_tables, schema=schema)
    if create_schema is not None and always_create_schema:
        create_schema(cur)
        check = check_required_tables(cur, required_tables, schema=schema)
        if not check.ok:
            raise PostgresSchemaError(_missing_tables_message(operation, check.missing_tables))
        return check

    if check.ok or create_schema is None:
        if not check.ok:
            raise PostgresSchemaError(_missing_tables_message(operation, check.missing_tables))
        return check

    create_schema(cur)
    check = check_required_tables(cur, required_tables, schema=schema)
    if not check.ok:
        raise PostgresSchemaError(_missing_tables_message(operation, check.missing_tables))
    return check


def with_postgres_connection(
    operation: str,
    action: Callable[[Any], Any],
    *,
    metadata: Mapping[str, object] | None = None,
    url: str | None = None,
    service: str | None = None,
    schema: str | None = None,
    password: str | None = None,
    prompt_for_password: bool = True,
    read_only: bool = False,
    required_tables: Iterable[str] = (),
    create_schema: Callable[[Any], None] | None = None,
    always_create_schema: bool = False,
    statement_timeout_ms: int = DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS,
    retries: int = DEFAULT_POSTGRES_WRITE_RETRIES,
    retry_delay: float = DEFAULT_POSTGRES_WRITE_RETRY_DELAY,
    application_name: str = DEFAULT_POSTGRES_APPLICATION_NAME,
) -> Any:
    """Run an operation with common connection, timeout, schema, and retry handling."""

    last_exc: BaseException | None = None
    attempts = max(1, int(retries))
    schema_name = configured_postgres_schema(metadata, explicit=schema)
    for attempt in range(1, attempts + 1):
        conn = None
        try:
            conn = connect_postgres(
                metadata,
                url,
                service=service,
                password=password,
                prompt_for_password=prompt_for_password,
                application_name=application_name,
            )
            with conn:
                with postgres_cursor(conn) as cur:
                    if read_only:
                        cur.execute("set transaction read only")
                    set_statement_timeout(cur, timeout_ms=statement_timeout_ms)
                    cur.execute(f"set local search_path to {_quote_identifier(schema_name)}")
                    ensure_required_tables(
                        cur,
                        required_tables,
                        schema=schema_name,
                        create_schema=create_schema,
                        always_create_schema=always_create_schema,
                        operation=operation,
                    )
                    return action(cur)
        except ModuleNotFoundError:
            raise
        except Exception as exc:
            last_exc = exc
            if not retryable_postgres_error(exc) or attempt >= attempts:
                raise PostgresConnectionError(f"{operation} failed: {redact_postgres_error(exc, url)}") from exc
            time.sleep(max(0.0, float(retry_delay)) * attempt)
        finally:
            if conn is not None:
                conn.close()
    raise PostgresConnectionError(f"{operation} failed: {redact_postgres_error(last_exc, url)}")


def retryable_postgres_error(exc: BaseException) -> bool:
    try:
        import psycopg2

        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
    except ModuleNotFoundError:
        return False


def redact_postgres_error(exc: BaseException | None, *urls: object) -> str:
    """Return a redacted exception string with a common-setup hint when known."""

    if exc is None:
        return ""
    message = str(exc)
    for candidate in (*urls, configured_postgres_url()):
        text = str(candidate or "")
        if text:
            message = message.replace(text, redact_postgres_target(text))
    hint = postgres_connection_hint(exc)
    if hint and "hint:" not in message.casefold() and hint not in message:
        message = "{} Hint: {}".format(message.rstrip(), hint)
    return message


def translate_sqlite_placeholders(sql: str) -> str:
    """Translate SQLite ``?`` placeholders to psycopg2 ``%s`` placeholders."""

    out: list[str] = []
    quote: str | None = None
    escape = False
    for char in str(sql):
        if quote:
            out.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            out.append(char)
            continue
        if char == "?":
            out.append("%s")
        else:
            out.append(char)
    return "".join(out)


def translate_identifier_quotes(sql: str) -> str:
    """Translate SQLite backtick identifier quotes to PostgreSQL double quotes."""

    out: list[str] = []
    quote: str | None = None
    escape = False
    for char in str(sql):
        if quote:
            if quote == "`" and char == "`":
                out.append('"')
                quote = None
                continue
            out.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            continue
        if char == "`":
            quote = "`"
            out.append('"')
            continue
        if char in {"'", '"'}:
            quote = char
            out.append(char)
            continue
        out.append(char)
    return "".join(out)


def translate_sql_for_postgres(sql: str) -> str:
    """Apply the small SQL translations needed for direct SQL escape hatches."""

    return translate_sqlite_placeholders(translate_identifier_quotes(sql))


class PostgresConnectionAdapter:
    """Small sqlite3-shaped facade over a psycopg2 connection."""

    def __init__(self, raw_connection: Any):
        self.raw_connection = raw_connection

    def cursor(self, *args: Any, **kwargs: Any) -> "PostgresCursorAdapter":
        return PostgresCursorAdapter(self.raw_connection.cursor(*args, **kwargs))

    def execute(self, sql: str, values: Sequence[Any] | None = None) -> "PostgresCursorAdapter":
        cur = self.cursor()
        cur.execute(sql, values)
        return cur

    def executemany(self, sql: str, values: Iterable[Sequence[Any]]) -> "PostgresCursorAdapter":
        cur = self.cursor()
        cur.executemany(sql, values)
        return cur

    def executescript(self, sqlscript: str) -> None:
        statements = [stmt.strip() for stmt in str(sqlscript).split(";") if stmt.strip()]
        cur = self.cursor()
        for statement in statements:
            cur.execute(statement)

    def get(self, sql: str, values: Sequence[Any] | None = None, *, all: bool = True) -> Any:
        cur = self.execute(sql, values)
        if all:
            return cur.fetchall()
        row = cur.fetchone()
        if not row:
            return None
        try:
            return row[0]
        except Exception:
            return next(iter(row.values()))

    def get_row(self, sql: str, values: Sequence[Any] | None = None, *, all: bool = True) -> Any:
        cur = self.execute(sql, values)
        return cur.fetchall() if all else cur.fetchone()

    def commit(self) -> None:
        self.raw_connection.commit()

    def rollback(self) -> None:
        self.raw_connection.rollback()

    def close(self) -> None:
        self.raw_connection.close()

    def __enter__(self) -> "PostgresConnectionAdapter":
        self.raw_connection.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self.raw_connection.__exit__(exc_type, exc, tb)


class PostgresCursorAdapter:
    """Small sqlite3-shaped facade over a psycopg2 cursor."""

    lastrowid = None

    def __init__(self, raw_cursor: Any):
        self.raw_cursor = raw_cursor

    def execute(self, sql: str, values: Sequence[Any] | None = None) -> "PostgresCursorAdapter":
        translated = translate_sql_for_postgres(sql)
        if values is None:
            self.raw_cursor.execute(translated)
        else:
            self.raw_cursor.execute(translated, values)
        return self

    def executemany(self, sql: str, values: Iterable[Sequence[Any]]) -> "PostgresCursorAdapter":
        self.raw_cursor.executemany(translate_sql_for_postgres(sql), values)
        return self

    def fetchone(self) -> Any:
        return self.raw_cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self.raw_cursor.fetchall()

    def close(self) -> None:
        self.raw_cursor.close()

    def __iter__(self) -> Any:
        return iter(self.raw_cursor)

    def __enter__(self) -> "PostgresCursorAdapter":
        self.raw_cursor.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return self.raw_cursor.__exit__(exc_type, exc, tb)


def _missing_tables_message(operation: str, missing_tables: tuple[str, ...]) -> str:
    return f"{operation} missing required PostgreSQL tables: {', '.join(missing_tables)}"


def _row_bool(row: Any, key: str) -> bool:
    if row is None:
        return False
    if isinstance(row, Mapping):
        return bool(row.get(key))
    try:
        return bool(row[key])
    except Exception:
        return False


def _table_regclass(table_name: str, *, schema: str) -> str:
    text = str(table_name).strip()
    if "." in text:
        return text
    return f"{schema}.{text}"


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'
