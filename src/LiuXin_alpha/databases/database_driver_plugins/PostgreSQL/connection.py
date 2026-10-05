"""
Connect, inspect required tables and adapt direct SQL calls for PostgreSQL.

Helpers centralize optional psycopg2 loading, one authentication prompt, transaction
timeouts and bounded retries. Connection/cursor adapters translate a small SQLite
SQL subset; they do not provide a complete SQL dialect parser.
"""

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
    """
    Report a connection or managed PostgreSQL operation failure.

    Example:
        Catch ``PostgresConnectionError`` around a managed operation to show its
        redacted connection detail and available setup hint.
    """


class PostgresSchemaError(PostgresConnectionError):
    """
    Report required relations still missing after optional schema creation.

    Example:
        ``ensure_required_tables`` raises this when works is required but absent
        and no successful creation callback supplies it.
    """


def postgres_driver_is_available() -> bool:
    """
    Probe for the optional psycopg2 module without importing it.

    ImportError and ValueError from spec lookup count as unavailable.

    Example:
        Use ``postgres_driver_is_available()`` to decide whether a connection
        attempt can proceed or should show the installation hint.


    :return: Whether importlib can locate a psycopg2 module spec.
    """

    try:
        return importlib.util.find_spec("psycopg2") is not None
    except (ImportError, ValueError):
        return False


def postgres_connection_hint(exc: BaseException | None) -> str:
    """
    Select one setup hint from exception text and its cause/context chain.

    The first recognized category wins: missing driver, missing database, missing service,
    then unreachable server. Traversal avoids exception-chain cycles.

    Example:
        >>> postgres_connection_hint(None)
        ''


    :param exc: Optional exception whose messages and SQLSTATE attributes are inspected.
    :return: One actionable hint or an empty string for an unrecognized failure.
    """

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
    """
    Immutable required/missing table tuples returned by schema checks.

    ``required_tables`` retains request order and ``missing_tables`` records failed
    lookups; constructing this value performs no database queries.

    Example:
        >>> PostgresSchemaCheck(("works",), ()).ok
        True
    """

    required_tables: tuple[str, ...]
    missing_tables: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """
        Report success when the missing-table tuple is empty.

        Example:
            >>> PostgresSchemaCheck(("works",), ("works",)).ok
            False


        :return: True when no required table is missing.
        """
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
    """
    Open a raw psycopg2 connection and transfer closing responsibility to the caller.

    Resolve configuration before importing the optional driver. Prompt once only for
    recognized authentication failures with no configured or URL-embedded password.
    The prompted password is stored in the process environment. Missing configuration
    raises PostgresConfigError; driver/connection failures raise PostgresConnectionError.

    Example:
        ``connect_postgres(service="library", prompt_for_password=False)`` opens
        the service target; close the returned connection after use.


    :param metadata: Optional target/schema configuration mapping.
    :param url: Explicit URL candidate, otherwise resolved from configuration.
    :param service: Optional explicit PostgreSQL service profile.
    :param password: Optional password, otherwise read from configuration.
    :param prompt_for_password: Whether missing authentication may prompt once on a terminal.
    :param connect_timeout_seconds: Connection timeout coerced to an integer of at least one second.
    :param application_name: Application name passed to psycopg2 for server-side identification.
    :return: Open raw psycopg2 connection; this function does not set schema/search_path.
    """

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
    """
    Call the supplied driver with native service keywords or a password-augmented DSN.

    For URL targets an embedded password takes precedence over the supplied password.
    Connection errors propagate to the caller.

    Example:
        Service targets pass ``service=target.value`` rather than manufacturing
        a service URL.


    :param psycopg2_module: Driver module or test double exposing connect.
    :param target: Resolved PostgreSQL URL or service target.
    :param password: Optional password, otherwise read from configuration.
    :param connect_timeout: Timeout in seconds, already normalized by the caller.
    :param application_name: Application name passed to psycopg2 for server-side identification.
    :return: The connection returned by psycopg2.connect.
    """
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
    """
    Recognize an authority password only for URL-kind targets.

    Example:
        >>> _target_has_password(PostgresConnectionTarget("service", "library"))
        False


    :param target: Resolved PostgreSQL URL or service target.
    :return: Whether the URL target embeds a password, including an empty one.
    """
    return target.kind == "url" and url_has_password(target.value)


def postgres_cursor(conn: Any) -> Any:
    """
    Open a RealDictCursor on an existing connection.

    The caller owns the cursor and must close it or use its context manager.

    Example:
        ``with postgres_cursor(conn) as cur:`` makes fetched rows addressable
        by column labels.


    :param conn: Open psycopg2 connection supplying cursor().
    :return: New psycopg2 RealDictCursor.
    """

    from psycopg2.extras import RealDictCursor

    return conn.cursor(cursor_factory=RealDictCursor)


def set_statement_timeout(cur: Any, timeout_ms: int = DEFAULT_POSTGRES_STATEMENT_TIMEOUT_MS) -> None:
    """
    Set a transaction-local statement timeout, clamped to nonnegative milliseconds.

    Example:
        ``set_statement_timeout(cur, 0)`` disables the local timeout.


    :param cur: Open mapping-row cursor owned by the caller.
    :param timeout_ms: Milliseconds coerced to an integer; negative values become zero.
    :return: None; executes SET LOCAL on the cursor.
    """
    cur.execute("set local statement_timeout = %s", (max(0, int(timeout_ms)),))


def table_exists(cur: Any, table_name: str, *, schema: str = "public") -> bool:
    """
    Resolve a relation through to_regclass using a bound textual name.

    A name already containing a dot bypasses schema prefixing. The text is interpreted
    by PostgreSQL rather than quoted as a single identifier.

    Example:
        ``table_exists(cur, "works", schema="library")`` checks library.works.


    :param cur: Open mapping-row cursor owned by the caller.
    :param table_name: Relation name, optionally already schema-qualified.
    :param schema: Schema name used for unqualified relations.
    :return: Whether to_regclass resolves a relation.
    """
    cur.execute("select to_regclass(%s) is not null as exists", (_table_regclass(table_name, schema=schema),))
    row = cur.fetchone()
    return _row_bool(row, "exists")


def check_required_tables(
    cur: Any,
    required_tables: Iterable[str],
    *,
    schema: str = "public",
) -> PostgresSchemaCheck:
    """
    Record missing relations while retaining nonblank requested names and order.

    Example:
        ``check_required_tables(cur, ("works", "agents"))`` returns both required
        names and the subset that failed lookup.


    :param cur: Open mapping-row cursor owned by the caller.
    :param required_tables: Names to check; blank names are excluded.
    :param schema: Schema name used for unqualified relations.
    :return: Immutable required/missing table result.
    """
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
    """
    Check relations, optionally invoke one creation callback, then recheck.

    Call the callback when tables are missing or always_create_schema is true. Raise
    PostgresSchemaError if requirements still fail, including when no callback is given.
    Transaction boundaries belong to the caller.

    Example:
        A schema callback can create works when ``required_tables=("works",)``;
        the helper checks again before returning success.


    :param cur: Open mapping-row cursor owned by the caller.
    :param required_tables: Names to check; blank names are excluded.
    :param schema: Schema name used for unqualified relations.
    :param create_schema: Optional callback receiving the open cursor to create or upgrade tables.
    :param always_create_schema: Whether to call create_schema even when all required tables exist.
    :param operation: Operation label used in failure messages.
    :return: Successful required/missing table result.
    """

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
    """
    Run a callback inside an owned connection, transaction and mapping cursor.

    Apply read-only mode when requested, a local timeout and quoted search_path before
    checking/creating schema and invoking action. Close every connection. OperationalError
    and InterfaceError can retry the entire callback with a fresh transaction and linear
    delay; actions must tolerate replay and external side effects. Other failures are
    wrapped as PostgresConnectionError, while ModuleNotFoundError propagates.

    Example:
        ``with_postgres_connection("count works", action, service="library",
        read_only=True, required_tables=("works",))`` runs action after schema checks.


    :param operation: Operation label used in failure messages.
    :param action: Callable receiving the open cursor; it may run again after a retryable error.
    :param metadata: Optional target/schema configuration mapping.
    :param url: Explicit URL candidate, otherwise resolved from configuration.
    :param service: Optional explicit PostgreSQL service profile.
    :param schema: Schema name used for unqualified relations.
    :param password: Optional password, otherwise read from configuration.
    :param prompt_for_password: Whether missing authentication may prompt once on a terminal.
    :param read_only: Whether to issue SET TRANSACTION READ ONLY before other statements.
    :param required_tables: Names to check; blank names are excluded.
    :param create_schema: Optional callback receiving the open cursor to create or upgrade tables.
    :param always_create_schema: Whether to call create_schema even when all required tables exist.
    :param statement_timeout_ms: Nonnegative transaction-local query timeout in milliseconds.
    :param retries: Total attempts, not extra retries; coerced to at least one.
    :param retry_delay: Base delay in seconds, clamped to zero and multiplied by the failed attempt number.
    :param application_name: Application name passed to psycopg2 for server-side identification.
    :return: Value returned by action after the transaction/context completes.
    """

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
    """
    Recognize psycopg2 OperationalError or InterfaceError for managed retries.

    Example:
        A connection interruption represented by OperationalError is retryable;
        a schema validation PostgresSchemaError is not.


    :param exc: Failure instance checked against psycopg2 exception classes.
    :return: False when psycopg2 is absent; otherwise whether the exception is one of the retry types.
    """
    try:
        import psycopg2

        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))
    except ModuleNotFoundError:
        return False


def redact_postgres_error(exc: BaseException | None, *urls: object) -> str:
    """
    Replace known target strings in an error and append one recognized setup hint.

    Only supplied targets and the configured URL are replaced; arbitrary secret text is
    not scanned. Existing hint: text suppresses adding another hint.

    Example:
        >>> redact_postgres_error(None)
        ''


    :param exc: Optional exception to stringify.
    :param urls: Additional raw target strings to replace with redacted versions.
    :return: Error text with exact target substitutions and optional hint.
    """

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
    """
    Replace question marks outside single/double-quoted text with %s.

    This character scanner handles backslash escapes inside quotes. It does not parse
    SQL comments, dollar-quoted strings or PostgreSQL question-mark operators.

    Example:
        >>> translate_sqlite_placeholders("select ? as value")
        'select %s as value'


    :param sql: SQL text subjected to the small placeholder/identifier translations.
    :return: SQL text with unquoted question marks replaced.
    """

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
    """
    Replace backtick identifier delimiters with double quotes outside quoted text.

    This is a small character scanner, not a SQL parser; comments and dollar quoting
    are not recognized.

    Example:
        >>> translate_identifier_quotes("select `work_id` from `works`")
        'select "work_id" from "works"'


    :param sql: SQL text subjected to the small placeholder/identifier translations.
    :return: SQL text with recognized backtick delimiters replaced.
    """

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
    """
    Translate identifier quotes first, then unquoted SQLite placeholders.

    Example:
        >>> translate_sql_for_postgres("select `work_id` from `works` where work_id = ?")
        'select "work_id" from "works" where work_id = %s'


    :param sql: SQL text subjected to the small placeholder/identifier translations.
    :return: SQL text after both limited translations.
    """

    return translate_sqlite_placeholders(translate_identifier_quotes(sql))


class PostgresConnectionAdapter:
    """
    Expose the connection operations needed by SQLite-shaped direct SQL callers.

    ``raw_connection`` retains the underlying connection. Context exit delegates commit/
    rollback but does not explicitly close it; callers remain responsible for closing.
    Convenience fetch/script methods do not explicitly close their temporary cursors.

    Example:
        Wrap an open connection with ``PostgresConnectionAdapter(raw)`` to use
        ``execute("select ?", (1,))`` with translated placeholders.
    """

    def __init__(self, raw_connection: Any):
        """
        Retain a raw connection without opening a transaction or taking a copy.

        Example:
            ``adapter.raw_connection is raw`` after ``adapter = PostgresConnectionAdapter(raw)``.


        :param raw_connection: Open psycopg2 connection to wrap; no new connection is opened.
        :return: None; stores the connection.
        """
        self.raw_connection = raw_connection

    def cursor(self, *args: Any, **kwargs: Any) -> "PostgresCursorAdapter":
        """
        Create and wrap a raw cursor, forwarding all cursor-construction options.

        Example:
            ``adapter.cursor(cursor_factory=RealDictCursor)`` wraps a mapping cursor.


        :param args: Positional arguments forwarded to raw_connection.cursor.
        :param kwargs: Keyword arguments forwarded to raw_connection.cursor.
        :return: New cursor adapter; the caller owns cursor cleanup.
        """
        return PostgresCursorAdapter(self.raw_connection.cursor(*args, **kwargs))

    def execute(self, sql: str, values: Sequence[Any] | None = None) -> "PostgresCursorAdapter":
        """
        Open a cursor and execute one translated statement.

        Example:
            ``adapter.execute("select ?", (1,))`` returns a cursor for fetching.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Bound values passed to the underlying cursor.
        :return: New cursor adapter; the caller owns cursor cleanup.
        """
        cur = self.cursor()
        cur.execute(sql, values)
        return cur

    def executemany(self, sql: str, values: Iterable[Sequence[Any]]) -> "PostgresCursorAdapter":
        """
        Open a cursor and execute translated SQL for each parameter sequence.

        Example:
            ``adapter.executemany("insert into t(v) values (?)", [(1,), (2,)])``
            uses one newly opened cursor.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Iterable of parameter sequences passed to raw executemany.
        :return: New cursor adapter; the caller owns cursor cleanup.
        """
        cur = self.cursor()
        cur.executemany(sql, values)
        return cur

    def executescript(self, sqlscript: str) -> None:
        """
        Split text on semicolons and execute nonblank fragments on one cursor.

        Semicolons inside strings, comments or procedural bodies are also split. No commit
        or explicit cursor close is performed here; use only suitable simple scripts.

        Example:
            ``adapter.executescript("select 1; select 2;")`` executes two fragments.


        :param sqlscript: Simple semicolon-separated SQL script.
        :return: None; leaves transaction ownership with the caller.
        """
        statements = [stmt.strip() for stmt in str(sqlscript).split(";") if stmt.strip()]
        cur = self.cursor()
        for statement in statements:
            cur.execute(statement)

    def get(self, sql: str, values: Sequence[Any] | None = None, *, all: bool = True) -> Any:
        """
        Fetch all rows, or the first scalar when all is false.

        Scalar mode tries index zero, then the first mapping value. The temporary cursor
        is not explicitly closed.

        Example:
            ``adapter.get("select 1", all=False)`` returns the scalar 1.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Bound values passed to the underlying cursor.
        :param all: Whether to fetch all rows rather than one result.
        :return: All rows, the first scalar, or None when scalar mode finds no row.
        """
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
        """
        Fetch all rows or one complete row, retaining the raw row representation.

        Example:
            ``adapter.get_row("select 1", all=False)`` returns one row rather than
            extracting its scalar value.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Bound values passed to the underlying cursor.
        :param all: Whether to fetch all rows rather than one result.
        :return: All fetched rows or one row/None; cursor cleanup is not performed here.
        """
        cur = self.execute(sql, values)
        return cur.fetchall() if all else cur.fetchone()

    def commit(self) -> None:
        """
        Commit the pending raw-connection transaction.

        Example:
            ``adapter.commit()`` makes pending writes durable.


        :return: None; forwards the operation to the raw connection.
        """
        self.raw_connection.commit()

    def rollback(self) -> None:
        """
        Roll back the pending raw-connection transaction.

        Example:
            ``adapter.rollback()`` discards uncommitted writes.


        :return: None; forwards the operation to the raw connection.
        """
        self.raw_connection.rollback()

    def close(self) -> None:
        """
        Close the underlying connection using its driver-defined cleanup.

        Example:
            ``adapter.close()`` releases the connection after use.


        :return: None; forwards the operation to the raw connection.
        """
        self.raw_connection.close()

    def __enter__(self) -> "PostgresConnectionAdapter":
        """
        Enter the raw connection transaction context and retain the adapter interface.

        Example:
            ``with adapter as active:`` binds active to the same adapter.


        :return: This adapter after raw context entry succeeds.
        """
        self.raw_connection.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        """
        Delegate transaction context exit to the raw connection.

        No explicit close is performed here; raw context semantics determine commit,
        rollback and exception handling.

        Example:
            Leaving ``with adapter:`` forwards the body exception, if any, to psycopg2.


        :param exc_type: Exception type from the context body, or None.
        :param exc: Exception from the context body, or None.
        :param tb: Traceback from the context body, or None.
        :return: Underlying context exit result.
        """
        return self.raw_connection.__exit__(exc_type, exc, tb)


class PostgresCursorAdapter:
    """
    Translate SQL while retaining the underlying cursor row and context behavior.

    ``raw_cursor`` owns actual execution; ``lastrowid`` is always None. Use RETURNING
    when an inserted PostgreSQL ID is needed.

    Example:
        ``PostgresCursorAdapter(raw).execute("select ?", (1,))`` returns the
        same adapter for subsequent fetch calls.
    """

    lastrowid = None

    def __init__(self, raw_cursor: Any):
        """
        Retain an existing cursor without changing its row factory.

        Example:
            ``adapter.raw_cursor is raw`` after cursor adapter construction.


        :param raw_cursor: Open psycopg2 cursor to wrap.
        :return: None; stores the cursor.
        """
        self.raw_cursor = raw_cursor

    def execute(self, sql: str, values: Sequence[Any] | None = None) -> "PostgresCursorAdapter":
        """
        Translate SQL and execute with optional bound values.

        When values is None, omit the second raw execute argument entirely.

        Example:
            ``cur.execute("select ?", (1,)).fetchone()`` executes then fetches one row.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Bound values passed to the underlying cursor.
        :return: This cursor adapter for chained operations.
        """
        translated = translate_sql_for_postgres(sql)
        if values is None:
            self.raw_cursor.execute(translated)
        else:
            self.raw_cursor.execute(translated, values)
        return self

    def executemany(self, sql: str, values: Iterable[Sequence[Any]]) -> "PostgresCursorAdapter":
        """
        Translate one SQL template and pass all parameter sequences to the raw cursor.

        Example:
            ``cur.executemany("insert into t(v) values (?)", [(1,), (2,)])``
            retains this cursor adapter.


        :param sql: SQL text subjected to the small placeholder/identifier translations.
        :param values: Iterable of parameter sequences passed through without materialization.
        :return: This cursor adapter.
        """
        self.raw_cursor.executemany(translate_sql_for_postgres(sql), values)
        return self

    def fetchone(self) -> Any:
        """
        Fetch the next row without changing its representation.

        Example:
            A mapping raw cursor produces a mapping from ``cur.fetchone()``.


        :return: Next raw row, or None when exhausted.
        """
        return self.raw_cursor.fetchone()

    def fetchall(self) -> list[Any]:
        """
        Fetch all remaining rows from the raw cursor.

        Example:
            ``cur.fetchall()`` returns an empty list after all rows have been consumed.


        :return: List of remaining raw rows.
        """
        return self.raw_cursor.fetchall()

    def close(self) -> None:
        """
        Close the underlying cursor.

        Example:
            ``cur.close()`` releases cursor resources after fetching.


        :return: None; delegates cleanup to the driver.
        """
        self.raw_cursor.close()

    def __iter__(self) -> Any:
        """
        Iterate directly over the underlying cursor.

        Example:
            ``for row in cur:`` yields raw rows without conversion.


        :return: Iterator supplied by the raw cursor.
        """
        return iter(self.raw_cursor)

    def __enter__(self) -> "PostgresCursorAdapter":
        """
        Enter the raw cursor context and expose this adapter in the with body.

        Example:
            ``with adapter as cur:`` binds cur to the adapter.


        :return: This cursor adapter.
        """
        self.raw_cursor.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        """
        Forward context exit and exception details to the raw cursor.

        Example:
            Leaving ``with cur:`` lets the raw cursor perform its own context cleanup.


        :param exc_type: Exception type from the context body, or None.
        :param exc: Exception from the context body, or None.
        :param tb: Traceback from the context body, or None.
        :return: Underlying cursor context exit result.
        """
        return self.raw_cursor.__exit__(exc_type, exc, tb)


def _missing_tables_message(operation: str, missing_tables: tuple[str, ...]) -> str:
    """
    Format the operation label and missing relation names in their supplied order.

    Example:
        >>> _missing_tables_message("read", ("works",))
        'read missing required PostgreSQL tables: works'


    :param operation: Operation label used in failure messages.
    :param missing_tables: Missing relation names in request order.
    :return: Failure message suitable for PostgresSchemaError.
    """
    return f"{operation} missing required PostgreSQL tables: {', '.join(missing_tables)}"


def _row_bool(row: Any, key: str) -> bool:
    """
    Read a named truth value, returning False for absent or inaccessible values.

    Example:
        >>> _row_bool({"exists": True}, "exists")
        True


    :param row: Mapping or key-addressable row object.
    :param key: Column label to inspect.
    :return: Boolean value or False on missing row/key/index failure.
    """
    if row is None:
        return False
    if isinstance(row, Mapping):
        return bool(row.get(key))
    try:
        return bool(row[key])
    except Exception:
        return False


def _table_regclass(table_name: str, *, schema: str) -> str:
    """
    Trim a relation name and prefix the schema only if it contains no dot.

    Example:
        >>> _table_regclass("works", schema="library")
        'library.works'


    :param table_name: Relation text, optionally already qualified.
    :param schema: Schema name used for unqualified relations.
    :return: Text to bind to to_regclass; identifiers are not quoted here.
    """
    text = str(table_name).strip()
    if "." in text:
        return text
    return f"{schema}.{text}"


def _quote_identifier(value: str) -> str:
    """
    Quote one identifier, doubling embedded double quotes.

    Example:
        >>> _quote_identifier("public")
        '"public"'


    :param value: Identifier converted to text before escaping.
    :return: Delimited SQL identifier.
    """
    return '"' + str(value).replace('"', '""') + '"'
