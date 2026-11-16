"""
SQL transaction ownership and cooperating writer locks.

These helpers operate on supplied connections. They neither open handles nor
own facade lifetimes; callers choose transaction, savepoint and cleanup scopes.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from uuid import uuid4

from LiuXin_alpha.databases.connection_hooks import (
    execute_transaction_control,
    flush_connection_work,
)
from LiuXin_alpha.databases.db_types.driver_types import QueryConnection


# Todo: This should be more strictly typeable
def transaction_active(connection: object) -> bool:
    """
    Inspect supported transaction state without starting or ending a SQL unit.

    Example:
        >>> connection = sqlite3.connect(":memory:")  # doctest: +SKIP
        >>> transaction_active(connection)  # doctest: +SKIP
        False


    :param connection: SQLite, APSW or PostgreSQL handle exposing transaction
        state.
    :return: True for an active or aborted unit that must remain under caller
        control.
    :raises TypeError: The handle does not expose a supported state interface.
    """
    active = getattr(connection, "in_transaction", None)
    if isinstance(active, bool):
        return active
    getautocommit = getattr(connection, "getautocommit", None)
    if callable(getautocommit):
        return not bool(getautocommit())
    raw = getattr(connection, "raw_connection", connection)
    status = getattr(raw, "get_transaction_status", None)
    if callable(status):
        # psycopg2's TRANSACTION_STATUS_IDLE is zero. Other states, including an
        # aborted transaction, still belong to the caller and must not be ended.
        return bool(status() != 0)
    raise TypeError("Connection must expose its transaction state before borrowing it")


def lock_postgres_writer(connection: QueryConnection, schema: str) -> None:
    """
    Reserve the cooperating PostgreSQL writer boundary until its SQL unit ends.

    Acquire the same database/schema advisory lock before reading protected
    revisions or fingerprints. Repeated acquisition in one transaction is safe.
    Rollback to a savepoint can release a lock acquired within that savepoint,
    so nested operations reacquire it instead of caching a Python flag.

    The caller supplies an active PostgreSQL transaction and owns its completion.
    Writers issuing raw SQL outside this protocol are not serialized by it.

    Example:
        >>> lock_postgres_writer(connection, "public")  # doctest: +SKIP


    :param connection: Active PostgreSQL handle supporting the adapter qmark
        syntax.
    :param schema: Physical schema shared by cooperating writers.
    :return: None after acquiring the lock; no commit or connection close is
        performed.
    :raises RuntimeError: The supplied connection has no active transaction.
    """
    if not transaction_active(connection):
        raise RuntimeError("A writer reservation requires an active transaction")
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(?))",
        (f"liuxin.serialized:{schema}",),
    ).close()


@contextmanager
def transaction_scope[ConnectionT: QueryConnection](
    connection: ConnectionT, *, owns_transaction: bool, immediate: bool = False
) -> Generator[ConnectionT, None, None]:
    """
    Commit an owned transaction or isolate a borrowed unit with a savepoint.

    An owned unit requires an idle connection and explicitly starts BEGIN.
    Successful exit flushes installed completion work and commits; body or
    completion failure attempts rollback. immediate selects SQLite BEGIN
    IMMEDIATE to reserve its writer before reading and requires ownership.

    A borrowed unit requires an existing transaction. It creates a savepoint,
    flushes completion work before release, and rolls back to the savepoint on
    failure. The outer transaction remains the caller's responsibility. Neither
    mode closes the connection. Backend cleanup errors propagate.

    Example:
        >>> import sqlite3
        >>> connection = sqlite3.connect(":memory:")
        >>> with transaction_scope(connection, owns_transaction=True):
        ...     connection.execute("CREATE TABLE example (id INTEGER)").close()
        >>> transaction_active(connection)
        False
        >>> connection.close()


    :param connection: Open handle whose lifetime remains with its supplying
        owner.
    :param owns_transaction: True begins/completes the whole unit; False creates
        a savepoint in an active unit.
    :param immediate: Use SQLite BEGIN IMMEDIATE for an owned transaction.
    :return: A context yielding the supplied connection.
    :raises ValueError: immediate is requested without transaction ownership.
    :raises RuntimeError: Connection state disagrees with the requested
        ownership.
    """
    if immediate and not owns_transaction:
        raise ValueError("An immediate unit must own its transaction")
    if owns_transaction:
        if transaction_active(connection):
            raise RuntimeError("An owned unit requires an idle connection")
        execute_transaction_control(
            connection, "BEGIN IMMEDIATE" if immediate else "BEGIN"
        )
        try:
            yield connection
            flush_connection_work(connection)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return

    if not transaction_active(connection):
        raise RuntimeError("A borrowed or nested unit requires an active transaction")
    name = f"liuxin_unit_{uuid4().hex}"
    execute_transaction_control(connection, f"SAVEPOINT {name}")
    try:
        yield connection
        flush_connection_work(connection)
    except BaseException:
        execute_transaction_control(connection, f"ROLLBACK TO SAVEPOINT {name}")
        raise
    finally:
        execute_transaction_control(connection, f"RELEASE SAVEPOINT {name}")
