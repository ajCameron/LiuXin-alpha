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
    Inspect supported connection state without starting or ending a transaction.

    :param connection: SQLite, APSW or PostgreSQL handle exposing transaction state.
    :return: Whether a transaction, including an aborted one, belongs to the caller.
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
    """Reserve the cooperating writer boundary until the SQL unit completes.

    Use the same database/schema key for outer serialized macro transactions and
    native operations nested inside ordinary transactions. Acquire before reading
    the revisions or schema fingerprints protected by the lock. It is reentrant
    on the same transaction and never commits. A rollback to a savepoint may
    release locks acquired within it, so each nested operation acquires its own
    reservation instead of caching a Python flag.

    This is a PostgreSQL transaction advisory lock, not protection against raw SQL
    writers that ignore the protocol. SQLite uses its native single-writer rules;
    a stale read snapshot must fail its attempted write rather than be reused.

    :param connection: Active PostgreSQL transaction owned by the calling scope.
    :param schema: Physical schema shared by cooperating writers.
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
    Commit an owned unit, or isolate a nested/borrowed unit with a savepoint.

    This never closes the connection. A connection owner must do so separately.
    An owned unit starts BEGIN explicitly: releasing a root SQLite savepoint
    would otherwise publish before the intended commit operation.
    ``immediate`` reserves SQLite's writer before any read and requires ownership.

    :param connection: Open handle whose lifetime remains with the caller.
    :param owns_transaction: Begin and complete the whole unit when true; otherwise
        create a savepoint inside the caller's active transaction.
    :param immediate: Reserve SQLite's writer at BEGIN, before taking a snapshot.
    :return: A context yielding the same connection. Successful exit commits or
        releases its savepoint; exceptional exit rolls back its own unit.
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
