"""SQL transaction ownership independent of connection and facade lifetime."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4


def transaction_active(connection: Any) -> bool:
    """Inspect supported connection state without starting or ending a transaction."""
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


@contextmanager
def transaction_scope(
    connection: Any, *, owns_transaction: bool, immediate: bool = False
) -> Generator[Any, None, None]:
    """Commit an owned unit, or isolate a nested/borrowed unit with a savepoint.

    This never closes the connection. A connection owner must do so separately.
    An owned unit starts BEGIN explicitly: releasing a root SQLite savepoint
    would otherwise publish before the intended commit operation.
    ``immediate`` reserves SQLite's writer before any read and requires ownership.
    """
    if immediate and not owns_transaction:
        raise ValueError("An immediate unit must own its transaction")
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return

    if not transaction_active(connection):
        raise RuntimeError("A borrowed or nested unit requires an active transaction")
    name = f"liuxin_unit_{uuid4().hex}"
    connection.execute(f"SAVEPOINT {name}")
    try:
        yield connection
    except BaseException:
        connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
        raise
    finally:
        connection.execute(f"RELEASE SAVEPOINT {name}")
