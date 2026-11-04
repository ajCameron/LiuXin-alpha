# Todo: This feels like it's a driver thing - not this high level

"""
SQL transaction ownership independent of connection and facade lifetime.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

from LiuXin_alpha.databases.connection_hooks import (
    execute_transaction_control,
    flush_connection_work,
)


# Todo: This should be more strictly typeable
def transaction_active(connection: Any) -> bool:
    """
    Inspect supported connection state without starting or ending a transaction.

    :param connection:
    :return:
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


@contextmanager
def transaction_scope(
    connection: Any, *, owns_transaction: bool, immediate: bool = False
) -> Generator[Any, None, None]:
    """
    Commit an owned unit, or isolate a nested/borrowed unit with a savepoint.

    This never closes the connection. A connection owner must do so separately.
    An owned unit starts BEGIN explicitly: releasing a root SQLite savepoint
    would otherwise publish before the intended commit operation.
    ``immediate`` reserves SQLite's writer before any read and requires ownership.

    :param connection:
    :param owns_transaction:
    :param immediate:
    :return:
    """
    if immediate and not owns_transaction:
        raise ValueError("An immediate unit must own its transaction")
    if owns_transaction:
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
