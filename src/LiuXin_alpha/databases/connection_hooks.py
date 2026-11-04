"""Run connection-owned work before a transaction or savepoint completes.

Connections without an installed hook keep their existing behavior. The hook
must use the supplied transaction and must never commit it independently.
"""

from __future__ import annotations

import sqlite3
from functools import lru_cache
from typing import Any


def flush_connection_work(connection: Any) -> None:
    """Flush an explicitly installed hook without probing dynamic mock attributes."""
    try:
        hook = vars(connection).get("_liuxin_before_completion")
    except TypeError:
        return
    if hook is not None:
        hook(connection)


def before_statement(connection: Any, sql: str) -> None:
    """Run an optional boundary hook before executing SQL, never inside a trigger."""
    if vars(connection).get("_liuxin_transaction_control", False):
        return
    hook = vars(connection).get("_liuxin_before_statement")
    if hook is not None:
        hook(connection, sql)


def execute_transaction_control(connection: Any, sql: str) -> None:
    """Execute scope-owned controls after the scope's explicit completion check.

    In particular, releasing a rolled-back savepoint must not dispatch unrelated
    outer obligations during exception cleanup.
    """
    try:
        attributes = vars(connection)
    except TypeError:
        connection.execute(sql)
        return
    previous = attributes.get("_liuxin_transaction_control", False)
    attributes["_liuxin_transaction_control"] = True
    try:
        connection.execute(sql)
    finally:
        attributes["_liuxin_transaction_control"] = previous


class SQLiteCompletionCursor(sqlite3.Cursor):
    """Keep cursor SQL on the same maintenance boundaries as connection SQL."""

    def execute(self, sql, parameters=()):
        """Prepare the connection before one statement."""
        before_statement(self.connection, sql)
        return super().execute(sql, parameters)

    def executemany(self, sql, parameters):
        """Prepare each supplied row batch before execution."""
        before_statement(self.connection, sql)
        return super().executemany(sql, parameters)

    def executescript(self, sql):
        """Keep installed-maintenance scripts atomic without an implicit pre-commit."""
        if vars(self.connection).get("_liuxin_before_completion") is not None:
            # An attached but uninstalled runtime must not change old script APIs.
            installed = sqlite3.Connection.execute(
                self.connection,
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='maintenance_metadata'",
            ).fetchone()
            if installed:
                from LiuXin_alpha.databases.maintenance.bound_connection import (
                    statement_keyword,
                )
                from LiuXin_alpha.databases.transactions import (
                    transaction_active,
                    transaction_scope,
                )

                statements, start = [], 0
                for position, character in enumerate(sql):
                    if character == ";" and sqlite3.complete_statement(
                        sql[start : position + 1]
                    ):
                        statements.append(sql[start : position + 1])
                        start = position + 1
                if sql[start:].strip():
                    statements.append(sql[start:])
                if any(
                    statement_keyword(statement)
                    in {"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"}
                    for statement in statements
                ):
                    raise RuntimeError(
                        "Maintenance scripts cannot contain transaction controls"
                    )
                with transaction_scope(
                    self.connection,
                    owns_transaction=not transaction_active(self.connection),
                ):
                    for statement in statements:
                        self.execute(statement)
                return self
        return super().executescript(sql)


@lru_cache(maxsize=64)
def _completion_cursor_factory(factory):
    """Preserve custom cursor behavior behind the connection's boundary hooks.

    Wrap from creation, including before maintenance installation. Otherwise an
    older cursor could later commit an installed catalogue without completion.
    """
    if not isinstance(factory, type) or not issubclass(factory, sqlite3.Cursor):
        raise TypeError("Cursor factories must be sqlite3.Cursor subclasses")
    if issubclass(factory, SQLiteCompletionCursor):
        return factory
    if factory is sqlite3.Cursor:
        return SQLiteCompletionCursor
    return type(f"Completion{factory.__name__}", (SQLiteCompletionCursor, factory), {})


class SQLiteCompletionConnection(sqlite3.Connection):
    """Include required connection work in native SQLite commit/context handling."""

    def __setattr__(self, name, value):
        """Keep settings that implicitly commit inside the completion boundary."""
        completes = (name == "autocommit" and value is True) or (
            name == "isolation_level"
            and value is None
            and getattr(self, "autocommit", None) is not True
        )
        if (
            completes
            and vars(self).get("_liuxin_before_completion") is not None
            and self.in_transaction
        ):
            try:
                flush_connection_work(self)
            except BaseException:
                super().rollback()
                # With autocommit=True, native rollback() is a no-op even for
                # an explicitly opened transaction. The setter would commit it.
                if getattr(self, "autocommit", None) is True and self.in_transaction:
                    super().execute("ROLLBACK").close()
                raise
        super().__setattr__(name, value)

    def cursor(self, factory=SQLiteCompletionCursor):
        """Return a cursor that honors transaction completion and read boundaries."""
        return super().cursor(_completion_cursor_factory(factory))

    def execute(self, sql, parameters=()):
        """Execute through the same hooks as explicitly created cursors."""
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, parameters):
        """Execute a batch through the same hooks as explicitly created cursors."""
        return self.cursor().executemany(sql, parameters)

    def executescript(self, sql):
        """Delegate scripts to the cursor's explicit transaction safeguard."""
        return self.cursor().executescript(sql)

    def commit(self) -> None:
        """Commit only after required effects succeed; otherwise roll back the unit."""
        try:
            flush_connection_work(self)
        except BaseException:
            super().rollback()
            raise
        super().commit()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        """Keep hook failures inside the native transaction's rollback boundary."""
        if exc_type is None:
            try:
                flush_connection_work(self)
            except BaseException as error:
                super().__exit__(type(error), error, error.__traceback__)
                raise
        return super().__exit__(exc_type, exc, tb)
