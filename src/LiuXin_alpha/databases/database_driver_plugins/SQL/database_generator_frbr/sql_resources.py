"""Load immutable SQLite resource plans separately from staged execution.

BREAK markers are ordinary comments. SQLite's own completeness recognizer keeps
quoted semicolons and complete trigger bodies together. All resources are read
and checked for incomplete statements before execution of the stage begins.
"""

import os
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.sql_statements import statement_keyword


@dataclass(frozen=True)
class SQLResource:
    """An ordered snapshot; later filesystem changes cannot alter its statements."""

    path: Path
    statements: tuple[str, ...]


def split_resource_sql(sql: str, *, path: Path) -> tuple[str, ...]:
    """Split complete SQLite statements, rejecting unterminated resource tails."""
    statements: list[str] = []
    start = 0
    for index, char in enumerate(sql):
        if char == ";" and sqlite3.complete_statement(sql[start : index + 1]):
            statement = sql[start : index + 1]
            if statement_keyword(statement):
                statements.append(statement)
            start = index + 1
    tail = sql[start:]
    if tail.strip():
        # A final ordinary statement may omit its terminator. A partial trigger,
        # quote or block comment must not be mistaken for an ignorable tail.
        if not sqlite3.complete_statement(tail + "\n;"):
            raise ValueError(
                f"Incomplete SQL in {path}, statement {len(statements) + 1}"
            )
        if statement_keyword(tail):
            statements.append(tail)
    return tuple(statements)


def load_sql_resources(
    paths: Iterable[Path],
    *,
    transform: Callable[[str], str] | None = None,
) -> tuple[SQLResource, ...]:
    """Read and validate resources once, retaining first occurrence order."""
    resources = []
    seen: set[tuple[int, int]] = set()
    for path in paths:
        path = Path(os.path.abspath(path))
        try:
            with path.open(encoding="utf-8") as handle:
                stat = os.fstat(handle.fileno())
                identity = (stat.st_dev, stat.st_ino)
                if identity in seen:
                    continue
                seen.add(identity)
                sql = handle.read()
        except OSError as error:
            raise FileNotFoundError(f"Unable to read SQL resource: {path}") from error
        if transform is not None:
            sql = transform(sql)
        resources.append(SQLResource(path, split_resource_sql(sql, path=path)))
    return tuple(resources)


def execute_sql_resources(
    connection: SQLConnection, resources: Iterable[SQLResource]
) -> None:
    """Execute a validated snapshot, committing each resource; retain the handle."""
    cursor = connection.cursor()
    for resource in resources:
        for ordinal, statement in enumerate(resource.statements, 1):
            try:
                cursor.execute(statement)
            except sqlite3.Error as error:
                raise TypeError(
                    f"SQL resource {resource.path}, statement {ordinal}: {error}"
                ) from error
            except Exception as error:
                error.add_note(f"SQL resource {resource.path}, statement {ordinal}")
                raise
        if resource.statements:
            connection.commit()
