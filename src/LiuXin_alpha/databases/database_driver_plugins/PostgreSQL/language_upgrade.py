"""Plan explicit language seeding while preserving existing row identities."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from LiuXin_alpha.databases.language_constants import language_rows
from LiuXin_alpha.databases.maintenance.schema import quote, table_sql

COLUMNS = (
    "language_id",
    "language",
    "language_code",
    "language_iso639_1",
    "language_iso639_2_b",
    "language_iso639_2_t",
    "language_bcp47_primary",
    "language_bcp47_variants",
)


def plan_rows(
    existing: Sequence[Sequence[Any]], schema: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Insert missing constants; reject conflicting codes without renumbering rows."""
    used = {int(row[0]) for row in existing}
    by_code = {row[4]: row for row in existing}
    canonical = language_rows()
    next_id = max((*used, *(row[0] for row in canonical)), default=0) + 1
    statements = []
    blockers = []
    for row in canonical:
        old = by_code.get(row[4])
        if old is not None:
            if tuple(old[2:7]) != row[2:7]:
                blockers.append(
                    f"Conflicting language mapping for {row[4]} (ID {old[0]})"
                )
            continue
        if any(old[2] == row[2] or old[6] == row[6] for old in existing):
            blockers.append(f"Conflicting language identity for {row[4]}")
            continue
        identity = row[0]
        if identity in used:
            identity, next_id = next_id, next_id + 1
        used.add(identity)
        values = (identity, *row[1:])
        literals = [
            str(value)
            if type(value) is int
            else "NULL"
            if value is None
            else "'" + str(value).replace("'", "''") + "'"
            for value in values
        ]
        statements.append(
            f"INSERT INTO {table_sql('languages', schema)} "
            f"({', '.join(quote(column) for column in COLUMNS)}) "
            f"VALUES ({', '.join(literals)})"
        )
    if statements:
        # Restored catalogues may have arbitrary surviving IDs. Advance the
        # identity sequence only after the entire repair's inserts are present.
        table = table_sql("languages", schema)
        literal = table.replace("'", "''")
        statements.append(
            f"SELECT setval(pg_get_serial_sequence('{literal}', 'language_id'), "
            f"(SELECT MAX(language_id) FROM {table}), true)"
        )
    return tuple(statements), tuple(blockers)
