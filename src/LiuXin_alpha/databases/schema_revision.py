"""Version the generated catalogue format independently of dynamic field DDL.

Only generators write the revision. Opening a catalogue never adopts or upgrades
an unversioned schema. Bump CURRENT_SCHEMA_REVISION when the base format changes.
"""

from __future__ import annotations

from contextlib import closing

from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection
from LiuXin_alpha.errors import DatabaseIntegrityError

CURRENT_SCHEMA_REVISION = 1


def validate_empty_creation_target(connection: RowSQLConnection) -> None:
    """Permit the generator's private connection only on an entirely empty target."""
    with closing(
        connection.execute("SELECT name FROM sqlite_master LIMIT 1")
    ) as cursor:
        if cursor.fetchone() is not None:
            raise DatabaseIntegrityError(
                "Catalogue creation requires an empty private target"
            )


def validate_schema_revision(
    connection: RowSQLConnection, *, schema: str | None = None
) -> None:
    """Reject missing, malformed and unsupported revisions without changing data.

    PostgreSQL callers supply their selected schema, preventing a table in another
    search-path namespace from authorizing this catalogue. Call before bootstrap.
    """
    parameters: tuple[str, ...]
    if schema is None:
        query = (
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_revision'"
        )
        parameters = ()
        table = '"schema_revision"'
    else:
        query = (
            "SELECT 1 FROM information_schema.tables WHERE table_schema=? "
            "AND table_name='schema_revision' AND table_type='BASE TABLE'"
        )
        parameters = (schema,)
        table = '"' + schema.replace('"', '""') + '"."schema_revision"'
    with closing(connection.execute(query, parameters)) as cursor:
        exists = cursor.fetchone() is not None
    recorded = None
    if exists:
        try:
            with closing(
                connection.execute(
                    f"SELECT schema_revision_id, schema_revision_revision FROM {table} LIMIT 2"
                )
            ) as cursor:
                rows = cursor.fetchall()
            if len(rows) == 1 and rows[0][0] == 1:
                recorded = rows[0][1]
        except Exception as error:
            raise DatabaseIntegrityError(
                "Malformed catalogue schema revision; regenerate this unsupported catalogue."
            ) from error
    if recorded != CURRENT_SCHEMA_REVISION or type(recorded) is not int:
        raise DatabaseIntegrityError(
            f"Unsupported catalogue schema revision: recorded={recorded!r}, "
            f"supported={CURRENT_SCHEMA_REVISION}. Regenerate this catalogue with "
            "the current LiuXin generator; unversioned pre-release libraries are not upgraded."
        )
