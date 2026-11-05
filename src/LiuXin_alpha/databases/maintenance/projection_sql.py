"""Execute physical projection queries on a caller-owned connection.

These helpers never open transactions or connections. Catalog selects projection
keys, payloads, dependencies and freshness policy; plugin calls retain the bound
connection supplied by the maintenance runtime.
"""

from collections.abc import Sequence
from contextlib import closing
from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection
from LiuXin_alpha.databases.maintenance.schema import table_sql


def put_projection(
    connection: RowSQLConnection,
    plugin_id: str,
    key: str,
    value_json: str,
    revision: int,
) -> None:
    """Store a serialized projection without replacing a newer revision."""
    connection.execute(
        "INSERT INTO maintenance_projections (plugin_id,projection_key,value_json,revision) VALUES (?,?,?,?) "
        "ON CONFLICT (plugin_id,projection_key) DO UPDATE SET value_json=excluded.value_json, revision=excluded.revision "
        "WHERE maintenance_projections.revision <= excluded.revision",
        (plugin_id, key, value_json, revision),
    ).close()


def projection_revision(
    connection: RowSQLConnection, plugin_id: str, key: str
) -> int | None:
    """Read a stored revision, or None if the projection is absent."""
    with closing(
        connection.execute(
            "SELECT revision FROM maintenance_projections WHERE plugin_id=? AND projection_key=?",
            (plugin_id, key),
        )
    ) as cursor:
        row = cursor.fetchone()
    return row[0] if row is not None else None


def projections_for_key(
    connection: RowSQLConnection, key: str, *, schema: str | None = None
) -> list[Sequence[Any]]:
    """Read stored plugin/payload/revision tuples for one projection key."""
    with closing(
        connection.execute(
            f"SELECT plugin_id, value_json, revision FROM {table_sql('maintenance_projections', schema)} WHERE projection_key=?",
            (key,),
        )
    ) as cursor:
        return cursor.fetchall()


def work_exists(connection: RowSQLConnection, work_id: int) -> bool:
    """Check one physical Work row on the supplied transaction connection."""
    with closing(
        connection.execute("SELECT 1 FROM works WHERE work_id=?", (work_id,))
    ) as cursor:
        return cursor.fetchone() is not None


def count_work_expressions(connection: RowSQLConnection, work_id: int) -> int:
    """Count distinct Expression endpoints in physical Work interlinks."""
    with closing(
        connection.execute(
            "SELECT COUNT(DISTINCT expression_work_link_expression_id) FROM expression_work_links "
            "WHERE expression_work_link_work_id=?",
            (work_id,),
        )
    ) as cursor:
        row = cursor.fetchone()
        assert row is not None, "COUNT must produce one row"
        return row[0]
