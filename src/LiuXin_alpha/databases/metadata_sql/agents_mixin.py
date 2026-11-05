"""Read and lock physical Agent rows on the caller's database connection."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, cast

from LiuXin_alpha.databases.connection_hooks import before_statement
from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow
from LiuXin_alpha.databases.transactions import transaction_active

from ._relationships import require_link_spec

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection


class _QualifiedTableDriver(Protocol):
    """Native backend capability used after the PostgreSQL schema check."""

    def _table_sql(self, table_name: str) -> str: ...


class AgentsSQLMixin:
    """Own table-specific reads and locks while callers retain semantic policy.

    Example:
        Use these methods through ``db.metadata_sql`` on an open database.
    """

    db: DatabaseAPI

    def lock_agent_rows(
        self, connection: RowSQLConnection, agent_ids: Sequence[int]
    ) -> None:
        """Lock selected physical rows before a caller's read/modify/write unit.

        Require an active transaction and at most 500 positive IDs. PostgreSQL
        locks in ID order; SQLite callers must reserve the writer with a serialized
        macro transaction. No connection is opened, committed or closed here.
        Prepare installed maintenance's writer boundary before row locks so its
        advisory-lock order matches ordinary source writes. Missing IDs remain
        for the Catalog owner to diagnose when reading the selected rows.
        """
        if not transaction_active(connection):
            raise RuntimeError(
                "Locking Agent rows requires an active write transaction"
            )
        if not 1 <= len(agent_ids) <= 500 or any(
            type(row_id) is not int or row_id < 1 for row_id in agent_ids
        ):
            raise ValueError("Select between 1 and 500 positive Agent IDs")
        # Prepare the normal DML boundary without executing an Agent UPDATE.
        before_statement(connection, "UPDATE agents")
        if getattr(self.db.driver, "schema", None):
            table = cast(_QualifiedTableDriver, self.db.driver)._table_sql("agents")
            placeholders = ",".join("?" for _ in agent_ids)
            cursor = connection.execute(
                f"SELECT agent_id FROM {table} WHERE agent_id IN ({placeholders}) ORDER BY agent_id FOR UPDATE",
                tuple(agent_ids),
            )
            try:
                cursor.fetchall()
            finally:
                cursor.close()

    def get_agents_linked_from(
        self,
        source_table: str,
        source_id: int,
        *,
        link_type: object = LINK_TYPE_UNSET,
    ) -> tuple[LinkedTableRow, ...]:
        """Batch physical agents destinations for one declared relationship.

        Resolve the explicit source table through schema introspection. Preserve
        link order, duplicate destinations and all link metadata; missing rows
        remain None. Callers validate source existence and choose relationships.
        Supplied type filters require a typed relation. No new transaction opens.
        """
        spec = require_link_spec(self.db, source_table, "agents")
        return self.db.macros.get_linked_rows(spec, source_id, link_type=link_type)
