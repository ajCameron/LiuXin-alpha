"""
Repair the schema-specific sentinel rows used for absent series and publishing agents.

These helpers perform writes through the owning macro transaction. Current FRBR schemas use an
organisation agent sentinel; a legacy publisher sentinel is considered only when the
agents table is absent.
"""

from __future__ import annotations

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME
from LiuXin_alpha.databases.database._state import DatabaseMixinState


# Todo: Null rows class?
class DatabaseNullRowsMixin(DatabaseMixinState):
    """
    Ensure conventional ID-zero records exist with normalized display fields.

    Example:
        During writable database bootstrap, db.ensure_null_rows() repairs sentinel
        records after schema discovery.
    """

    # Todo: Methods to get null rows?
    # Todo: These methods should be private - only run during startup - probably not
    def ensure_null_rows(self) -> None:
        """
        Insert or repair the series and publishing-entity sentinel records.

        When present, series ID 0 has a NULL display value. Prefer agents ID 0 with
        organisation type and AGENTS_NULL_CANONICAL_NAME; otherwise use publishers ID 0
        when that legacy table exists. With no all_tables cache, attempt series and
        agents directly. The caller's bootstrap transaction reserves the check-and-insert
        sequence across concurrent opens; backend errors propagate and roll back the unit.

        Example:
            For an open writable database, db.ensure_null_rows() restores a changed
            series sentinel display to None and the agent sentinel to its canonical
            organisation identity.


        :return: None; existing sentinel rows may be updated even when already valid.
        """
        with self._database.macros.transaction(serialized=True):
            self._ensure_reserved_rows()

    def _ensure_reserved_rows(self) -> None:
        """Repair sentinels inside the caller's reserved macro transaction."""
        # Ensure the series null row
        tables: set[str] | None = getattr(self, "all_tables", None)
        if tables is None or "series" in tables:
            series_0_row = self._database.macros.get_row("series", 0)
            if not series_0_row:
                series_null_row = {"series_id": 0}
                self._database.macros.insert_row("series", series_null_row)
            else:
                # Convention: the sentinel row's display value is NULL
                self._database.macros.update_row("series", 0, {"series": None})

        # Preferred (FRBR-first): ensure an organisation agent sentinel row
        if tables is None or "agents" in tables:
            agent_0_row = self._database.macros.get_row("agents", 0)
            if not agent_0_row:
                agent_null_row = {
                    "agent_id": 0,
                    "agent_type": "organisation",
                    # agent_canonical_name is NOT NULL in the current schema
                    "agent_canonical_name": AGENTS_NULL_CANONICAL_NAME,
                }
                self._database.macros.insert_row("agents", agent_null_row)
            else:
                # Always repair/normalize the sentinel row's canonical name.
                # (It's NOT NULL in schema, so we use a clearly intentional string.)
                self._database.macros.update_row(
                    "agents",
                    0,
                    {
                        "agent_type": "organisation",
                        "agent_canonical_name": AGENTS_NULL_CANONICAL_NAME,
                    },
                )

        # Legacy fallback (Calibre-style DBs): keep the old publishers sentinel row if that table exists.
        elif tables is None or "publishers" in tables:
            pub_0_row = self._database.macros.get_row("publishers", 0)
            if not pub_0_row:
                pub_null_row = {"publisher_id": 0}
                self._database.macros.insert_row("publishers", pub_null_row)
            else:
                self._database.macros.update_row("publishers", 0, {"publisher": None})
