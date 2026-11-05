"""
Repair the schema-specific sentinel rows used for absent series and publishing agents.

These helpers perform writes through the facade wrapper. Current FRBR schemas use an
organisation agent sentinel; a legacy publisher sentinel is considered only when the
agents table is absent.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database._state import DatabaseMixinState


from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME


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
        agents directly. Operations are sequential with no grouped rollback, and backend
        errors propagate.

        Example:
            For an open writable database, db.ensure_null_rows() restores a changed
            series sentinel display to None and the agent sentinel to its canonical
            organisation identity.


        :return: None; existing sentinel rows may be updated even when already valid.
        """

        # Ensure the series null row
        tables: set[str] | None = getattr(self, "all_tables", None)
        if tables is None or "series" in tables:
            series_0_row = self._database.driver_wrapper.get_row_from_id("series", 0)
            if not series_0_row:
                series_null_row = {"series_id": 0}
                self._database.driver_wrapper.add_row(series_null_row)
            else:
                # Convention: the sentinel row's display value is NULL
                series_0_row["series"] = None
                self._database.driver_wrapper.update_row(series_0_row)

        # Preferred (FRBR-first): ensure an organisation agent sentinel row
        if tables is None or "agents" in tables:
            agent_0_row = self._database.driver_wrapper.get_row_from_id("agents", 0)
            if not agent_0_row:
                agent_null_row = {
                    "agent_id": 0,
                    "agent_type": "organisation",
                    # agent_canonical_name is NOT NULL in the current schema
                    "agent_canonical_name": AGENTS_NULL_CANONICAL_NAME,
                }
                self._database.driver_wrapper.add_row(agent_null_row)
            else:
                agent_0_row["agent_type"] = "organisation"
                # Always repair/normalize the sentinel row's canonical name.
                # (It's NOT NULL in schema, so we use a clearly intentional string.)
                if (
                    agent_0_row.get("agent_canonical_name")
                    != AGENTS_NULL_CANONICAL_NAME
                ):
                    agent_0_row["agent_canonical_name"] = AGENTS_NULL_CANONICAL_NAME
                self._database.driver_wrapper.update_row(agent_0_row)

        # Legacy fallback (Calibre-style DBs): keep the old publishers sentinel row if that table exists.
        elif tables is None or "publishers" in tables:
            pub_0_row = self._database.driver_wrapper.get_row_from_id("publishers", 0)
            if not pub_0_row:
                pub_null_row = {"publisher_id": 0}
                self._database.driver_wrapper.add_row(pub_null_row)
            else:
                pub_0_row["publisher"] = None
                self._database.driver_wrapper.update_row(pub_0_row)
