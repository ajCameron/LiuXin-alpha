
"""
Mixin to handle access and control of the null rows.
"""

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


# Todo: Null rows class?
class DatabaseNullRowsMixin:
    """
    Methods to retrieve and manipulate the null rows.
    """
    # Todo: Methods to get null rows?
    # Todo: These methods should be private - only run during startup - probably not
    def ensure_null_rows(self: "DatabaseAPI") -> None:
        """
        Ensure required sentinel/null rows exist.

        Historically, LiuXin used id=0 in certain tables as a "null" record for
        link tables.

        In the FRBR-first/WEMI schema, publishing entities are modelled via
        `agents` (+ subtype sidecars like `org_agents`) rather than a dedicated
        `publishers` table.
        """

        # Ensure the series null row
        if getattr(self, "all_tables", None) is None or "series" in self.all_tables:
            series_0_row = self.driver_wrapper.get_row_from_id("series", 0)
            if not series_0_row:
                series_null_row = {"series_id": 0}
                self.driver_wrapper.add_row(series_null_row)
            else:
                # Convention: the sentinel row's display value is NULL
                series_0_row["series"] = None
                self.driver_wrapper.update_row(series_0_row)

        # Preferred (FRBR-first): ensure an organisation agent sentinel row
        if getattr(self, "all_tables", None) is None or "agents" in self.all_tables:
            agent_0_row = self.driver_wrapper.get_row_from_id("agents", 0)
            if not agent_0_row:
                agent_null_row = {
                    "agent_id": 0,
                    "agent_type": "organisation",
                    # agent_canonical_name is NOT NULL in the current schema
                    "agent_canonical_name": AGENTS_NULL_CANONICAL_NAME,
                }
                self.driver_wrapper.add_row(agent_null_row)
            else:
                agent_0_row["agent_type"] = "organisation"
                # Always repair/normalize the sentinel row's canonical name.
                # (It's NOT NULL in schema, so we use a clearly intentional string.)
                if agent_0_row.get("agent_canonical_name") != AGENTS_NULL_CANONICAL_NAME:
                    agent_0_row["agent_canonical_name"] = AGENTS_NULL_CANONICAL_NAME
                self.driver_wrapper.update_row(agent_0_row)

        # Legacy fallback (Calibre-style DBs): keep the old publishers sentinel row if that table exists.
        elif getattr(self, "all_tables", None) is None or "publishers" in self.all_tables:
            pub_0_row = self.driver_wrapper.get_row_from_id("publishers", 0)
            if not pub_0_row:
                pub_null_row = {"publisher_id": 0}
                self.driver_wrapper.add_row(pub_null_row)
            else:
                pub_0_row["publisher"] = None
                self.driver_wrapper.update_row(pub_0_row)
