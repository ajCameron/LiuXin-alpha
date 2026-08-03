"""Interactive wizard command for adding publisher organisation rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewPublisherWizardCommand(TerminalCommandAPI):
    """Create a publisher as an organisation-typed agent through prompts."""

    group = "add"
    name = "publisher"
    aliases = (
        "new-publisher",
        "new_publisher",
        "add-publisher",
        "add_publisher",
    )
    summary = "Interactive wizard to add a publisher."
    usage = "add publisher"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "org_agents"} - tables)
        if missing:
            raise ValueError("Database schema missing required tables: {}".format(", ".join(missing)))

        browser.emit("New publisher wizard")
        browser.emit("-------------------")

        publisher = browser.prompt_text("Publisher name", default="").strip()
        if not publisher:
            raise ValueError("Publisher name cannot be blank.")

        publisher_sort = browser.prompt_text("Publisher sort name", default=publisher).strip() or publisher
        publisher_phash = _clean_optional(browser.prompt_text("Publisher phash", default=""))
        publisher_description = _clean_optional(browser.prompt_text("Publisher description", default=""))
        publisher_wikipedia = _clean_optional(browser.prompt_text("Publisher Wikipedia URL", default=""))
        publisher_website = _clean_optional(browser.prompt_text("Publisher website", default=""))

        parent_id_text = browser.prompt_text("Parent publisher agent id (optional)", default="")
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent publisher agent id must be an integer.")
        if parent_id is not None:
            if browser.db.get_row_from_id("agents", parent_id) is None:
                raise ValueError("No agent exists with agent_id={}.".format(parent_id))

        publishr_position_text = browser.prompt_text("Publisher position (optional)", default="")
        publishr_position = _safe_int(publishr_position_text)
        if publishr_position_text.strip() and publishr_position is None:
            raise ValueError("Publisher position must be an integer.")

        publisher_full = _clean_optional(browser.prompt_text("Publisher full hierarchy text", default=""))

        existing = browser.db.search("agents", "agent_canonical_name", publisher)
        filtered_existing = []
        for row in existing:
            try:
                agent_type = str(row["agent_type"]).lower()
            except Exception:
                continue
            if agent_type == "organisation":
                filtered_existing.append(row)
        existing = filtered_existing
        if existing:
            browser.emit(
                "Possible duplicate publisher exists: agent_id={} name={!r}".format(
                    existing[0]["agent_id"],
                    existing[0]["agent_canonical_name"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another publisher with this name?", default=False)
            if not proceed_duplicate:
                raise ValueError("Publisher wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("name", publisher),
                        ("sort", publisher_sort),
                        ("website", publisher_website or ""),
                        ("parent_agent_id", parent_id if parent_id is not None else ""),
                    ],
                )
            ],
            title="Publisher summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this publisher now?", default=True)
        if not proceed:
            raise ValueError("Publisher wizard canceled.")

        aliases = []
        if publisher_phash:
            aliases.append("publisher_phash:{}".format(publisher_phash))
        if publishr_position is not None:
            aliases.append("publisher_position:{}".format(publishr_position))
        if publisher_full:
            aliases.append("publisher_full:{}".format(publisher_full))
        identifiers = [
            {"scheme": scheme, "value": value, "is_primary": is_primary}
            for scheme, value, is_primary in (
                ("url", publisher_website, True),
                ("wikipedia_url", publisher_wikipedia, True),
                ("publisher_phash", publisher_phash, False),
            )
            if value is not None
        ]

        result = browser.execute_core_command(
            "catalog.agent.create-organisation",
            payload={
                "data": {
                "name": publisher,
                "sort_name": publisher_sort,
                "aliases": aliases,
                },
                "details": {
                "org_agent_website": publisher_website,
                "org_agent_description": publisher_description,
                },
                "parent_id": parent_id,
                "relation_type": "imprint_of",
                "identifiers": identifiers,
            },
        )
        row = dict(result["agent"])

        browser.emit(
            "Publisher created: agent_id={} canonical_name={!r}".format(
                row["agent_id"],
                row["agent_canonical_name"],
            )
        )
        return True
