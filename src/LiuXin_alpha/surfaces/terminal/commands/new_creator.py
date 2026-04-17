"""Interactive wizard command for adding creator (human agent) entries."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.metadata_tools.add import Add
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.constants import CREATOR_TYPES
from LiuXin_alpha.metadata.utils import author_to_author_sort


def _clean_optional(value: str) -> Optional[str]:
    text = str(value).strip()
    return text or None


class NewCreatorWizardCommand(TerminalCommandAPI):
    """Create a creator as an `agents` + `human_agents` entity via wizard prompts."""

    group = "add"
    name = "creator"
    aliases = ("new-creator", "new_creator", "add-creator", "add_creator")
    summary = "Interactive wizard to add a creator (human agent)."
    usage = "add creator"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "human_agents"} - tables)
        if missing:
            raise ValueError("Database schema missing required tables: {}".format(", ".join(missing)))

        browser.emit("New creator wizard")
        browser.emit("------------------")

        creator_name = browser.prompt_text("Creator canonical name", default="").strip()
        if not creator_name:
            raise ValueError("Creator name cannot be blank.")

        creator_type = browser.prompt_text("Creator type", default="authors").strip().lower() or "authors"
        valid_creator_types = sorted({str(item).lower() for item in CREATOR_TYPES})
        if creator_type not in valid_creator_types:
            raise ValueError(
                "Unrecognized creator type {!r}. Valid types include: {}".format(
                    creator_type,
                    ", ".join(valid_creator_types[:20]),
                )
            )

        default_sort = author_to_author_sort(creator_name)
        creator_sort = browser.prompt_text("Creator sort name", default=default_sort).strip() or default_sort
        creator_short_name = _clean_optional(browser.prompt_text("Creator short name", default=""))
        creator_legal_name = _clean_optional(browser.prompt_text("Creator legal name", default=""))
        creator_birth_date = _clean_optional(browser.prompt_text("Creator birth date (YYYY-MM-DD)", default=""))
        creator_death_date = _clean_optional(browser.prompt_text("Creator death date (YYYY-MM-DD)", default=""))
        creator_language = _clean_optional(browser.prompt_text("Creator language", default=""))
        creator_bio = _clean_optional(browser.prompt_text("Creator biography/note", default=""))
        creator_wikipedia = _clean_optional(browser.prompt_text("Creator Wikipedia URL", default=""))
        creator_imdb = _clean_optional(browser.prompt_text("Creator IMDB id", default=""))
        creator_link = _clean_optional(browser.prompt_text("Creator external URL", default=""))
        creator_seminal_work = _clean_optional(browser.prompt_text("Creator seminal work", default=""))
        creator_one_person = browser.prompt_yes_no("Single-person attribution?", default=True)

        existing = self._find_existing_person_agent(browser, creator_name)
        if existing is not None:
            browser.emit(
                "Possible duplicate creator exists: agent_id={} name={!r}".format(
                    existing["agent_id"],
                    existing["agent_canonical_name"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another creator with this name?", default=False)
            if not proceed_duplicate:
                raise ValueError("Creator wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("name", creator_name),
                        ("type", creator_type),
                        ("sort", creator_sort),
                        ("one_person", bool(creator_one_person)),
                    ],
                )
            ],
            title="Creator summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this creator now?", default=True)
        if not proceed:
            raise ValueError("Creator wizard canceled.")

        add = Add(browser.db)
        creator_row = add.creator(
            creator=creator_name,
            creator_sort=creator_sort,
            creator_short_name=creator_short_name,
            creator_legal_name=creator_legal_name,
            creator_birth_date=creator_birth_date,
            creator_death_date=creator_death_date,
            creator_type=creator_type,
            creator_seminal_work=creator_seminal_work,
            creator_one_person=bool(creator_one_person),
            creator_wikipedia=creator_wikipedia,
            creator_imdb=creator_imdb,
            creator_link=creator_link,
            creator_language=creator_language,
            creator_bio=creator_bio,
        )

        browser.emit(
            "Creator created: agent_id={} canonical_name={!r}".format(
                creator_row["agent_id"],
                creator_row["agent_canonical_name"],
            )
        )
        return True

    def _find_existing_person_agent(self, browser, creator_name: str):
        rows = browser.db.search("agents", "agent_canonical_name", creator_name)
        for row in rows:
            try:
                if str(row["agent_type"]).lower() == "person":
                    return row
            except Exception:
                continue
        return None
