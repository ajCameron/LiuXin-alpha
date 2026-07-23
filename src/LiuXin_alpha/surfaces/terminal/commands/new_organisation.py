"""Interactive wizard command for adding organisation agent rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.utils.language_tools import best_effort_language_id


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


def _split_aliases(raw: str) -> Optional[list[str]]:
    text = str(raw).strip()
    if not text:
        return None
    parts = [part.strip() for part in text.split(",")]
    out = [part for part in parts if part]
    return out or None


class NewOrganisationWizardCommand(TerminalCommandAPI):
    """Create an organisation as an agent + org sidecar through prompts."""

    group = "add"
    name = "organisation"
    aliases = (
        "organization",
        "new-organisation",
        "new_organisation",
        "new-organization",
        "new_organization",
        "add-organisation",
        "add_organisation",
        "add-organization",
        "add_organization",
    )
    summary = "Interactive wizard to add an organisation."
    usage = "add organisation"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "org_agents"} - tables)
        if missing:
            raise ValueError("Database schema missing required tables: {}".format(", ".join(missing)))

        browser.emit("New organisation wizard")
        browser.emit("-----------------------")

        organisation = browser.prompt_text("Organisation name", default="").strip()
        if not organisation:
            raise ValueError("Organisation name cannot be blank.")

        organisation_sort = browser.prompt_text("Organisation sort name", default=organisation).strip() or organisation
        organisation_aliases = _split_aliases(browser.prompt_text("Organisation aliases (comma separated)", default=""))
        organisation_note = _clean_optional(browser.prompt_text("Organisation note", default=""))
        organisation_legal_name = _clean_optional(browser.prompt_text("Organisation legal name", default=""))
        organisation_trading_name = _clean_optional(browser.prompt_text("Organisation trading name", default=""))
        organisation_registration_id = _clean_optional(browser.prompt_text("Organisation registration id", default=""))
        organisation_jurisdiction = _clean_optional(browser.prompt_text("Organisation jurisdiction", default=""))
        organisation_founded_date = _clean_optional(browser.prompt_text("Organisation founded date", default=""))
        organisation_dissolved_date = _clean_optional(browser.prompt_text("Organisation dissolved date", default=""))
        organisation_website = _clean_optional(browser.prompt_text("Organisation website", default=""))
        organisation_contact_email = _clean_optional(browser.prompt_text("Organisation contact email", default=""))
        organisation_description = _clean_optional(browser.prompt_text("Organisation description", default=""))

        parent_id_text = browser.prompt_text("Parent organisation agent id (optional)", default="")
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent organisation agent id must be an integer.")
        if parent_id is not None:
            if browser.db.get_row_from_id("agents", parent_id) is None:
                raise ValueError("No agent exists with agent_id={}.".format(parent_id))

        organisation_relation_type = (
            browser.prompt_text("Organisation relation type", default="imprint_of").strip() or "imprint_of"
        )
        organisation_relation_note = _clean_optional(browser.prompt_text("Organisation relation note", default=""))

        language_text = browser.prompt_text("Organisation language (optional)", default="").strip()
        if language_text:
            organisation_language: Optional[str | int]
            organisation_language = int(language_text) if language_text.isdigit() else language_text
        else:
            organisation_language = None
        organisation_synopsis = _clean_optional(browser.prompt_text("Organisation synopsis", default=""))

        existing = browser.db.search("agents", "agent_canonical_name", organisation)
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
                "Possible duplicate organisation exists: agent_id={} name={!r}".format(
                    existing[0]["agent_id"],
                    existing[0]["agent_canonical_name"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another organisation with this name?", default=False)
            if not proceed_duplicate:
                raise ValueError("Organisation wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("name", organisation),
                        ("sort", organisation_sort),
                        ("website", organisation_website or ""),
                        ("parent_agent_id", parent_id if parent_id is not None else ""),
                    ],
                )
            ],
            title="Organisation summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this organisation now?", default=True)
        if not proceed:
            raise ValueError("Organisation wizard canceled.")

        note = organisation_note
        if organisation_relation_note:
            relation_line = "organisation_relation_note={}".format(organisation_relation_note)
            note = "{}\n{}".format(note, relation_line) if note else relation_line
        language_ids = []
        if organisation_language is not None:
            language_id = best_effort_language_id(
                browser.db,
                organisation_language,
                default=None,
                strict=False,
            )
            if language_id is None:
                raise ValueError("Organisation language could not be resolved.")
            language_ids.append(int(language_id))
        identifiers = (
            [{"scheme": "url", "value": organisation_website, "is_primary": True}]
            if organisation_website is not None
            else []
        )

        catalog = Catalog(browser.db)
        agent_id = catalog.agents.create_organisation(
            {
                "name": organisation,
                "sort_name": organisation_sort,
                "aliases": organisation_aliases or (),
                "note": note,
            },
            details={
                "org_agent_legal_name": organisation_legal_name,
                "org_agent_trading_name": organisation_trading_name,
                "org_agent_registration_id": organisation_registration_id,
                "org_agent_jurisdiction": organisation_jurisdiction,
                "org_agent_founded_date": organisation_founded_date,
                "org_agent_dissolved_date": organisation_dissolved_date,
                "org_agent_website": organisation_website,
                "org_agent_contact_email": organisation_contact_email,
                "org_agent_description": organisation_description,
            },
            parent_id=parent_id,
            relation_type=organisation_relation_type,
            relation_note=organisation_relation_note,
            identifiers=identifiers,
            language_ids=language_ids,
            synopses=[organisation_synopsis] if organisation_synopsis is not None else (),
        )
        row = catalog.agents.require(agent_id)

        browser.emit(
            "Organisation created: agent_id={} canonical_name={!r}".format(
                row["agent_id"],
                row["agent_canonical_name"],
            )
        )
        return True
