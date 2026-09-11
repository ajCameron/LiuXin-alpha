"""
Collect a human creator's names, biographical details, identifiers, and optional language for Core.

Creation is submitted as one person-agent command. Role/single-person/seminal-work
metadata is encoded in note text; the terminal does not directly insert agent or
human-detail rows or attach the creator to a work.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.constants import CREATOR_TYPES
from LiuXin_alpha.metadata.utils import author_to_author_sort


def _clean_optional(value: str) -> Optional[str]:
    """
    Stringify optional prompt text, strip its edges, and replace blank results with ``None``.

    Example:
        >>> _clean_optional(" Pen name "), _clean_optional(" ")
        ('Pen name', None)


    :param value: Prompt result converted to text before stripping.
    :return: Nonblank stripped text or ``None``; dates and URLs are not validated here.
    """
    text = str(value).strip()
    return text or None


class NewCreatorWizardCommand(TerminalCommandAPI):
    """
    Prompt for a person creator and submit generic-agent plus human-detail data to Core.

    Creator type is checked against the configured role set. Canonical-name matches
    of person type trigger an advisory duplicate confirmation rather than automatic reuse.

    Example:
        >>> NewCreatorWizardCommand().usage
        'add creator'
    """

    group = "add"
    name = "creator"
    aliases = ("new-creator", "new_creator", "add-creator", "add_creator")
    summary = "Interactive wizard to add a creator (human agent)."
    usage = "add creator"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Gather creator fields, confirm creation, resolve optional language, and call the person-agent endpoint.

        Name parts use simple comma/space splitting, with first/last tokens used
        as given/family names; this is not a general personal-name parser. Short
        and legal names become aliases when different from the canonical name.
        Optional dates/URLs remain text. Language resolution follows confirmation
        but precedes the write. Result/output failures after creation are not undone.

        Example:
            >>> NewCreatorWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing schema/search/language reads, prompts, Core person creation, and output.
        :param args: Must be empty; creator data is gathered interactively.
        :return: ``True`` after reporting the created agent.
        :raises ValueError: For arguments, missing required tables/name, invalid role/language, or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "human_agents"} - tables)
        if missing:
            raise ValueError(
                "Database schema missing required tables: {}".format(", ".join(missing))
            )

        browser.emit("New creator wizard")
        browser.emit("------------------")

        creator_name = browser.prompt_text("Creator canonical name", default="").strip()
        if not creator_name:
            raise ValueError("Creator name cannot be blank.")

        creator_type = (
            browser.prompt_text("Creator type", default="authors").strip().lower()
            or "authors"
        )
        valid_creator_types = sorted({str(item).lower() for item in CREATOR_TYPES})
        if creator_type not in valid_creator_types:
            raise ValueError(
                "Unrecognized creator type {!r}. Valid types include: {}".format(
                    creator_type,
                    ", ".join(valid_creator_types[:20]),
                )
            )

        default_sort = author_to_author_sort(creator_name)
        creator_sort = (
            browser.prompt_text("Creator sort name", default=default_sort).strip()
            or default_sort
        )
        creator_short_name = _clean_optional(
            browser.prompt_text("Creator short name", default="")
        )
        creator_legal_name = _clean_optional(
            browser.prompt_text("Creator legal name", default="")
        )
        creator_birth_date = _clean_optional(
            browser.prompt_text("Creator birth date (YYYY-MM-DD)", default="")
        )
        creator_death_date = _clean_optional(
            browser.prompt_text("Creator death date (YYYY-MM-DD)", default="")
        )
        creator_language = _clean_optional(
            browser.prompt_text("Creator language", default="")
        )
        creator_bio = _clean_optional(
            browser.prompt_text("Creator biography/note", default="")
        )
        creator_wikipedia = _clean_optional(
            browser.prompt_text("Creator Wikipedia URL", default="")
        )
        creator_imdb = _clean_optional(
            browser.prompt_text("Creator IMDB id", default="")
        )
        creator_link = _clean_optional(
            browser.prompt_text("Creator external URL", default="")
        )
        creator_seminal_work = _clean_optional(
            browser.prompt_text("Creator seminal work", default="")
        )
        creator_one_person = browser.prompt_yes_no(
            "Single-person attribution?", default=True
        )

        existing = self._find_existing_person_agent(browser, creator_name)
        if existing is not None:
            browser.emit(
                "Possible duplicate creator exists: agent_id={} name={!r}".format(
                    existing["agent_id"],
                    existing["agent_canonical_name"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no(
                "Create another creator with this name?", default=False
            )
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

        aliases = [
            value
            for value in (creator_short_name, creator_legal_name)
            if value and value != creator_name
        ]
        note_lines = [
            "creator_type={}".format(creator_type),
            "creator_one_person={}".format(int(bool(creator_one_person))),
        ]
        if creator_seminal_work:
            note_lines.append("creator_seminal_work={}".format(creator_seminal_work))
        name_parts = [
            part for part in creator_name.replace(",", " ").split(" ") if part
        ]
        identifiers = [
            {"scheme": scheme, "value": value, "is_primary": is_primary}
            for scheme, value, is_primary in (
                ("wikipedia_url", creator_wikipedia, True),
                ("imdb_id", creator_imdb, False),
                ("url", creator_link, False),
            )
            if value is not None
        ]
        language_ids = []
        if creator_language is not None:
            language_id = browser.resolve_language_id(creator_language)
            if language_id is None:
                raise ValueError("Creator language could not be resolved.")
            language_ids.append(int(language_id))

        result = browser.execute_core_command(
            "catalog.agent.create-person",
            payload={
                "data": {
                    "name": creator_name,
                    "sort_name": creator_sort,
                    "aliases": aliases,
                    "note": "\n".join(note_lines),
                },
                "details": {
                    "human_agent_given_name": name_parts[0] if name_parts else None,
                    "human_agent_middle_name": (
                        " ".join(name_parts[1:-1]) if len(name_parts) > 2 else None
                    ),
                    "human_agent_family_name": name_parts[-1] if name_parts else None,
                    "human_agent_preferred_name": creator_short_name,
                    "human_agent_birth_date": creator_birth_date,
                    "human_agent_death_date": creator_death_date,
                    "human_agent_biography": creator_bio,
                },
                "identifiers": identifiers,
                "language_ids": language_ids,
                "notes": [creator_bio] if creator_bio is not None else [],
            },
        )
        creator_row = dict(result["agent"])

        browser.emit(
            "Creator created: agent_id={} canonical_name={!r}".format(
                creator_row["agent_id"],
                creator_row["agent_canonical_name"],
            )
        )
        return True

    def _find_existing_person_agent(self, browser, creator_name: str):
        """
        Return the first exact-name search result whose readable agent type lowercases to person.

        Per-row type lookup/conversion failures are skipped; the initial database
        search failure still propagates. No normalized or fuzzy name search occurs here.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock()
            >>> host.db.search.return_value = [{}, {"agent_type": "PERSON", "agent_id": 2}]
            >>> NewCreatorWizardCommand()._find_existing_person_agent(host, "Example")
            {'agent_type': 'PERSON', 'agent_id': 2}


        :param browser: Host exposing canonical agent-name search.
        :param creator_name: Exact canonical-name value forwarded to the search.
        :return: First person-typed matching row, or ``None`` if none qualifies.
        """
        rows = browser.db.search("agents", "agent_canonical_name", creator_name)
        for row in rows:
            try:
                if str(row["agent_type"]).lower() == "person":
                    return row
            except Exception:
                continue
        return None
