"""
Prompt for organisation-agent metadata, optional parent relation, and associated language/identifier facts.

The terminal submits one Core organisation-creation command. Parent validation
checks agent existence, not organisation type; deeper validity belongs to Core.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    """
    Convert an optional prompt result to stripped text or ``None`` when blank.

    Example:
        >>> _clean_optional(" Legal name "), _clean_optional("")
        ('Legal name', None)


    :param value: Prompt result stringified before stripping.
    :return: Nonblank text or ``None``; dates, addresses, and identifiers are not validated here.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse an optional integer ID, distinguishing nonblank validity only through the caller's input check.

    Example:
        >>> _safe_int("12"), _safe_int("invalid"), _safe_int("")
        (12, None, None)


    :param value: Prompt result stringified and stripped before integer conversion.
    :return: Integer without positivity checks, or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _split_aliases(raw: str) -> Optional[list[str]]:
    """
    Split comma-separated aliases, dropping blank pieces but preserving duplicates and case.

    Example:
        >>> _split_aliases(" Archive, ,Archive,Press ")
        ['Archive', 'Archive', 'Press']


    :param raw: Alias prompt text, with no quoting or escaped-comma grammar.
    :return: Stripped alias strings in input order, or ``None`` when none remain.
    """
    text = str(raw).strip()
    if not text:
        return None
    parts = [part.strip() for part in text.split(",")]
    out = [part for part in parts if part]
    return out or None


class NewOrganisationWizardCommand(TerminalCommandAPI):
    """
    Create an organisation agent with prompted descriptive details and optional parent/language relations.

    Exact-name matches of organisation type require duplicate confirmation. Optional
    dates, contact details, and relation-type text are passed on without local semantic validation.

    Example:
        >>> NewOrganisationWizardCommand().usage
        'add organisation'
    """

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
        """
        Gather organisation data, validate parent existence, confirm, resolve language, and submit Core creation.

        Parent lookup accepts any existing agent type. Unreadable type fields are
        skipped during duplicate filtering. Relation notes are included both in
        the general note text and in the parent-relation payload. Language is
        resolved after confirmation but before mutation. The wizard provides no
        rollback for a successful write followed by result/output failure.

        Example:
            >>> NewOrganisationWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host supplying schema/agent/language reads, prompts, Core creation, and output.
        :param args: Must be empty; all organisation choices are collected interactively.
        :return: ``True`` after reporting the created organisation agent.
        :raises ValueError: For invalid arguments/schema/name/parent/language or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "org_agents"} - tables)
        if missing:
            raise ValueError(
                "Database schema missing required tables: {}".format(", ".join(missing))
            )

        browser.emit("New organisation wizard")
        browser.emit("-----------------------")

        organisation = browser.prompt_text("Organisation name", default="").strip()
        if not organisation:
            raise ValueError("Organisation name cannot be blank.")

        organisation_sort = (
            browser.prompt_text("Organisation sort name", default=organisation).strip()
            or organisation
        )
        organisation_aliases = _split_aliases(
            browser.prompt_text("Organisation aliases (comma separated)", default="")
        )
        organisation_note = _clean_optional(
            browser.prompt_text("Organisation note", default="")
        )
        organisation_legal_name = _clean_optional(
            browser.prompt_text("Organisation legal name", default="")
        )
        organisation_trading_name = _clean_optional(
            browser.prompt_text("Organisation trading name", default="")
        )
        organisation_registration_id = _clean_optional(
            browser.prompt_text("Organisation registration id", default="")
        )
        organisation_jurisdiction = _clean_optional(
            browser.prompt_text("Organisation jurisdiction", default="")
        )
        organisation_founded_date = _clean_optional(
            browser.prompt_text("Organisation founded date", default="")
        )
        organisation_dissolved_date = _clean_optional(
            browser.prompt_text("Organisation dissolved date", default="")
        )
        organisation_website = _clean_optional(
            browser.prompt_text("Organisation website", default="")
        )
        organisation_contact_email = _clean_optional(
            browser.prompt_text("Organisation contact email", default="")
        )
        organisation_description = _clean_optional(
            browser.prompt_text("Organisation description", default="")
        )

        parent_id_text = browser.prompt_text(
            "Parent organisation agent id (optional)", default=""
        )
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent organisation agent id must be an integer.")
        if parent_id is not None:
            if browser.db.get_row_from_id("agents", parent_id) is None:
                raise ValueError("No agent exists with agent_id={}.".format(parent_id))

        organisation_relation_type = (
            browser.prompt_text(
                "Organisation relation type", default="imprint_of"
            ).strip()
            or "imprint_of"
        )
        organisation_relation_note = _clean_optional(
            browser.prompt_text("Organisation relation note", default="")
        )

        language_text = browser.prompt_text(
            "Organisation language (optional)", default=""
        ).strip()
        if language_text:
            organisation_language: Optional[str | int]
            organisation_language = (
                int(language_text) if language_text.isdigit() else language_text
            )
        else:
            organisation_language = None
        organisation_synopsis = _clean_optional(
            browser.prompt_text("Organisation synopsis", default="")
        )

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
            proceed_duplicate = browser.prompt_yes_no(
                "Create another organisation with this name?", default=False
            )
            if not proceed_duplicate:
                raise ValueError(
                    "Organisation wizard canceled to avoid duplicate entry."
                )

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
            relation_line = "organisation_relation_note={}".format(
                organisation_relation_note
            )
            note = "{}\n{}".format(note, relation_line) if note else relation_line
        language_ids = []
        if organisation_language is not None:
            language_id = browser.resolve_language_id(organisation_language)
            if language_id is None:
                raise ValueError("Organisation language could not be resolved.")
            language_ids.append(int(language_id))
        identifiers = (
            [{"scheme": "url", "value": organisation_website, "is_primary": True}]
            if organisation_website is not None
            else []
        )

        result = browser.execute_core_command(
            "catalog.agent.create-organisation",
            payload={
                "data": {
                    "name": organisation,
                    "sort_name": organisation_sort,
                    "aliases": organisation_aliases or (),
                    "note": note,
                },
                "details": {
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
                "parent_id": parent_id,
                "relation_type": organisation_relation_type,
                "relation_note": organisation_relation_note,
                "identifiers": identifiers,
                "language_ids": language_ids,
                "synopses": (
                    [organisation_synopsis] if organisation_synopsis is not None else []
                ),
            },
        )
        row = dict(result["agent"])

        browser.emit(
            "Organisation created: agent_id={} canonical_name={!r}".format(
                row["agent_id"],
                row["agent_canonical_name"],
            )
        )
        return True
