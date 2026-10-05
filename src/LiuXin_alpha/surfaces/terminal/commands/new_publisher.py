"""
Collect publisher metadata and submit it as an organisation-agent creation to Core.

Publisher hash, position, and hierarchy text are preserved as encoded aliases;
they are not direct publisher-table columns or relation-priority assignments here.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    """
    Normalize optional prompt text to a stripped nonblank string or ``None``.

    Example:
        >>> _clean_optional(" Publisher details "), _clean_optional(" ")
        ('Publisher details', None)


    :param value: Prompt result stringified before stripping.
    :return: Nonblank text or ``None`` without URL/hash validation.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse an optional parent ID or position, returning ``None`` for blank/invalid integer text.

    Example:
        >>> _safe_int("-1"), _safe_int("")
        (-1, None)


    :param value: Prompt result stringified and stripped before integer conversion.
    :return: Parsed integer without range constraints, or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewPublisherWizardCommand(TerminalCommandAPI):
    """
    Prompt for a publisher organisation, optional parent, web identifiers, and legacy metadata aliases.

    Possible duplicates are organisation-typed agents with the same canonical
    name; confirming them allows a creation attempt rather than reusing a row.

    Example:
        >>> NewPublisherWizardCommand().usage
        'add publisher'
    """

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
        """
        Validate prompted publisher fields, confirm, and create an organisation with an imprint parent relation.

        Parent validation checks existence of any agent, not publisher/organisation
        type. Hash, position, and full hierarchy text become prefixed aliases;
        hash is also sent as an identifier. Website and Wikipedia identifiers each
        request primary status for their schemes. Post-write result/output failures
        propagate without local compensation.

        Example:
            >>> NewPublisherWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing agent/schema reads, prompts, Core organisation creation, and output.
        :param args: Must be empty; publisher fields are collected by prompts.
        :return: ``True`` after reporting the created organisation agent.
        :raises ValueError: For arguments, missing schema/name, invalid parent/position, or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"agents", "org_agents"} - tables)
        if missing:
            raise ValueError(
                "Database schema missing required tables: {}".format(", ".join(missing))
            )

        browser.emit("New publisher wizard")
        browser.emit("-------------------")

        publisher = browser.prompt_text("Publisher name", default="").strip()
        if not publisher:
            raise ValueError("Publisher name cannot be blank.")

        publisher_sort = (
            browser.prompt_text("Publisher sort name", default=publisher).strip()
            or publisher
        )
        publisher_phash = _clean_optional(
            browser.prompt_text("Publisher phash", default="")
        )
        publisher_description = _clean_optional(
            browser.prompt_text("Publisher description", default="")
        )
        publisher_wikipedia = _clean_optional(
            browser.prompt_text("Publisher Wikipedia URL", default="")
        )
        publisher_website = _clean_optional(
            browser.prompt_text("Publisher website", default="")
        )

        parent_id_text = browser.prompt_text(
            "Parent publisher agent id (optional)", default=""
        )
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent publisher agent id must be an integer.")
        if parent_id is not None:
            if browser.db.get_row_from_id("agents", parent_id) is None:
                raise ValueError("No agent exists with agent_id={}.".format(parent_id))

        publishr_position_text = browser.prompt_text(
            "Publisher position (optional)", default=""
        )
        publishr_position = _safe_int(publishr_position_text)
        if publishr_position_text.strip() and publishr_position is None:
            raise ValueError("Publisher position must be an integer.")

        publisher_full = _clean_optional(
            browser.prompt_text("Publisher full hierarchy text", default="")
        )

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
            proceed_duplicate = browser.prompt_yes_no(
                "Create another publisher with this name?", default=False
            )
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
