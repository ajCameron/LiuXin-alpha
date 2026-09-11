"""
Collect series metadata, create its catalog record, and optionally link an existing creator agent.

Series creation and agent linking are separate Core operations. A linking or
output failure can leave the new series in place; no compensating cleanup occurs here.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.standardization import (
    make_title_search_term,
    make_series_phash,
)
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort


def _clean_optional(value: str) -> Optional[str]:
    """
    Strip optional prompt text and represent blank results as ``None``.

    Example:
        >>> _clean_optional(" Cycle/Part "), _clean_optional(" ")
        ('Cycle/Part', None)


    :param value: Prompt value converted to text before stripping.
    :return: Nonblank stripped text or ``None`` for an empty result.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse an optional integer prompt, returning ``None`` for blank or invalid text.

    No positivity or series-position constraint is enforced here; stringification
    occurs outside the integer-conversion exception handler.

    Example:
        >>> _safe_int("0"), _safe_int("")
        (0, None)


    :param value: Prompt text for a parent/creator ID or position.
    :return: Parsed integer or ``None`` when blank/invalid.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewSeriesWizardCommand(TerminalCommandAPI):
    """
    Prompt for a series, sort/hash defaults, hierarchy details, and an optional creator link.

    Parent and creator rows must already exist. Confirmed creation uses supported
    schema fields; the optional creator relation is attempted only afterward.

    Example:
        >>> NewSeriesWizardCommand().usage
        'add series'
    """

    group = "add"
    name = "series"
    aliases = (
        "new-series",
        "new_series",
        "add-series",
        "add_series",
    )
    summary = "Interactive wizard to add a series."
    usage = "add series"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Validate prompted series data, create and reload the row, then optionally link the creator at priority zero.

        Duplicate lookup uses phash when supported, otherwise series text. Optional
        fields absent from the schema are omitted after prompting/validation.
        Creator-link lookup errors are treated as no link and followed by a new
        relation request. No rollback is attempted if reload, linking, or output
        fails after creation.

        Example:
            >>> NewSeriesWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing schema/entity/relation reads, prompts, Core writes, and output.
        :param args: Must be empty; series and creator choices are prompted.
        :return: ``True`` after creating the series and handling the optional creator relation.
        :raises ValueError: For invalid input/schema/parent/creator or declined confirmation.
        :raises RuntimeError: If the newly created series cannot be reloaded by its returned ID.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        if "series" not in tables:
            raise ValueError("Database schema does not contain `series` table.")
        columns = set(browser.db.get_column_headings("series"))

        browser.emit("New series wizard")
        browser.emit("----------------")

        series_name = browser.prompt_text("Series", default="").strip()
        if not series_name:
            raise ValueError("Series cannot be blank.")

        default_sort = generate_title_sort(series_name)
        series_sort = (
            browser.prompt_text("Series sort", default=default_sort).strip()
            or default_sort
        )

        default_phash = make_series_phash("", series_name)
        series_phash = (
            browser.prompt_text("Series phash", default=default_phash).strip()
            or default_phash
        )

        parent_id_text = browser.prompt_text("Parent series id (optional)", default="")
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent series id must be an integer.")
        parent_row = None
        if parent_id is not None:
            parent_row = browser.db.get_row_from_id("series", parent_id)
            if parent_row is None:
                raise ValueError(
                    "No series exists with series_id={}.".format(parent_id)
                )

        parent_position_text = browser.prompt_text(
            "Parent position (optional)", default=""
        )
        parent_position = _safe_int(parent_position_text)
        if parent_position_text.strip() and parent_position is None:
            raise ValueError("Parent position must be an integer.")

        series_full = _clean_optional(
            browser.prompt_text("Series full path (optional)", default="")
        )
        over_author = browser.prompt_yes_no("Series over author?", default=False)

        creator_agent_id_text = browser.prompt_text(
            "Creator agent id to link (optional)", default=""
        )
        creator_agent_id = _safe_int(creator_agent_id_text)
        if creator_agent_id_text.strip() and creator_agent_id is None:
            raise ValueError("Creator agent id must be an integer.")
        creator_row = None
        if creator_agent_id is not None:
            if "agents" not in tables:
                raise ValueError("Schema has no `agents` table for linking creator.")
            creator_row = browser.db.get_row_from_id("agents", creator_agent_id)
            if creator_row is None:
                raise ValueError(
                    "No agent exists with agent_id={}.".format(creator_agent_id)
                )

        duplicate_column = "series_phash" if "series_phash" in columns else "series"
        duplicate_term = (
            series_phash if duplicate_column == "series_phash" else series_name
        )
        existing = browser.db.search("series", duplicate_column, duplicate_term)
        if existing:
            browser.emit(
                "Possible duplicate series exists: series_id={} series={!r}".format(
                    existing[0]["series_id"],
                    existing[0]["series"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no(
                "Create another series with this value?", default=False
            )
            if not proceed_duplicate:
                raise ValueError("Series wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("series", series_name),
                        ("sort", series_sort),
                        ("phash", series_phash),
                        ("parent_id", parent_id if parent_id is not None else ""),
                        (
                            "creator_agent_id",
                            creator_agent_id if creator_agent_id is not None else "",
                        ),
                    ],
                )
            ],
            title="Series summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this series now?", default=True)
        if not proceed:
            raise ValueError("Series wizard canceled.")

        row_dict = {"series": series_name}
        if "series_sort" in columns:
            row_dict["series_sort"] = series_sort
        if "series_name_norm" in columns:
            row_dict["series_name_norm"] = make_title_search_term(series_name)
        if "series_phash" in columns:
            row_dict["series_phash"] = series_phash
        if "series_parent_id" in columns and parent_row is not None:
            row_dict["series_parent_id"] = parent_row.row_id
        if "series_parent_position" in columns and parent_position is not None:
            row_dict["series_parent_position"] = parent_position
        if "series_full" in columns and series_full is not None:
            row_dict["series_full"] = series_full
        if "series_over_author" in columns:
            row_dict["series_over_author"] = int(bool(over_author))

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={"repository": "series", "data": row_dict},
        )
        series_row = browser.db.get_row_from_id(
            "series",
            int(result["entity_id"]),
        )
        if series_row is None:
            raise RuntimeError("Core did not return the created series row.")

        if creator_row is not None:
            try:
                existing_link = browser.db.get_interlink_row(
                    primary_row=series_row, secondary_row=creator_row
                )
            except Exception:
                existing_link = None
            if existing_link is None:
                browser.execute_core_command(
                    "admin.relation.link",
                    payload={
                        "table": "series",
                        "row_id": int(series_row.row_id),
                        "related_table": str(creator_row.table),
                        "related_row_id": int(creator_row.row_id),
                        "priority": 0,
                    },
                )

        browser.emit(
            "Series created: series_id={} series={!r}".format(
                series_row["series_id"],
                series_row["series"],
            )
        )
        return True
