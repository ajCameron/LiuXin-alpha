"""Interactive wizard command for adding work entries."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.metadata_tools.add import Add
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort


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


class NewWorkWizardCommand(TerminalCommandAPI):
    """Create a work row through guided prompts."""

    group = "add"
    name = "work"
    aliases = ("new-work", "new_work", "add-work", "add_work")
    summary = "Interactive wizard to add a work."
    usage = "add work"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "works" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `works` table.")

        browser.emit("New work wizard")
        browser.emit("---------------")

        work_title = browser.prompt_text("Work title", default="").strip()
        if not work_title:
            raise ValueError("Work title cannot be blank.")

        default_canonical = work_title
        default_sort = title_sort(work_title)

        work_canonical_title = browser.prompt_text("Work canonical title", default=default_canonical).strip() or default_canonical
        work_sort_title = browser.prompt_text("Work sort title", default=default_sort).strip() or default_sort
        work_creator_sort = _clean_optional(browser.prompt_text("Work creator sort", default=""))
        work_type = _clean_optional(browser.prompt_text("Work type", default=""))
        work_medium = _clean_optional(browser.prompt_text("Work medium", default=""))
        work_original_language = _clean_optional(browser.prompt_text("Work original language", default=""))
        work_original_date = _clean_optional(browser.prompt_text("Work original date", default=""))

        work_original_year_text = browser.prompt_text("Work original year", default="")
        work_original_year = _safe_int(work_original_year_text)
        if work_original_year_text.strip() and work_original_year is None:
            raise ValueError("Work original year must be an integer.")

        work_wikipedia_link = _clean_optional(browser.prompt_text("Work Wikipedia link", default=""))
        work_is_fiction = int(browser.prompt_yes_no("Is fiction?", default=True))
        work_audience = _clean_optional(browser.prompt_text("Work audience", default=""))
        work_completion_status = _clean_optional(browser.prompt_text("Work completion status", default=""))
        work_discovery_note = _clean_optional(browser.prompt_text("Work discovery note", default=""))

        existing = browser.db.search("works", "work_canonical_title", work_canonical_title)
        if existing:
            browser.emit(
                "Possible duplicate work exists: work_id={} canonical_title={!r}".format(
                    existing[0]["work_id"],
                    existing[0]["work_canonical_title"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another work with this canonical title?", default=False)
            if not proceed_duplicate:
                raise ValueError("Work wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("title", work_title),
                        ("canonical_title", work_canonical_title),
                        ("sort_title", work_sort_title),
                        ("type", work_type or ""),
                        ("medium", work_medium or ""),
                        ("original_language", work_original_language or ""),
                        ("original_year", work_original_year if work_original_year is not None else ""),
                        ("is_fiction", bool(work_is_fiction)),
                    ],
                )
            ],
            title="Work summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this work now?", default=True)
        if not proceed:
            raise ValueError("Work wizard canceled.")

        add = Add(browser.db)
        work_row = add.work(
            work_title=work_title,
            work_canonical_title=work_canonical_title,
            work_sort_title=work_sort_title,
            work_creator_sort=work_creator_sort,
            work_type=work_type,
            work_medium=work_medium,
            work_original_language=work_original_language,
            work_original_date=work_original_date,
            work_original_year=work_original_year,
            work_wikipedia_link=work_wikipedia_link,
            work_is_fiction=work_is_fiction,
            work_audience=work_audience,
            work_completion_status=work_completion_status,
            work_discovery_note=work_discovery_note,
        )

        browser.emit(
            "Work created: work_id={} title={!r}".format(
                work_row["work_id"],
                work_row["work_title"],
            )
        )
        return True
