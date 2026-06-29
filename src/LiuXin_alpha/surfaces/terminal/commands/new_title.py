"""Interactive wizard command for adding titles (and WEMI chain) entries."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.catalog.metadata_tools import Add
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


def _to_break_joined(value: str) -> Optional[str]:
    text = str(value).strip()
    if not text:
        return None
    if "(#BREAK#)" in text:
        parts = [part.strip() for part in text.split("(#BREAK#)") if part.strip()]
    else:
        parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return None
    return "(#BREAK#)".join(parts)


class NewTitleWizardCommand(TerminalCommandAPI):
    """Create a title and project it into work/expression/manifestation/items."""

    group = "add"
    name = "title"
    aliases = (
        "new-title",
        "new_title",
        "add-title",
        "add_title",
    )
    summary = "Interactive wizard to add a title and its WEMI stack."
    usage = "add title"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        if "works" not in tables:
            raise ValueError("Database schema does not contain `works`; WEMI title flow is unavailable.")

        browser.emit("New title wizard")
        browser.emit("----------------")

        title = browser.prompt_text("Title", default="").strip()
        if not title:
            raise ValueError("Title cannot be blank.")

        default_sort = title_sort(title)
        title_sort_value = browser.prompt_text("Title sort", default=default_sort).strip() or default_sort
        title_creator_sort = _clean_optional(browser.prompt_text("Creator sort", default=""))
        title_pub_date = _clean_optional(browser.prompt_text("Publication date", default=""))
        title_copyright_date = _clean_optional(browser.prompt_text("Copyright date", default=""))
        title_wikipedia = _clean_optional(browser.prompt_text("Wikipedia URL", default=""))
        title_fiction_length_category = _clean_optional(browser.prompt_text("Fiction length category", default=""))
        title_type = _clean_optional(browser.prompt_text("Title type", default=""))

        wordcount_text = browser.prompt_text("Wordcount", default="")
        title_wordcount = _safe_int(wordcount_text)
        if wordcount_text.strip() and title_wordcount is None:
            raise ValueError("Wordcount must be an integer.")

        title_source = _clean_optional(browser.prompt_text("Source", default="manual_terminal_add"))
        source_paths = _to_break_joined(
            browser.prompt_text(
                "Source paths (comma or (#BREAK#) separated)",
                default="",
            )
        )
        source_names = _to_break_joined(
            browser.prompt_text(
                "Source names (comma or (#BREAK#) separated)",
                default="",
            )
        )

        if source_paths and not source_names:
            source_names = None

        existing = browser.db.search("works", "work_canonical_title", title)
        if existing:
            browser.emit(
                "Possible duplicate work exists: work_id={} canonical_title={!r}".format(
                    existing[0]["work_id"],
                    existing[0]["work_canonical_title"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another title with this canonical title?", default=False)
            if not proceed_duplicate:
                raise ValueError("Title wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("title", title),
                        ("sort", title_sort_value),
                        ("creator_sort", title_creator_sort or ""),
                        ("pub_date", title_pub_date or ""),
                        ("type", title_type or ""),
                        ("wordcount", title_wordcount if title_wordcount is not None else ""),
                        ("source", title_source or ""),
                        ("source_paths", source_paths or ""),
                    ],
                )
            ],
            title="Title summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create title + WEMI stack now?", default=True)
        if not proceed:
            raise ValueError("Title wizard canceled.")

        add = Add(browser.db)
        title_row = add.title(
            title=title,
            title_sort=title_sort_value,
            title_creator_sort=title_creator_sort,
            title_pub_date=title_pub_date,
            title_copyright_date=title_copyright_date,
            title_wikipedia=title_wikipedia,
            title_fiction_length_category=title_fiction_length_category,
            title_type=title_type,
            title_wordcount=title_wordcount,
            title_source=title_source,
            title_source_path=source_paths,
            title_source_name=source_names,
        )

        bundle = getattr(add, "_last_title_wemi_bundle", None) or {}
        work_row = bundle.get("work")
        expression_row = bundle.get("expression")
        manifestation_row = bundle.get("manifestation")
        item_rows = bundle.get("items") or []

        browser.emit("Title created:")
        if work_row is not None:
            browser.emit("  work_id={}".format(work_row["work_id"]))
        if expression_row is not None:
            browser.emit("  expression_id={}".format(expression_row["expression_id"]))
        if manifestation_row is not None:
            browser.emit("  manifestation_id={}".format(manifestation_row["manifestation_id"]))
        browser.emit("  items_created={}".format(len(item_rows)))

        try:
            work_id = title_row["work_id"]
        except Exception:
            work_id = None
        if work_id is not None:
            browser.emit("  title_work_id={}".format(work_id))

        return True
