"""Interactive wizard command for adding titles (and WEMI chain) entries."""

from __future__ import annotations

import os
from typing import Optional

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms


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


def _split_break_joined(value: Optional[str]) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(part for part in value.split("(#BREAK#)") if part)


def _epoch_ms(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(to_epoch_ms(value))
    except Exception:
        return None


def _extract_year(value: Optional[str]) -> Optional[int]:
    if value is not None and len(value.strip()) >= 4 and value.strip()[:4].isdigit():
        return int(value.strip()[:4])
    return None


def _guess_format_detail(*values: str) -> Optional[str]:
    extensions = {
        extension.lstrip(".").lower()
        for value in values
        for _, extension in (os.path.splitext(value.strip()),)
        if extension
    }
    return next(iter(extensions)).upper() if len(extensions) == 1 else None


def _guess_carrier_type(format_detail: Optional[str]) -> Optional[str]:
    if format_detail is None:
        return None
    format_name = format_detail.lower()
    if format_name in {"epub", "pdf", "mobi", "azw3", "cbz", "cbr", "djvu", "fb2", "txt", "rtf", "docx"}:
        return "ebook"
    if format_name in {"mp3", "m4b", "flac", "ogg", "aac", "wav"}:
        return "audiobook"
    if format_name in {"mp4", "mkv", "avi"}:
        return "video"
    return None


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

        paths = _split_break_joined(source_paths)
        names = _split_break_joined(source_names)
        format_detail = _guess_format_detail(*names, *paths)
        item_count = max(len(paths), len(names))
        if item_count == 0 and title_source is not None:
            item_count = 1
        items = []
        for index in range(item_count):
            source_path = paths[index] if index < len(paths) else None
            source_name = names[index] if index < len(names) else None
            if source_name is None and source_path is not None:
                source_name = os.path.basename(source_path)
            items.append(
                {
                    "item_type": "digital" if source_name or source_path else None,
                    "item_source": title_source,
                    "item_source_path": source_path,
                    "item_source_name": source_name,
                }
            )

        publication_year = _extract_year(title_pub_date)
        copyright_date = title_copyright_date or title_pub_date
        catalog = Catalog(browser.db)
        created = catalog.mutations.writer.create_wemi_stack(
            work={
                "work_title": title,
                "work_canonical_title": title,
                "work_sort_title": title_sort_value,
                "work_creator_sort": title_creator_sort,
                "work_type": title_type,
                "work_original_date": _epoch_ms(title_pub_date),
                "work_original_year": publication_year or _extract_year(title_copyright_date),
                "work_original_copyright_date": copyright_date,
                "work_wikipedia_link": title_wikipedia,
                "work_discovery_note": title_source,
            },
            expression={
                "expression_subtitle": None,
                "expression_title_override": None,
                "expression_type": None,
                "expression_label": None,
                "expression_year": publication_year,
                "expression_is_preferred": 1,
                "expression_original_date": _epoch_ms(title_pub_date),
                "expression_original_copyright_date": copyright_date,
                "expression_wordcount": title_wordcount,
                "expression_fiction_length_category": title_fiction_length_category,
            },
            manifestation={
                "manifestation_subtitle": None,
                "manifestation_carrier_type": _guess_carrier_type(format_detail),
                "manifestation_format_detail": format_detail,
                "manifestation_pub_year": publication_year,
                "manifestation_pub_date": title_pub_date,
                "manifestation_status": None,
                "manifestation_note": None,
            },
            items=items,
            origin=title_source,
        )

        browser.emit("Title created:")
        browser.emit("  work_id={}".format(created.work_id))
        browser.emit("  expression_id={}".format(created.expression_id))
        browser.emit("  manifestation_id={}".format(created.manifestation_id))
        browser.emit("  items_created={}".format(len(created.item_ids)))
        browser.emit("  title_work_id={}".format(created.work_id))

        return True
