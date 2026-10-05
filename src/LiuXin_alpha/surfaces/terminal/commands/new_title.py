"""
Collect a title and provenance, then ask Core to create a work/expression/manifestation/item stack.

This flow does not insert a separate legacy title row. Source names/paths are
metadata used to infer item count and carrier hints, not files opened or verified
by the wizard.
"""

from __future__ import annotations

import os
from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms


def _clean_optional(value: str) -> Optional[str]:
    """
    Strip optional title prompt text and replace blank results with ``None``.

    Example:
        >>> _clean_optional(" Manual source "), _clean_optional(" ")
        ('Manual source', None)


    :param value: Prompt result stringified before stripping.
    :return: Nonblank text or ``None`` without metadata-specific validation.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse optional word-count text without imposing a nonnegative range.

    Example:
        >>> _safe_int("1200"), _safe_int("many")
        (1200, None)


    :param value: Prompt value stringified and stripped before integer conversion.
    :return: Parsed integer or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _to_break_joined(value: str) -> Optional[str]:
    """
    Normalize a source list to the legacy break marker, using commas only when no marker is present.

    Blank pieces are removed and order/duplicates preserved. If a break marker
    occurs anywhere, commas remain literal within those pieces; no escaping or
    quoted-value grammar is supported.

    Example:
        >>> _to_break_joined("a.epub, b.epub,,")
        'a.epub(#BREAK#)b.epub'
        >>> _to_break_joined("a,b(#BREAK#)c")
        'a,b(#BREAK#)c'


    :param value: Comma- or break-separated source path/name text.
    :return: Break-joined stripped nonblank pieces, or ``None`` if none remain.
    """
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
    """
    Split legacy break-joined text, dropping empty pieces without stripping their contents.

    Example:
        >>> _split_break_joined("a(#BREAK#)(#BREAK#) b "), _split_break_joined(None)
        (('a', ' b '), ())


    :param value: Optional break-joined string, normally already normalized by the companion helper.
    :return: Nonempty string pieces in order, or an empty tuple for absent/empty input.
    """
    if value is None:
        return ()
    return tuple(part for part in value.split("(#BREAK#)") if part)


def _epoch_ms(value: Optional[str]) -> Optional[int]:
    """
    Convert a prompted date to epoch milliseconds, silently returning ``None`` for failed conversion.

    Example:
        >>> _epoch_ms("1970-01-01"), _epoch_ms("not-a-date")
        (0, None)


    :param value: Optional timestamp-like text handled by the shared date converter.
    :return: Integer milliseconds or ``None`` for absence or an ordinary conversion exception.
    """
    if value is None:
        return None
    try:
        return int(to_epoch_ms(value))
    except Exception:
        return None


def _extract_year(value: Optional[str]) -> Optional[int]:
    """
    Take four leading digit characters as a year without validating a date or year range.

    Example:
        >>> _extract_year(" 2020-edition "), _extract_year("June 2020")
        (2020, None)


    :param value: Optional date-like string, stripped before examining its first four characters.
    :return: Integer prefix or ``None`` when absent, too short, or not digit-prefixed.
    """
    if value is not None and len(value.strip()) >= 4 and value.strip()[:4].isdigit():
        return int(value.strip()[:4])
    return None


def _guess_format_detail(*values: str) -> Optional[str]:
    """
    Infer one uppercase format suffix when all nonempty observed extensions agree.

    Extensionless inputs are ignored rather than vetoing a guess. Uses platform
    path-suffix parsing, not URI decoding, MIME detection, or file content reads.

    Example:
        >>> _guess_format_detail("book.epub", "copy.EPUB", "no-extension")
        'EPUB'
        >>> _guess_format_detail("book.epub", "book.pdf") is None
        True


    :param values: Source names/paths whose stripped final extensions are inspected.
    :return: Unique normalized extension in uppercase, or ``None`` for no suffixes or conflicting suffixes.
    """
    extensions = {
        extension.lstrip(".").lower()
        for value in values
        for _, extension in (os.path.splitext(value.strip()),)
        if extension
    }
    return next(iter(extensions)).upper() if len(extensions) == 1 else None


def _guess_carrier_type(format_detail: Optional[str]) -> Optional[str]:
    """
    Map a recognized format label to ebook, audiobook, or video using a fixed suffix list.

    Matching lowercases but does not strip text. Unsupported formats remain
    unspecified; this is not a capability or media-content probe.

    Example:
        >>> _guess_carrier_type("EPUB"), _guess_carrier_type("mp3"), _guess_carrier_type("unknown")
        ('ebook', 'audiobook', None)


    :param format_detail: Optional extension-like format label, normally from the guessing helper.
    :return: Carrier category or ``None`` for absent/unrecognized input.
    """
    if format_detail is None:
        return None
    format_name = format_detail.lower()
    if format_name in {
        "epub",
        "pdf",
        "mobi",
        "azw3",
        "cbz",
        "cbr",
        "djvu",
        "fb2",
        "txt",
        "rtf",
        "docx",
    }:
        return "ebook"
    if format_name in {"mp3", "m4b", "flac", "ogg", "aac", "wav"}:
        return "audiobook"
    if format_name in {"mp4", "mkv", "avi"}:
        return "video"
    return None


class NewTitleWizardCommand(TerminalCommandAPI):
    """
    Prompt for a title and ask Core to create its linked WEMI stack with inferred source items.

    Source paths and names are paired by position, with basename fallbacks for
    unnamed paths. A nonblank source label creates one placeholder item even when
    no paths or names are supplied.

    Example:
        >>> NewTitleWizardCommand().usage
        'add title'
    """

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
        """
        Validate title/word-count prompts, confirm, infer item and format hints, and submit ``catalog.wemi.create``.

        Exact canonical-title matches trigger advisory duplicate confirmation.
        Item count is the larger path/name list length, or one for source-only
        provenance. Backing files are not opened. Dates use permissive millisecond
        conversion plus independent four-digit year extraction; those results can
        disagree for malformed date text. Copyright defaults to publication text.

        Core owns stack creation semantics. The adapter does not roll back after
        missing result fields or a later output failure.

        Example:
            >>> NewTitleWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing work search, prompts, Core WEMI creation, and output.
        :param args: Must be empty; title and source information are collected interactively.
        :return: ``True`` after reporting Core's work/expression/manifestation/item identifiers.
        :raises ValueError: For arguments, absent works table, blank title, invalid word count, or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        if "works" not in tables:
            raise ValueError(
                "Database schema does not contain `works`; WEMI title flow is unavailable."
            )

        browser.emit("New title wizard")
        browser.emit("----------------")

        title = browser.prompt_text("Title", default="").strip()
        if not title:
            raise ValueError("Title cannot be blank.")

        default_sort = title_sort(title)
        title_sort_value = (
            browser.prompt_text("Title sort", default=default_sort).strip()
            or default_sort
        )
        title_creator_sort = _clean_optional(
            browser.prompt_text("Creator sort", default="")
        )
        title_pub_date = _clean_optional(
            browser.prompt_text("Publication date", default="")
        )
        title_copyright_date = _clean_optional(
            browser.prompt_text("Copyright date", default="")
        )
        title_wikipedia = _clean_optional(
            browser.prompt_text("Wikipedia URL", default="")
        )
        title_fiction_length_category = _clean_optional(
            browser.prompt_text("Fiction length category", default="")
        )
        title_type = _clean_optional(browser.prompt_text("Title type", default=""))

        wordcount_text = browser.prompt_text("Wordcount", default="")
        title_wordcount = _safe_int(wordcount_text)
        if wordcount_text.strip() and title_wordcount is None:
            raise ValueError("Wordcount must be an integer.")

        title_source = _clean_optional(
            browser.prompt_text("Source", default="manual_terminal_add")
        )
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
            proceed_duplicate = browser.prompt_yes_no(
                "Create another title with this canonical title?", default=False
            )
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
                        (
                            "wordcount",
                            title_wordcount if title_wordcount is not None else "",
                        ),
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
        created = browser.execute_core_command(
            "catalog.wemi.create",
            payload={
                "work": {
                    "work_title": title,
                    "work_canonical_title": title,
                    "work_sort_title": title_sort_value,
                    "work_creator_sort": title_creator_sort,
                    "work_type": title_type,
                    "work_original_date": _epoch_ms(title_pub_date),
                    "work_original_year": publication_year
                    or _extract_year(title_copyright_date),
                    "work_original_copyright_date": copyright_date,
                    "work_wikipedia_link": title_wikipedia,
                    "work_discovery_note": title_source,
                },
                "expression": {
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
                "manifestation": {
                    "manifestation_subtitle": None,
                    "manifestation_carrier_type": _guess_carrier_type(format_detail),
                    "manifestation_format_detail": format_detail,
                    "manifestation_pub_year": publication_year,
                    "manifestation_pub_date": title_pub_date,
                    "manifestation_status": None,
                    "manifestation_note": None,
                },
                "items": items,
                "origin": title_source,
            },
        )

        browser.emit("Title created:")
        browser.emit("  work_id={}".format(created["work_id"]))
        browser.emit("  expression_id={}".format(created["expression_id"]))
        browser.emit("  manifestation_id={}".format(created["manifestation_id"]))
        browser.emit("  items_created={}".format(len(created.get("item_ids", ()))))
        browser.emit("  title_work_id={}".format(created["work_id"]))

        return True
