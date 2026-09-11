"""
Collect an expression's descriptive fields and submit a standalone catalog creation to Core.

The wizard does not select a parent work. Numeric prompts are checked for integer
syntax, while failed original-date conversion becomes an absent value and unresolved
language IDs can be forwarded as ``None``.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.metadata.ebook_metadata_tools import to_epoch_ms
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    """
    Strip optional prompt text, using ``None`` for blank results.

    Example:
        >>> _clean_optional(" Edition "), _clean_optional(" ")
        ('Edition', None)


    :param value: Prompt value stringified before whitespace stripping.
    :return: Nonblank text or ``None`` without content validation.
    """
    text = str(value).strip()
    return text or None


def _clean_flags(value: str) -> tuple[str, ...]:
    """
    Split comma-separated flags, stripping pieces and deduplicating exact spellings in order.

    Example:
        >>> _clean_flags("draft, draft, Draft, ")
        ('draft', 'Draft')


    :param value: Comma-separated prompt text; case and internal whitespace remain significant.
    :return: Ordered tuple of unique nonblank flag strings.
    """
    flags = [flag.strip() for flag in str(value).split(",") if flag.strip()]
    return tuple(dict.fromkeys(flags))


def _safe_int(value: str) -> Optional[int]:
    """
    Parse optional integer text without validating sign or a field-specific range.

    Example:
        >>> _safe_int("2020"), _safe_int("")
        (2020, None)


    :param value: Prompt result stringified and stripped before parsing.
    :return: Integer or ``None`` for blank/invalid integer text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _epoch_ms(value: Optional[str]) -> Optional[int]:
    """
    Convert original-date text with the shared timestamp parser, suppressing conversion failures.

    Missing and invalid dates both become ``None`` rather than an input error.

    Example:
        >>> _epoch_ms("1970-01-01"), _epoch_ms("not-a-date")
        (0, None)


    :param value: Optional timestamp-like text accepted by ``to_epoch_ms``.
    :return: Integer epoch milliseconds or ``None`` for absence or any ordinary conversion failure.
    """
    if value is None:
        return None
    try:
        return int(to_epoch_ms(value))
    except Exception:
        return None


class NewExpressionWizardCommand(TerminalCommandAPI):
    """
    Prompt for an expression's optional label, dates, language, flags, and content measurements.

    No label/title is locally required. Only a supplied label triggers an exact
    duplicate search; preferred-expression defaults to true.

    Example:
        >>> NewExpressionWizardCommand().usage
        'add expression'
    """

    group = "add"
    name = "expression"
    aliases = (
        "new-expression",
        "new_expression",
        "add-expression",
        "add_expression",
    )
    summary = "Interactive wizard to add an expression."
    usage = "add expression"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Collect expression fields, confirm, normalize selected values, and submit Core creation.

        Integer fields are not range-checked. Original date becomes milliseconds
        or ``None`` on failure; copyright date remains text. Flags are stored as a
        comma-joined string. Language lookup follows confirmation and its ``None``
        result is forwarded without an explicit unknown-language error here.
        Post-write result/output failure does not undo creation.

        Example:
            >>> NewExpressionWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing schema/search/language reads, prompts, Core catalog creation, and output.
        :param args: Must be empty; expression data is collected interactively.
        :return: ``True`` after reporting the created expression.
        :raises ValueError: For arguments, missing expressions table, invalid numeric syntax, or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "expressions" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `expressions` table.")

        browser.emit("New expression wizard")
        browser.emit("---------------------")

        expression_subtitle = _clean_optional(
            browser.prompt_text("Expression subtitle", default="")
        )
        expression_title_override = _clean_optional(
            browser.prompt_text("Expression title override", default="")
        )
        expression_type = _clean_optional(
            browser.prompt_text("Expression type", default="")
        )
        expression_label = _clean_optional(
            browser.prompt_text("Expression label", default="")
        )

        year_text = browser.prompt_text("Expression year", default="")
        expression_year = _safe_int(year_text)
        if year_text.strip() and expression_year is None:
            raise ValueError("Expression year must be an integer.")

        expression_is_preferred = int(
            browser.prompt_yes_no("Preferred expression?", default=True)
        )
        expression_original_date = _clean_optional(
            browser.prompt_text("Expression original date", default="")
        )
        expression_original_copyright_date = _clean_optional(
            browser.prompt_text("Expression original copyright date", default="")
        )
        expression_flags = _clean_flags(
            browser.prompt_text("Expression flags", default="")
        )

        language_text = browser.prompt_text("Expression language", default="").strip()
        if language_text:
            expression_language: Optional[str | int]
            expression_language = (
                int(language_text) if language_text.isdigit() else language_text
            )
        else:
            expression_language = None

        expression_mode = _clean_optional(
            browser.prompt_text("Expression mode", default="")
        )

        wordcount_text = browser.prompt_text("Expression wordcount", default="")
        expression_wordcount = _safe_int(wordcount_text)
        if wordcount_text.strip() and expression_wordcount is None:
            raise ValueError("Expression wordcount must be an integer.")

        fiction_len_text = browser.prompt_text(
            "Expression fiction length category", default=""
        )
        expression_fiction_length_category = _safe_int(fiction_len_text)
        if fiction_len_text.strip() and expression_fiction_length_category is None:
            raise ValueError("Expression fiction length category must be an integer.")

        expression_cut_type = _clean_optional(
            browser.prompt_text("Expression cut type", default="")
        )

        duration_text = browser.prompt_text(
            "Expression nominal duration seconds", default=""
        )
        expression_nominal_duration_seconds = _safe_int(duration_text)
        if duration_text.strip() and expression_nominal_duration_seconds is None:
            raise ValueError("Expression nominal duration seconds must be an integer.")

        expression_status = _clean_optional(
            browser.prompt_text("Expression status", default="")
        )
        expression_origin_note = _clean_optional(
            browser.prompt_text("Expression origin note", default="")
        )

        if expression_label:
            existing = browser.db.search(
                "expressions", "expression_label", expression_label
            )
            if existing:
                browser.emit(
                    "Possible duplicate expression exists: expression_id={} label={!r}".format(
                        existing[0]["expression_id"],
                        existing[0]["expression_label"],
                    )
                )
                proceed_duplicate = browser.prompt_yes_no(
                    "Create another expression with this label?",
                    default=False,
                )
                if not proceed_duplicate:
                    raise ValueError(
                        "Expression wizard canceled to avoid duplicate entry."
                    )

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("label", expression_label or ""),
                        ("type", expression_type or ""),
                        (
                            "year",
                            expression_year if expression_year is not None else "",
                        ),
                        (
                            "language",
                            expression_language
                            if expression_language is not None
                            else "",
                        ),
                        ("mode", expression_mode or ""),
                        ("preferred", bool(expression_is_preferred)),
                    ],
                )
            ],
            title="Expression summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this expression now?", default=True)
        if not proceed:
            raise ValueError("Expression wizard canceled.")

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={
                "repository": "expressions",
                "data": {
                    "expression_subtitle": expression_subtitle,
                    "expression_title_override": expression_title_override,
                    "expression_type": expression_type,
                    "expression_label": expression_label,
                    "expression_year": expression_year,
                    "expression_is_preferred": expression_is_preferred,
                    "expression_original_date": _epoch_ms(expression_original_date),
                    "expression_original_copyright_date": expression_original_copyright_date,
                    "expression_flags": ",".join(expression_flags) or None,
                    "expression_language_id": (
                        browser.resolve_language_id(expression_language)
                        if expression_language is not None
                        else None
                    ),
                    "expression_mode": expression_mode,
                    "expression_wordcount": expression_wordcount,
                    "expression_fiction_length_category": expression_fiction_length_category,
                    "expression_cut_type": expression_cut_type,
                    "expression_nominal_duration_seconds": expression_nominal_duration_seconds,
                    "expression_status": expression_status,
                    "expression_origin_note": expression_origin_note,
                },
            },
        )
        expression_row = dict(result["entity"])

        browser.emit(
            "Expression created: expression_id={} label={!r}".format(
                expression_row["expression_id"],
                expression_row["expression_label"],
            )
        )
        return True
