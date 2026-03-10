"""Interactive wizard command for adding expression entries."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.metadata_tools.add import Add
from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


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


class NewExpressionWizardCommand(TerminalCommandAPI):
    """Create an expression row through guided prompts."""

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
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "expressions" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `expressions` table.")

        browser.emit("New expression wizard")
        browser.emit("---------------------")

        expression_subtitle = _clean_optional(browser.prompt_text("Expression subtitle", default=""))
        expression_title_override = _clean_optional(browser.prompt_text("Expression title override", default=""))
        expression_type = _clean_optional(browser.prompt_text("Expression type", default=""))
        expression_label = _clean_optional(browser.prompt_text("Expression label", default=""))

        year_text = browser.prompt_text("Expression year", default="")
        expression_year = _safe_int(year_text)
        if year_text.strip() and expression_year is None:
            raise ValueError("Expression year must be an integer.")

        expression_is_preferred = int(browser.prompt_yes_no("Preferred expression?", default=True))
        expression_original_date = _clean_optional(browser.prompt_text("Expression original date", default=""))
        expression_original_copyright_date = _clean_optional(
            browser.prompt_text("Expression original copyright date", default="")
        )
        expression_flags = _clean_optional(browser.prompt_text("Expression flags", default=""))

        language_text = browser.prompt_text("Expression language", default="").strip()
        if language_text:
            expression_language: Optional[str | int]
            expression_language = int(language_text) if language_text.isdigit() else language_text
        else:
            expression_language = None

        expression_mode = _clean_optional(browser.prompt_text("Expression mode", default=""))

        wordcount_text = browser.prompt_text("Expression wordcount", default="")
        expression_wordcount = _safe_int(wordcount_text)
        if wordcount_text.strip() and expression_wordcount is None:
            raise ValueError("Expression wordcount must be an integer.")

        fiction_len_text = browser.prompt_text("Expression fiction length category", default="")
        expression_fiction_length_category = _safe_int(fiction_len_text)
        if fiction_len_text.strip() and expression_fiction_length_category is None:
            raise ValueError("Expression fiction length category must be an integer.")

        expression_cut_type = _clean_optional(browser.prompt_text("Expression cut type", default=""))

        duration_text = browser.prompt_text("Expression nominal duration seconds", default="")
        expression_nominal_duration_seconds = _safe_int(duration_text)
        if duration_text.strip() and expression_nominal_duration_seconds is None:
            raise ValueError("Expression nominal duration seconds must be an integer.")

        expression_status = _clean_optional(browser.prompt_text("Expression status", default=""))
        expression_origin_note = _clean_optional(browser.prompt_text("Expression origin note", default=""))

        if expression_label:
            existing = browser.db.search("expressions", "expression_label", expression_label)
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
                    raise ValueError("Expression wizard canceled to avoid duplicate entry.")

        browser.emit("Expression summary")
        browser.emit("  label: {}".format(expression_label or ""))
        browser.emit("  type: {}".format(expression_type or ""))
        browser.emit("  year: {}".format(expression_year if expression_year is not None else ""))
        browser.emit("  language: {}".format(expression_language if expression_language is not None else ""))
        browser.emit("  mode: {}".format(expression_mode or ""))
        browser.emit("  preferred: {}".format(bool(expression_is_preferred)))
        proceed = browser.prompt_yes_no("Create this expression now?", default=True)
        if not proceed:
            raise ValueError("Expression wizard canceled.")

        add = Add(browser.db)
        expression_row = add.expression(
            expression_subtitle=expression_subtitle,
            expression_title_override=expression_title_override,
            expression_type=expression_type,
            expression_label=expression_label,
            expression_year=expression_year,
            expression_is_preferred=expression_is_preferred,
            expression_original_date=expression_original_date,
            expression_original_copyright_date=expression_original_copyright_date,
            expression_flags=expression_flags,
            expression_language=expression_language,
            expression_mode=expression_mode,
            expression_wordcount=expression_wordcount,
            expression_fiction_length_category=expression_fiction_length_category,
            expression_cut_type=expression_cut_type,
            expression_nominal_duration_seconds=expression_nominal_duration_seconds,
            expression_status=expression_status,
            expression_origin_note=expression_origin_note,
        )

        browser.emit(
            "Expression created: expression_id={} label={!r}".format(
                expression_row["expression_id"],
                expression_row["expression_label"],
            )
        )
        return True

