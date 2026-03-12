"""Interactive wizard command for adding manifestation entries."""

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


class NewManifestationWizardCommand(TerminalCommandAPI):
    """Create a manifestation row through guided prompts."""

    group = "add"
    name = "manifestation"
    aliases = (
        "new-manifestation",
        "new_manifestation",
        "add-manifestation",
        "add_manifestation",
    )
    summary = "Interactive wizard to add a manifestation."
    usage = "add manifestation"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        if "manifestations" not in set(browser.db.get_tables()):
            raise ValueError("Database schema does not contain `manifestations` table.")

        browser.emit("New manifestation wizard")
        browser.emit("-----------------------")

        manifestation_subtitle = _clean_optional(browser.prompt_text("Manifestation subtitle", default=""))
        manifestation_carrier_type = _clean_optional(browser.prompt_text("Manifestation carrier type", default=""))
        manifestation_format_detail = _clean_optional(browser.prompt_text("Manifestation format detail", default=""))
        manifestation_edition_statement = _clean_optional(
            browser.prompt_text("Manifestation edition statement", default="")
        )

        pub_year_text = browser.prompt_text("Manifestation publication year", default="")
        manifestation_pub_year = _safe_int(pub_year_text)
        if pub_year_text.strip() and manifestation_pub_year is None:
            raise ValueError("Manifestation publication year must be an integer.")

        manifestation_pub_date = _clean_optional(browser.prompt_text("Manifestation publication date", default=""))
        manifestation_flags = _clean_optional(browser.prompt_text("Manifestation flags", default=""))

        page_count_text = browser.prompt_text("Manifestation page count", default="")
        manifestation_page_count = _safe_int(page_count_text)
        if page_count_text.strip() and manifestation_page_count is None:
            raise ValueError("Manifestation page count must be an integer.")

        runtime_minutes_text = browser.prompt_text("Manifestation runtime minutes", default="")
        manifestation_runtime_minutes = _safe_int(runtime_minutes_text)
        if runtime_minutes_text.strip() and manifestation_runtime_minutes is None:
            raise ValueError("Manifestation runtime minutes must be an integer.")

        manifestation_region_code = _clean_optional(browser.prompt_text("Manifestation region code", default=""))
        manifestation_status = _clean_optional(browser.prompt_text("Manifestation status", default=""))
        manifestation_note = _clean_optional(browser.prompt_text("Manifestation note", default=""))

        if manifestation_format_detail:
            existing = browser.db.search("manifestations", "manifestation_format_detail", manifestation_format_detail)
            if existing:
                browser.emit(
                    "Possible duplicate manifestation exists: manifestation_id={} format={!r}".format(
                        existing[0]["manifestation_id"],
                        existing[0]["manifestation_format_detail"],
                    )
                )
                proceed_duplicate = browser.prompt_yes_no(
                    "Create another manifestation with this format detail?",
                    default=False,
                )
                if not proceed_duplicate:
                    raise ValueError("Manifestation wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("subtitle", manifestation_subtitle or ""),
                        ("carrier_type", manifestation_carrier_type or ""),
                        ("format_detail", manifestation_format_detail or ""),
                        ("publication_year", manifestation_pub_year if manifestation_pub_year is not None else ""),
                        ("publication_date", manifestation_pub_date or ""),
                        ("page_count", manifestation_page_count if manifestation_page_count is not None else ""),
                        (
                            "runtime_minutes",
                            manifestation_runtime_minutes if manifestation_runtime_minutes is not None else "",
                        ),
                    ],
                )
            ],
            title="Manifestation summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this manifestation now?", default=True)
        if not proceed:
            raise ValueError("Manifestation wizard canceled.")

        add = Add(browser.db)
        manifestation_row = add.manifestation(
            manifestation_subtitle=manifestation_subtitle,
            manifestation_carrier_type=manifestation_carrier_type,
            manifestation_format_detail=manifestation_format_detail,
            manifestation_edition_statement=manifestation_edition_statement,
            manifestation_pub_year=manifestation_pub_year,
            manifestation_pub_date=manifestation_pub_date,
            manifestation_flags=manifestation_flags,
            manifestation_page_count=manifestation_page_count,
            manifestation_runtime_minutes=manifestation_runtime_minutes,
            manifestation_region_code=manifestation_region_code,
            manifestation_status=manifestation_status,
            manifestation_note=manifestation_note,
        )

        browser.emit(
            "Manifestation created: manifestation_id={} format={!r}".format(
                manifestation_row["manifestation_id"],
                manifestation_row["manifestation_format_detail"],
            )
        )
        return True
