"""Interactive wizard command for adding subject rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.standardization import make_title_search_term


def _safe_int(value: str) -> Optional[int]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


class NewSubjectWizardCommand(TerminalCommandAPI):
    """Create a subject row through guided prompts."""

    group = "add"
    name = "subject"
    aliases = (
        "new-subject",
        "new_subject",
        "add-subject",
        "add_subject",
    )
    summary = "Interactive wizard to add a subject."
    usage = "add subject"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        if "subjects" not in tables:
            raise ValueError("Database schema does not contain `subjects` table.")
        columns = set(browser.db.get_column_headings("subjects"))

        browser.emit("New subject wizard")
        browser.emit("-----------------")

        subject_text = browser.prompt_text("Subject", default="").strip()
        if not subject_text:
            raise ValueError("Subject cannot be blank.")

        default_sort = make_title_search_term(subject_text)
        subject_sort = browser.prompt_text("Subject sort", default=default_sort).strip() or default_sort

        parent_id_text = browser.prompt_text("Parent subject id (optional)", default="")
        parent_id = _safe_int(parent_id_text)
        if parent_id_text.strip() and parent_id is None:
            raise ValueError("Parent subject id must be an integer.")
        parent_row = None
        if parent_id is not None:
            parent_row = browser.db.get_row_from_id("subjects", parent_id)
            if parent_row is None:
                raise ValueError("No subject exists with subject_id={}.".format(parent_id))

        existing = browser.db.search("subjects", "subject_sort", subject_sort)
        if existing:
            browser.emit(
                "Possible duplicate subject exists: subject_id={} subject={!r}".format(
                    existing[0]["subject_id"],
                    existing[0]["subject"],
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another subject with this sort value?", default=False)
            if not proceed_duplicate:
                raise ValueError("Subject wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("subject", subject_text),
                        ("sort", subject_sort),
                        ("parent_id", parent_id if parent_id is not None else ""),
                    ],
                )
            ],
            title="Subject summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this subject now?", default=True)
        if not proceed:
            raise ValueError("Subject wizard canceled.")

        row_dict = {"subject": subject_text}
        if "subject_sort" in columns:
            row_dict["subject_sort"] = subject_sort
        if parent_row is not None:
            if "subject_parent_id" in columns:
                row_dict["subject_parent_id"] = parent_row.row_id
            elif "subject_parent" in columns:
                row_dict["subject_parent"] = parent_row.row_id

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={"repository": "subjects", "data": row_dict},
        )
        subject_row = dict(result["entity"])

        browser.emit(
            "Subject created: subject_id={} subject={!r}".format(
                subject_row["subject_id"],
                subject_row["subject"],
            )
        )
        return True
