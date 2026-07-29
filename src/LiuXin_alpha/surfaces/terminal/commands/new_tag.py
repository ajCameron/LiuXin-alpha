"""Interactive wizard command for adding tag/label rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.metadata_facets import (
    preferred_tag_table,
    search_tag_rows,
    tag_row_identity_column,
    tag_row_text,
    tag_search_value,
)
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    text = str(value).strip()
    return text or None


class NewTagWizardCommand(TerminalCommandAPI):
    """Create a tag row, falling back to legacy label rows when needed."""

    group = "add"
    name = "tag"
    aliases = (
        "new-tag",
        "new_tag",
        "add-tag",
        "add_tag",
        "new-label",
        "new_label",
        "add-label",
        "add_label",
    )
    summary = "Interactive wizard to add a tag/label."
    usage = "add tag"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tag_table = preferred_tag_table(browser.db)
        if tag_table is None:
            raise ValueError("Database schema has neither `labels` nor `tags` table.")

        browser.emit("New tag wizard")
        browser.emit("--------------")

        tag_text = browser.prompt_text("Tag text", default="").strip()
        if not tag_text:
            raise ValueError("Tag text cannot be blank.")

        tag_norm = tag_search_value(tag_text)
        description = _clean_optional(browser.prompt_text("Tag description", default=""))

        existing = search_tag_rows(browser.db, tag_table, tag_text)

        if existing:
            existing_id = existing[0][tag_row_identity_column(tag_table)]
            existing_value = tag_row_text(existing[0])
            browser.emit(
                "Possible duplicate tag exists: id={} value={!r}".format(
                    existing_id,
                    existing_value,
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another tag with this normalized form?", default=False)
            if not proceed_duplicate:
                raise ValueError("Tag wizard canceled to avoid duplicate entry.")

        summary_rows: list[tuple[str, object]] = [
            ("text", tag_text),
            ("normalized", tag_norm),
        ]
        summary_rows.append(("description", description or ""))
        browser.emit_detail_sections(
            [("", summary_rows)],
            title="Tag summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this tag now?", default=True)
        if not proceed:
            raise ValueError("Tag wizard canceled.")

        if tag_table == "tags":
            tag_data = {"text": tag_text, "phash": tag_norm}
            if description is not None and "tag_description" in set(browser.db.get_column_headings("tags")):
                tag_data["description"] = description
            result = browser.execute_core_command(
                "catalog.entity.create",
                payload={"repository": "tags", "data": tag_data},
            )
            tag_row = dict(result["entity"])
            browser.emit(
                "Tag created: tag_id={} tag={!r}".format(
                    tag_row["tag_id"],
                    tag_row["tag"],
                )
            )
            return True

        if tag_table == "labels":
            columns = set(browser.db.get_column_headings("labels"))
            label_data = {"text": tag_text, "normalized": tag_norm}
            if description is not None and "label_description" in columns:
                label_data["description"] = description
            result = browser.execute_core_command(
                "catalog.entity.create",
                payload={"repository": "labels", "data": label_data},
            )
            tag_row = dict(result["entity"])
            browser.emit(
                "Tag created: label_id={} label_text={!r}".format(
                    tag_row["label_id"],
                    tag_row_text(tag_row),
                )
            )
            return True

        raise ValueError("Unsupported tag table: {!r}".format(tag_table))
