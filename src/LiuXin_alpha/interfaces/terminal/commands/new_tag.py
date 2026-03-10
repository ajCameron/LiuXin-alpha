"""Interactive wizard command for adding tag/label rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.metadata_tools.add import Add
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.metadata.standardization import make_tag_search_term


def _clean_optional(value: str) -> Optional[str]:
    text = str(value).strip()
    return text or None


class NewTagWizardCommand(TerminalCommandAPI):
    """Create a tag-like row (`labels` on FRBR schema, `tags` on legacy schemas)."""

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

        tables = set(browser.db.get_tables())
        has_labels = "labels" in tables
        has_tags = "tags" in tables
        if not (has_labels or has_tags):
            raise ValueError("Database schema has neither `labels` nor `tags` table.")

        browser.emit("New tag wizard")
        browser.emit("--------------")

        tag_text = browser.prompt_text("Tag text", default="").strip()
        if not tag_text:
            raise ValueError("Tag text cannot be blank.")

        tag_norm = make_tag_search_term(tag_text)
        description = _clean_optional(browser.prompt_text("Tag description", default=""))

        if has_labels:
            existing = browser.db.search("labels", "label_text_norm", tag_norm)
        else:
            existing = browser.db.search("tags", "tag_phash", tag_norm)

        if existing:
            if has_labels:
                existing_id = existing[0]["label_id"]
                existing_value = existing[0]["label_text"]
            else:
                existing_id = existing[0]["tag_id"]
                existing_value = existing[0]["tag"]
            browser.emit(
                "Possible duplicate tag exists: id={} value={!r}".format(
                    existing_id,
                    existing_value,
                )
            )
            proceed_duplicate = browser.prompt_yes_no("Create another tag with this normalized form?", default=False)
            if not proceed_duplicate:
                raise ValueError("Tag wizard canceled to avoid duplicate entry.")

        browser.emit("Tag summary")
        browser.emit("  text: {}".format(tag_text))
        browser.emit("  normalized: {}".format(tag_norm))
        if has_labels:
            browser.emit("  description: {}".format(description or ""))
        proceed = browser.prompt_yes_no("Create this tag now?", default=True)
        if not proceed:
            raise ValueError("Tag wizard canceled.")

        if has_labels:
            row_dict = {
                "label_text": tag_text,
                "label_text_norm": tag_norm,
            }
            if description is not None:
                row_dict["label_description"] = description
            tag_row = Row.from_idless_row_dict(
                browser.db,
                row_dict=row_dict,
                table="labels",
            )
            browser.emit(
                "Tag created: label_id={} label_text={!r}".format(
                    tag_row["label_id"],
                    tag_row["label_text"],
                )
            )
            return True

        add = Add(browser.db)
        tag_row = add.tag(tag=tag_text, tag_phash=tag_norm)
        browser.emit(
            "Tag created: tag_id={} tag={!r}".format(
                tag_row["tag_id"],
                tag_row["tag"],
            )
        )
        return True

