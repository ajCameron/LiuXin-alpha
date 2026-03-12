"""Interactive wizard command for adding item entries."""

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


def _safe_float(value: str) -> Optional[float]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


class NewItemWizardCommand(TerminalCommandAPI):
    """Create an item row through guided prompts."""

    group = "add"
    name = "item"
    aliases = (
        "new-item",
        "new_item",
        "add-item",
        "add_item",
    )
    summary = "Interactive wizard to add an item."
    usage = "add item"

    def execute(self, browser, args: list[str]) -> bool:
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"items"} - tables)
        if missing:
            raise ValueError("Database schema missing required tables: {}".format(", ".join(missing)))

        browser.emit("New item wizard")
        browser.emit("---------------")

        manifestation_id_text = browser.prompt_text("Manifestation id (optional)", default="")
        item_manifestation_id = _safe_int(manifestation_id_text)
        if manifestation_id_text.strip() and item_manifestation_id is None:
            raise ValueError("Manifestation id must be an integer.")
        if item_manifestation_id is not None and "manifestations" in tables:
            linked = browser.db.search("manifestations", "manifestation_id", item_manifestation_id)
            if not linked:
                raise ValueError("No manifestation exists with manifestation_id={}.".format(item_manifestation_id))

        item_flags = _clean_optional(browser.prompt_text("Item flags", default=""))
        item_type = _clean_optional(browser.prompt_text("Item type", default=""))
        item_location = _clean_optional(browser.prompt_text("Item location", default=""))
        item_inventory_code = _clean_optional(browser.prompt_text("Item inventory code", default=""))
        item_original_date = _clean_optional(browser.prompt_text("Item original date", default=""))
        item_original_copyright_date = _clean_optional(browser.prompt_text("Item original copyright date", default=""))
        item_source = _clean_optional(browser.prompt_text("Item source", default=""))
        item_source_detail = _clean_optional(browser.prompt_text("Item source detail", default=""))
        item_source_path = _clean_optional(browser.prompt_text("Item source path", default=""))
        item_source_name = _clean_optional(browser.prompt_text("Item source name", default=""))
        item_acquired_date = _clean_optional(browser.prompt_text("Item acquired date", default=""))

        acquired_price_text = browser.prompt_text("Item acquired price minor", default="")
        item_acquired_price_minor = _safe_float(acquired_price_text)
        if acquired_price_text.strip() and item_acquired_price_minor is None:
            raise ValueError("Item acquired price minor must be numeric.")

        item_lifecycle_status = _clean_optional(browser.prompt_text("Item lifecycle status", default=""))
        item_condition = _clean_optional(browser.prompt_text("Item condition", default=""))

        if item_inventory_code:
            existing = browser.db.search("items", "item_inventory_code", item_inventory_code)
            if existing:
                browser.emit(
                    "Possible duplicate item exists: item_id={} inventory_code={!r}".format(
                        existing[0]["item_id"],
                        existing[0]["item_inventory_code"],
                    )
                )
                proceed_duplicate = browser.prompt_yes_no(
                    "Create another item with this inventory code?",
                    default=False,
                )
                if not proceed_duplicate:
                    raise ValueError("Item wizard canceled to avoid duplicate entry.")

        browser.emit_detail_sections(
            [
                (
                    "",
                    [
                        ("manifestation_id", item_manifestation_id if item_manifestation_id is not None else ""),
                        ("type", item_type or ""),
                        ("inventory_code", item_inventory_code or ""),
                        ("source", item_source or ""),
                        ("acquired_date", item_acquired_date or ""),
                        (
                            "acquired_price_minor",
                            item_acquired_price_minor if item_acquired_price_minor is not None else "",
                        ),
                    ],
                )
            ],
            title="Item summary",
            max_cell_width=120,
        )
        proceed = browser.prompt_yes_no("Create this item now?", default=True)
        if not proceed:
            raise ValueError("Item wizard canceled.")

        add = Add(browser.db)
        item_row = add.item(
            item_manifestation_id=item_manifestation_id,
            item_flags=item_flags,
            item_type=item_type,
            item_location=item_location,
            item_inventory_code=item_inventory_code,
            item_original_date=item_original_date,
            item_original_copyright_date=item_original_copyright_date,
            item_source=item_source,
            item_source_detail=item_source_detail,
            item_source_path=item_source_path,
            item_source_name=item_source_name,
            item_acquired_date=item_acquired_date,
            item_acquired_price_minor=item_acquired_price_minor,
            item_lifecycle_status=item_lifecycle_status,
            item_condition=item_condition,
        )

        browser.emit(
            "Item created: item_id={} inventory_code={!r}".format(
                item_row["item_id"],
                item_row["item_inventory_code"],
            )
        )
        return True
