"""
Collect one item's inventory, source, acquisition, and optional manifestation data for Core creation.

The adapter records supplied file/source descriptions without reading backing
files. Original-date conversion is permissive; acquisition dates and copyright
dates remain text and the minor-price prompt uses floating-point parsing.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.metadata.ebook_metadata_tools import to_epoch_ms
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _clean_optional(value: str) -> Optional[str]:
    """
    Strip optional item prompt text and replace blank results with ``None``.

    Example:
        >>> _clean_optional(" Shelf A "), _clean_optional(" ")
        ('Shelf A', None)


    :param value: Prompt result stringified before whitespace removal.
    :return: Nonblank stripped text or ``None`` without path/date validation.
    """
    text = str(value).strip()
    return text or None


def _safe_int(value: str) -> Optional[int]:
    """
    Parse optional manifestation-ID text without checking positivity.

    Example:
        >>> _safe_int("12"), _safe_int("")
        (12, None)


    :param value: Prompt result stringified and stripped before integer parsing.
    :return: Parsed integer or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _safe_float(value: str) -> Optional[float]:
    """
    Parse optional price text as a float without integer, sign, or finiteness constraints.

    Example:
        >>> _safe_float("12.5"), _safe_float("unknown")
        (12.5, None)


    :param value: Prompt text stringified and stripped before float conversion.
    :return: Parsed float or ``None`` for blank/invalid float text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _epoch_ms(value: Optional[str]) -> Optional[int]:
    """
    Convert original-date text to epoch milliseconds, treating conversion failures as absence.

    Example:
        >>> _epoch_ms("1970-01-01"), _epoch_ms("not-a-date")
        (0, None)


    :param value: Optional timestamp-like string interpreted by the shared date utility.
    :return: Integer epoch milliseconds or ``None`` for missing/failed conversion.
    """
    if value is None:
        return None
    try:
        return int(to_epoch_ms(value))
    except Exception:
        return None


class NewItemWizardCommand(TerminalCommandAPI):
    """
    Prompt for optional item identification, provenance, condition, and manifestation linkage.

    A supplied inventory code triggers duplicate confirmation. A supplied
    manifestation ID is checked for existence only when its table is advertised.

    Example:
        >>> NewItemWizardCommand().usage
        'add item'
    """

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
        """
        Validate prompt syntax and any available manifestation reference, confirm, and create an item through Core.

        An absent manifestations table does not prevent forwarding a supplied ID.
        Original-date failures become ``None``; other dates remain text. Price is
        accepted as a float despite the minor-unit label, without rounding or
        currency conversion. No inventory code is required locally, and a later
        result/output failure does not undo a completed write.

        Example:
            >>> NewItemWizardCommand().execute(browser, [])  # doctest: +SKIP


        :param browser: Host providing schema/reference/search reads, prompts, Core creation, and output.
        :param args: Must be empty; item fields are collected interactively.
        :return: ``True`` after reporting the created item.
        :raises ValueError: For arguments, absent items table, invalid numeric/reference input, or declined confirmation.
        """
        if args:
            raise ValueError("Usage: {}".format(self.usage))

        tables = set(browser.db.get_tables())
        missing = sorted({"items"} - tables)
        if missing:
            raise ValueError(
                "Database schema missing required tables: {}".format(", ".join(missing))
            )

        browser.emit("New item wizard")
        browser.emit("---------------")

        manifestation_id_text = browser.prompt_text(
            "Manifestation id (optional)", default=""
        )
        item_manifestation_id = _safe_int(manifestation_id_text)
        if manifestation_id_text.strip() and item_manifestation_id is None:
            raise ValueError("Manifestation id must be an integer.")
        if item_manifestation_id is not None and "manifestations" in tables:
            linked = browser.db.search(
                "manifestations", "manifestation_id", item_manifestation_id
            )
            if not linked:
                raise ValueError(
                    "No manifestation exists with manifestation_id={}.".format(
                        item_manifestation_id
                    )
                )

        item_flags = _clean_optional(browser.prompt_text("Item flags", default=""))
        item_type = _clean_optional(browser.prompt_text("Item type", default=""))
        item_location = _clean_optional(
            browser.prompt_text("Item location", default="")
        )
        item_inventory_code = _clean_optional(
            browser.prompt_text("Item inventory code", default="")
        )
        item_original_date = _clean_optional(
            browser.prompt_text("Item original date", default="")
        )
        item_original_copyright_date = _clean_optional(
            browser.prompt_text("Item original copyright date", default="")
        )
        item_source = _clean_optional(browser.prompt_text("Item source", default=""))
        item_source_detail = _clean_optional(
            browser.prompt_text("Item source detail", default="")
        )
        item_source_path = _clean_optional(
            browser.prompt_text("Item source path", default="")
        )
        item_source_name = _clean_optional(
            browser.prompt_text("Item source name", default="")
        )
        item_acquired_date = _clean_optional(
            browser.prompt_text("Item acquired date", default="")
        )

        acquired_price_text = browser.prompt_text(
            "Item acquired price minor", default=""
        )
        item_acquired_price_minor = _safe_float(acquired_price_text)
        if acquired_price_text.strip() and item_acquired_price_minor is None:
            raise ValueError("Item acquired price minor must be numeric.")

        item_lifecycle_status = _clean_optional(
            browser.prompt_text("Item lifecycle status", default="")
        )
        item_condition = _clean_optional(
            browser.prompt_text("Item condition", default="")
        )

        if item_inventory_code:
            existing = browser.db.search(
                "items", "item_inventory_code", item_inventory_code
            )
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
                        (
                            "manifestation_id",
                            item_manifestation_id
                            if item_manifestation_id is not None
                            else "",
                        ),
                        ("type", item_type or ""),
                        ("inventory_code", item_inventory_code or ""),
                        ("source", item_source or ""),
                        ("acquired_date", item_acquired_date or ""),
                        (
                            "acquired_price_minor",
                            item_acquired_price_minor
                            if item_acquired_price_minor is not None
                            else "",
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

        result = browser.execute_core_command(
            "catalog.entity.create",
            payload={
                "repository": "items",
                "data": {
                    "item_manifestation_id": item_manifestation_id,
                    "item_flags": item_flags,
                    "item_type": item_type,
                    "item_location": item_location,
                    "item_inventory_code": item_inventory_code,
                    "item_original_date": _epoch_ms(item_original_date),
                    "item_original_copyright_date": item_original_copyright_date,
                    "item_source": item_source,
                    "item_source_detail": item_source_detail,
                    "item_source_path": item_source_path,
                    "item_source_name": item_source_name,
                    "item_acquired_date": item_acquired_date,
                    "item_acquired_price_minor": item_acquired_price_minor,
                    "item_lifecycle_status": item_lifecycle_status,
                    "item_condition": item_condition,
                },
            },
        )
        item_row = dict(result["entity"])

        browser.emit(
            "Item created: item_id={} inventory_code={!r}".format(
                item_row["item_id"],
                item_row["item_inventory_code"],
            )
        )
        return True
