"""Metadata edit parsing and report formatting for the Tk GUI."""

from __future__ import annotations

import re

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.surfaces.metadata_write_bridge import normalize_metadata_write_field


METADATA_EDIT_FIELDS = ("tags", "labels", "genre", "series", "identifiers")


def parse_metadata_edit_payload(field: str, text: str) -> tuple[str, dict[str, Any]]:
    """Return a normalized write field and core metadata.write values payload."""
    field_name = normalize_metadata_write_field(field)
    if field_name not in METADATA_EDIT_FIELDS:
        raise ValueError("Metadata field {!r} is not editable in the Tk GUI.".format(field))
    if field_name == "identifiers":
        return field_name, {"identifiers": _parse_identifier_values(text)}
    values = _split_metadata_values(text)
    if not values:
        raise ValueError("Provide at least one value for {}.".format(field_name))
    return field_name, {field_name: values}


def format_metadata_write_result(result: Mapping[str, Any] | None) -> str:
    """Format a metadata write response for the metadata panel."""
    data = dict(result or {})
    report = data.get("report") if isinstance(data.get("report"), Mapping) else {}
    changed = "yes" if bool(data.get("changed")) else "no"
    fields = ", ".join(str(field) for field in data.get("fields", ()) or ()) or "-"
    lines = [
        "Metadata write report",
        "item_id: {}".format(data.get("item_id", "-")),
        "kind: {}".format(data.get("kind", "-")),
        "fields: {}".format(fields),
        "changed: {}".format(changed),
    ]
    refreshed = data.get("read_source_refreshed")
    if refreshed is not None:
        lines.append("read source refreshed: {}".format("yes" if bool(refreshed) else "no"))
    summary = data.get("summary")
    if summary:
        lines.append(str(summary))
    for label, key in (
        ("rows added", "rows_added"),
        ("rows updated", "rows_updated"),
        ("rows removed", "rows_removed"),
        ("links added", "links_added"),
        ("links removed", "links_removed"),
        ("skipped", "skipped"),
        ("errors", "errors"),
    ):
        lines.append("{}: {}".format(label, len(report.get(key, ()) or ())))
    lines.extend(_detail_lines("skipped", report.get("skipped", ())))
    lines.extend(_detail_lines("errors", report.get("errors", ())))
    return "\n".join(lines)


def _split_metadata_values(text: str) -> list[str]:
    values: list[str] = []
    for value in re.split(r"[;\n]+", str(text or "")):
        value = value.strip()
        if value and value not in values:
            values.append(value)
    return values


def _parse_identifier_values(text: str) -> dict[str, list[str]]:
    identifiers: dict[str, list[str]] = {}
    for entry in _split_metadata_values(text):
        if "=" in entry:
            scheme, value = entry.split("=", 1)
        elif ":" in entry:
            scheme, value = entry.split(":", 1)
        else:
            raise ValueError("Identifier values must use scheme=value or scheme:value.")
        scheme = scheme.strip()
        value = value.strip()
        if not scheme or not value:
            raise ValueError("Identifier values must include both scheme and value.")
        values = identifiers.setdefault(scheme, [])
        if value not in values:
            values.append(value)
    if not identifiers:
        raise ValueError("Provide at least one identifier value.")
    return identifiers


def _detail_lines(label: str, values: Any) -> list[str]:
    if not values:
        return []
    return ["{} detail: {}".format(label, value) for value in values]


__all__ = [
    "METADATA_EDIT_FIELDS",
    "format_metadata_write_result",
    "parse_metadata_edit_payload",
]
