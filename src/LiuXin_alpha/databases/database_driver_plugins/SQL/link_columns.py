"""One optional-column catalog for link configuration and both SQLite emitters."""

import re
from collections.abc import Iterable
from typing import Literal

LINK_COLUMN_DEFINITIONS: tuple[tuple[str, str, str], ...] = (
    ("priority", "INTEGER", "DEFAULT 0"),
    ("primary", "INTEGER", "NULL DEFAULT 0"),
    ("type", "TEXT", "NULL"),
    ("origin", "TEXT", "NULL"),
    ("source", "TEXT", "NULL"),
    ("policy", "TEXT", "NULL"),
    ("data", "TEXT", "NULL"),
    ("index", "TEXT", "NULL"),
    ("sequence_number", "INTEGER", "NULL"),
    ("is_required", "INTEGER", "DEFAULT 1"),
)


def requested_link_columns(
    requested: Literal["all"] | Iterable[str],
    *,
    reserved_names: Iterable[str] = (),
) -> tuple[tuple[str, str, str], ...]:
    """Resolve a selection once, always including provenance and safe custom fields."""
    names = {name for name, _, _ in LINK_COLUMN_DEFINITIONS}
    selected = names if requested == "all" else set(requested) | {"source"}
    extras = selected - names - {"nullable"}
    reserved = set(reserved_names) | {
        "id",
        "scratch",
        "datestamp",
        "primary_id",
        "secondary_id",
    }
    for name in sorted(extras):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in reserved:
            raise TypeError(f"Unsafe or reserved requested column name: {name!r}")
    return tuple(
        item for item in LINK_COLUMN_DEFINITIONS if item[0] in selected
    ) + tuple((name, "TEXT", "NULL") for name in sorted(extras))
