"""Shared terminal rendering and stream prompts, independent of UI startup."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import TextIO


def truncate(value: object, *, width: int = 80) -> str:
    """Abbreviate a value's repr for row summaries, retaining short values intact."""
    text = repr(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def truncate_text(value: object, *, width: int = 80) -> str:
    """Abbreviate display text without repr quoting or escaping."""
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def summarize_exception(exc: BaseException) -> str:
    """Include the exception class even when its message is empty."""
    text = str(exc).strip()
    name = exc.__class__.__name__
    if text:
        return f"{name}: {text}"
    return name


def stringify_table_cell(value: object, *, width: int = 60) -> str:
    """Escape embedded line breaks and bound one ASCII-table cell."""
    if value is None:
        text = ""
    else:
        text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def preview_row_text(value: object, *, max_len: int = 64) -> str:
    """Flatten row-preview whitespace and cap the displayed length."""
    text = (
        str(value or "")
        .replace("\r\n", " ")
        .replace("\r", " ")
        .replace("\n", " ")
        .strip()
    )
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def row_detail_group(column: str, *, id_column: str | None) -> str:
    """Classify columns for the existing identity/reference/detail sections."""
    text = str(column).strip().lower()
    if not text:
        return "other"
    if id_column is not None and text == str(id_column).strip().lower():
        return "identity"
    if text.endswith("_id"):
        return "references"
    if (
        text.startswith("supports_")
        or text.startswith("is_")
        or "read_only" in text
        or "eventually_consistent" in text
    ):
        return "capabilities"
    if any(
        token in text
        for token in ("timestamp", "datestamp", "date", "year", "seen", "healthcheck")
    ):
        return "dates"
    if any(
        token in text
        for token in (
            "uri",
            "path",
            "root",
            "protocol",
            "auth",
            "credential",
            "mask",
            "mount",
            "latency",
            "online",
            "location",
            "policy",
        )
    ):
        return "access"
    if any(
        token in text
        for token in (
            "name",
            "title",
            "canonical",
            "sort",
            "kind",
            "type",
            "medium",
            "status",
            "note",
            "flags",
            "scratch",
        )
    ):
        return "identity"
    return "other"


def pretty_row_detail_group(group: str) -> str:
    """Render a section label, with a readable fallback for unknown groups."""
    mapping = {
        "identity": "Identity",
        "references": "References",
        "access": "Access",
        "capabilities": "Capabilities",
        "dates": "Dates",
        "other": "Other",
    }
    return mapping.get(
        str(group).strip().lower(), str(group).strip().title() or "Other"
    )


def _strip_column_prefixes(original: str, ordered_prefixes: Sequence[str]) -> str:
    """Repeatedly remove known prefixes, retaining a nonempty label."""
    text = original
    changed = True
    while changed:
        changed = False
        for prefix in ordered_prefixes:
            if text.startswith(prefix) and len(text) > len(prefix):
                text = text[len(prefix) :]
                changed = True
                break
    if not text:
        text = original
    return text


def shorten_column_headers(
    headers: Sequence[str], *, table_name: str | None = None
) -> list[str]:
    """Remove known table prefixes while keeping duplicate labels unambiguous."""
    originals = [str(h) for h in headers]
    table_name = (table_name or "").strip().lower()

    known_prefixes = {
        "agent_",
        "human_agent_",
        "org_agent_",
        "store_",
        "file_",
        "folder_",
        "note_",
        "synopsis_",
        "identifier_",
        "series_",
        "tag_",
        "genre_",
        "subject_",
        "image_",
        "language_",
        "book_",
        "title_",
        "work_",
        "expression_",
        "manifestation_",
        "item_",
    }
    if table_name:
        known_prefixes.add(table_name + "_")
        tokens = [token for token in table_name.split("_") if token]
        if tokens:
            tail = tokens[-1]
            if tail.endswith("s") and len(tail) > 1:
                singular_tokens = list(tokens)
                singular_tokens[-1] = tail[:-1]
                known_prefixes.add("_".join(singular_tokens) + "_")

    ordered_prefixes = sorted(known_prefixes, key=len, reverse=True)
    shortened = [
        _strip_column_prefixes(original, ordered_prefixes) for original in originals
    ]

    # Keep headers unambiguous after shortening.
    counts: dict[str, int] = {}
    deduped: list[str] = []
    for _idx, text in enumerate(shortened):
        seen = counts.get(text, 0)
        counts[text] = seen + 1
        if seen == 0:
            deduped.append(text)
        else:
            deduped.append(f"{text}#{seen + 1}")

    return deduped


def _table_width(widths: Sequence[int]) -> int:
    """Account for each column plus the ASCII borders."""
    return 1 + sum(width + 3 for width in widths)


def _fit_table_width(
    normalized_headers: list[str],
    normalized_rows: list[list[str]],
    widths: list[int],
    max_table_width: int,
) -> bool:
    """Drop rightmost columns and shrink the remainder to the available width."""
    # Width model for this renderer:
    # total = 1 + sum(column_width + 3)
    preferred_min_widths = [min(max(6, len(h)), 14) for h in normalized_headers]

    # If preferred-readable columns won't fit, drop columns from the right.
    omitted_columns = 0
    while widths and _table_width(preferred_min_widths) > max_table_width:
        widths.pop()
        preferred_min_widths.pop()
        normalized_headers.pop()
        for row in normalized_rows:
            row.pop()
        omitted_columns += 1
    if omitted_columns:
        if normalized_headers:
            normalized_headers[-1] = (
                normalized_headers[-1] + f" (+{omitted_columns} cols)"
            )
            widths[-1] = max(widths[-1], len(normalized_headers[-1]))
        else:
            return False

    min_widths = [3 for _ in normalized_headers]

    # Shrink widest columns until table fits.
    while widths and _table_width(widths) > max_table_width:
        widest_idx = max(range(len(widths)), key=lambda idx: widths[idx])
        if widths[widest_idx] <= min_widths[widest_idx]:
            break
        widths[widest_idx] -= 1

    _truncate_fitted_cells(normalized_headers, normalized_rows, widths)
    return True


def _truncate_fitted_cells(
    normalized_headers: list[str], normalized_rows: list[list[str]], widths: list[int]
) -> None:
    """Apply final widths only after column allocation has settled."""
    # Re-truncate headers and cells to final widths.
    for idx, header in enumerate(normalized_headers):
        if len(header) > widths[idx]:
            normalized_headers[idx] = stringify_table_cell(header, width=widths[idx])
    for row in normalized_rows:
        for idx, cell in enumerate(row):
            if len(cell) > widths[idx]:
                row[idx] = stringify_table_cell(cell, width=widths[idx])


def render_ascii_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    *,
    max_cell_width: int = 60,
    max_table_width: int | None = None,
) -> str:
    """Render bounded cells and shrink columns to the requested terminal width."""
    if not headers:
        return "(no columns)"

    normalized_headers = [str(h) for h in headers]
    normalized_rows: list[list[str]] = []
    for row in rows:
        cells = [stringify_table_cell(value, width=max_cell_width) for value in row]
        if len(cells) < len(normalized_headers):
            cells.extend([""] * (len(normalized_headers) - len(cells)))
        elif len(cells) > len(normalized_headers):
            cells = cells[: len(normalized_headers)]
        normalized_rows.append(cells)

    widths = [len(h) for h in normalized_headers]
    for row in normalized_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    if max_table_width is not None and max_table_width > 0:
        if not _fit_table_width(
            normalized_headers, normalized_rows, widths, max_table_width
        ):
            return f"(table too wide to render in {max_table_width} columns)"

    divider = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    lines = [divider]
    lines.append(
        "| "
        + " | ".join(
            normalized_headers[idx].ljust(widths[idx]) for idx in range(len(widths))
        )
        + " |"
    )
    lines.append(divider)
    for row in normalized_rows:
        lines.append(
            "| "
            + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(widths)))
            + " |"
        )
    lines.append(divider)
    return "\n".join(lines)


def safe_int(value: str) -> int | None:
    """Return an integer selector, or None when conversion fails."""
    try:
        return int(value)
    except Exception:
        return None


def looks_like_id_selector(token: str) -> bool:
    """Whether token looks like an id selector: `1`, `1,2`, `10-20`, or mixed."""
    text = str(token).strip()
    if not text:
        return False

    parts = text.split(",")
    if not parts:
        return False

    for raw_part in parts:
        part = raw_part.strip()
        if not part:
            return False
        if "-" in part:
            left, right = part.split("-", 1)
            if safe_int(left.strip()) is None or safe_int(right.strip()) is None:
                return False
        else:
            if safe_int(part) is None:
                return False
    return True


def ask_text(
    prompt: str,
    *,
    default: str | None = None,
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> str:
    """Read one stripped response, using the default for a blank response or EOF."""
    suffix = ""
    if default is not None:
        suffix = f" [{default}]"
    output_stream.write(f"{prompt}{suffix}: ")
    output_stream.flush()
    raw = input_stream.readline()
    if raw == "":
        return default or ""
    value = raw.strip()
    if value == "" and default is not None:
        return default
    return value


def ask_yes_no(
    prompt: str,
    *,
    default: bool,
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
) -> bool:
    """Accept common boolean responses and report invalid responses before defaulting."""
    hint = "Y/n" if default else "y/N"
    raw = (
        ask_text(
            f"{prompt} ({hint})",
            default=None,
            input_stream=input_stream,
            output_stream=output_stream,
        )
        .strip()
        .lower()
    )
    if raw == "":
        return default
    if raw in {"y", "yes", "1", "true", "t"}:
        return True
    if raw in {"n", "no", "0", "false", "f"}:
        return False
    output_stream.write(
        "Invalid response {!r}; using default {}\n".format(
            raw, "yes" if default else "no"
        )
    )
    output_stream.flush()
    return default


__all__ = [
    "truncate",
    "truncate_text",
    "summarize_exception",
    "stringify_table_cell",
    "preview_row_text",
    "row_detail_group",
    "pretty_row_detail_group",
    "shorten_column_headers",
    "render_ascii_table",
    "safe_int",
    "looks_like_id_selector",
    "ask_text",
    "ask_yes_no",
]
