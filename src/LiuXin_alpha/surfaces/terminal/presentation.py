"""
Format terminal values, table layouts, row-detail labels, and simple stream prompts.

Rendering uses Python character counts, not terminal display-cell measurements.
Helpers neither initialize a UI nor sanitize arbitrary control sequences; their
documented escaping and truncation rules apply only to the named transformations.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import TextIO


def truncate(value: object, *, width: int = 80) -> str:
    """
    Abbreviate a value's Python representation, including repr quoting and escaping.

    Truncation reserves three characters for an ellipsis. A width below three can
    still produce three dots; representation and invalid-width errors propagate.

    Example:
        >>> truncate("abcdefgh", width=6)
        "'ab..."


    :param value: Object whose ``repr`` is used for the summary.
    :param width: Desired maximum character count, including the truncation marker.
    :return: Full representation when it fits, otherwise a shortened prefix plus ``...``.
    """
    text = repr(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def truncate_text(value: object, *, width: int = 80) -> str:
    """
    Abbreviate stringified display text without adding representation quotes or escaping.

    Width includes the three-dot marker, which can exceed a requested width below three.

    Example:
        >>> truncate_text("abcdefgh", width=6)
        'abc...'


    :param value: Object to convert with ``str`` for display.
    :param width: Desired maximum Python characters, including any ellipsis.
    :return: Full string or its shortened prefix followed by ``...``.
    """
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def summarize_exception(exc: BaseException) -> str:
    """
    Combine an exception class name with its nonblank, outer-whitespace-stripped message.

    Internal line breaks and other message content are retained; no traceback is included.

    Example:
        >>> summarize_exception(ValueError(" invalid id "))
        'ValueError: invalid id'
        >>> summarize_exception(ValueError())
        'ValueError'


    :param exc: Exception whose class and string message should be displayed.
    :return: Class name alone for an empty message, otherwise ``ClassName: message``.
    """
    text = str(exc).strip()
    name = exc.__class__.__name__
    if text:
        return f"{name}: {text}"
    return name


def stringify_table_cell(value: object, *, width: int = 60) -> str:
    """
    Stringify a table cell, render line breaks as backslash-n pairs, and abbreviate long text.

    ``None`` becomes empty. CRLF and lone CR normalize to newline before escaping;
    tabs and other control characters are not changed. Width includes the ellipsis,
    so a truncation marker can exceed a requested width below three.

    Example:
        >>> stringify_table_cell(None)
        ''
        >>> stringify_table_cell("abcdefgh", width=6)
        'abc...'


    :param value: Cell value to stringify, with ``None`` treated as absent text.
    :param width: Desired maximum character count after line-break escaping.
    :return: Single-line cell text, shortened with ``...`` when it exceeds the width.
    """
    if value is None:
        text = ""
    else:
        text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def preview_row_text(value: object, *, max_len: int = 64) -> str:
    """
    Convert truthy values to stripped row-preview text, replacing line breaks with spaces.

    Falsey values, including zero and ``False``, become empty. Other internal
    whitespace is retained. Long previews use a three-dot truncation marker,
    which can exceed a requested length below three.

    Example:
        >>> preview_row_text(" first " + chr(10) + "second ")
        'first  second'
        >>> preview_row_text(0)
        ''


    :param value: Row-field value to preview without repr quoting.
    :param max_len: Desired maximum Python characters, including any ellipsis.
    :return: Flattened, stripped, and optionally abbreviated preview text.
    """
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
    """
    Choose a row-detail group using ordered, case-insensitive column-name heuristics.

    An exact supplied ID column wins, followed by other ID suffixes, capability
    prefixes/keywords, date keywords, access keywords, and identity keywords.
    Keyword checks are substring matches, not schema/semantic validation.

    Example:
        >>> row_detail_group("work_id", id_column="work_id")
        'identity'
        >>> row_detail_group("store_id", id_column="file_id")
        'references'


    :param column: Column name to strip and lowercase before classification.
    :param id_column: Optional primary-ID name whose exact normalized match takes precedence.
    :return: ``identity``, ``references``, ``capabilities``, ``dates``, ``access``, or ``other``.
    """
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
    """
    Render known detail-group names or title-case an unknown nonblank label.

    Example:
        >>> pretty_row_detail_group(" REFERENCES ")
        'References'
        >>> pretty_row_detail_group("extra fields")
        'Extra Fields'


    :param group: Group token or custom label to stringify and strip.
    :return: Standard display label, title-cased fallback, or ``Other`` for blank input.
    """
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
    """
    Remove matching prefixes repeatedly in priority order, never consuming the whole label.

    Matching is case-sensitive. The caller must supply nonempty prefixes; an empty
    prefix would repeatedly match without making progress.

    Example:
        >>> _strip_column_prefixes("work_title_text", ("work_", "title_"))
        'text'


    :param original: Original label used as the initial text and empty-result fallback.
    :param ordered_prefixes: Nonempty prefixes to try in their supplied precedence order.
    :return: Remaining nonempty label, or the original value when no removal applies.
    """
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
    """
    Strip known/table-derived prefixes and suffix repeated shortened labels with occurrence numbers.

    Prefix removal is case-sensitive; the optional table name is normalized to
    lowercase and also supplies a simple trailing-s singular form. Repeated labels
    become ``label#2``, ``label#3``, and so on. Existing literal suffixed labels are
    not reserved, so the result is not guaranteed globally unique.

    Example:
        >>> shorten_column_headers(["work_title", "title", "title#2"], table_name="works")
        ['title', 'title#2', 'title#2']


    :param headers: Ordered column names to stringify without otherwise changing their case.
    :param table_name: Optional table name supplying additional plural/singular prefixes.
    :return: New display-label list in input order, with numbered repeated shortened labels.
    """
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
    """
    Compute the renderer's character-width model, including separators and outer borders.

    Example:
        >>> _table_width([2, 4])
        13


    :param widths: Content widths for columns, excluding padding and border characters.
    :return: One outer border plus each content width and three padding/separator characters.
    """
    return 1 + sum(width + 3 for width in widths)


def _fit_table_width(
    normalized_headers: list[str],
    normalized_rows: list[list[str]],
    widths: list[int],
    max_table_width: int,
) -> bool:
    """
    Mutate aligned table data to fit a width by dropping rightmost columns, then shrinking the widest.

    Header-derived preferred minima decide which columns survive. Add an omission
    note to the final retained header before shrinking; that note may itself be
    abbreviated. Shrink retained widths toward a three-character floor, then
    truncate cells. Input lists must have matching column counts.

    Example:
        >>> headers, rows, widths = ["id", "title"], [["1", "longtitle"]], [2, 9]
        >>> _fit_table_width(headers, rows, widths, 10)
        True
        >>> (len(headers), _table_width(widths))
        (1, 10)


    :param normalized_headers: Mutable string headers from which dropped columns are removed.
    :param normalized_rows: Mutable rows aligned with the header/width lists.
    :param widths: Mutable content widths, shortened and reduced during fitting.
    :param max_table_width: Available character width including padding and borders.
    :return: ``False`` if column dropping removes all columns; otherwise ``True`` after fitting.
    """
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
    """
    Replace overlong headers and cells in place using the final aligned column widths.

    Already-fitting strings are untouched. Widths below three retain the cell
    stringifier's three-dot behavior rather than guaranteeing a strict smaller bound.

    Example:
        >>> headers, rows = ["heading"], [["alphabet"]]
        >>> _truncate_fitted_cells(headers, rows, [4])
        >>> (headers, rows)
        (['h...'], [['a...']])


    :param normalized_headers: Mutable header strings aligned with ``widths``.
    :param normalized_rows: Mutable string rows whose cells have corresponding width entries.
    :param widths: Final per-column character budgets used only for overlong strings.
    :return: ``None``; affected list entries are replaced in place.
    """
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
    """
    Render left-aligned ASCII rows with escaped/abbreviated cells and optional total-width fitting.

    Short rows are padded with blanks and long rows lose extra cells. Initial
    header text is stringified, not escaped or capped by the cell limit. A positive
    total-width budget may drop rightmost columns and truncate remaining headers/
    cells; omitted or nonpositive budgets disable that fitting. Inputs are not mutated.

    Example:
        >>> print(render_ascii_table(["id"], [[1]]))
        +----+
        | id |
        +----+
        | 1  |
        +----+


    :param headers: Ordered column names defining the table's column count.
    :param rows: Ordered value sequences to pad/truncate to the header count.
    :param max_cell_width: Initial character budget for each data cell, including any ellipsis.
    :param max_table_width: Optional positive total character budget including borders and padding.
    :return: Multiline table, ``(no columns)``, or a message when no columns survive fitting.
    """
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
    """
    Convert a selector with ``int``, treating ordinary conversion exceptions as unavailable.

    Example:
        >>> safe_int(" +12 ")
        12
        >>> safe_int("not an id") is None
        True


    :param value: Selector text passed directly to Python's integer conversion.
    :return: Parsed integer, or ``None`` if conversion raises an ``Exception`` subclass.
    """
    try:
        return int(value)
    except Exception:
        return None


def looks_like_id_selector(token: str) -> bool:
    """
    Recognize comma-separated integer tokens and first-dash-separated integer ranges.

    This checks parsing shape, not positivity, range direction, size limits, or row
    existence. Blank comma elements fail. A leading minus is interpreted as a
    range separator, so a single negative integer does not pass this heuristic.

    Example:
        >>> looks_like_id_selector("1, 3-5")
        True
        >>> looks_like_id_selector("5-2")
        True
        >>> looks_like_id_selector("1,,2")
        False


    :param token: Candidate selector text to strip and split into comma-separated elements.
    :return: Whether every element parses as an integer or a two-integer range shape.
    """
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
    """
    Write/flush a prompt, read one line, and use the default for blank input or EOF.

    Typed responses are stripped; returned defaults are not. Stream defaults are
    bound when the function is defined, so callers replacing standard streams
    later should pass them explicitly. Stream errors propagate.

    Example:
        >>> from io import StringIO
        >>> output = StringIO()
        >>> ask_text("Name", default="Untitled", input_stream=StringIO(""), output_stream=output)
        'Untitled'
        >>> output.getvalue()
        'Name [Untitled]: '


    :param prompt: Label displayed before the optional bracketed default and colon.
    :param default: Optional response to return unchanged for blank input or EOF.
    :param input_stream: Text stream supplying one ``readline`` result.
    :param output_stream: Text stream receiving the flushed prompt.
    :return: Stripped response, supplied default, or empty text when neither is available.
    """
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
    """
    Prompt with a yes/no hint, accepting common boolean tokens and defaulting once on invalid input.

    Blank input/EOF silently returns the default. Case-insensitive y/yes/1/true/t
    and n/no/0/false/f select explicit values. Other responses print/flush a diagnostic
    and return the default without retrying. Stream errors propagate.

    Example:
        >>> from io import StringIO
        >>> ask_yes_no("Continue", default=False, input_stream=StringIO("YES"), output_stream=StringIO())
        True


    :param prompt: Label displayed with a default-sensitive Y/n or y/N hint.
    :param default: Boolean returned for absent or unrecognized responses.
    :param input_stream: Text stream passed explicitly to the single-response prompt helper.
    :param output_stream: Text stream for prompts and any invalid-response diagnostic.
    :return: Recognized boolean response or the supplied default.
    """
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
