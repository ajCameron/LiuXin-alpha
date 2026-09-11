"""
Resolve terminal row targets and dispatch field updates, interactive edits, and deletion through Core.

Input coercion is based on current Python values rather than a schema type system.
Interactive edits collect changes before one update request; deletion shows a
separate impact preview before confirmation. These adapters do not provide an
atomic preview/write boundary or undo a successful write when later output fails.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> Optional[int]:
    """
    Parse stripped integer text, returning ``None`` for blank or invalid integer syntax.

    Conversion to text occurs before the exception handler and may still raise.

    Example:
        >>> _safe_int(" 12 "), _safe_int("1.2")
        (12, None)


    :param value: Value stringified and stripped before integer conversion.
    :return: Parsed integer without range checks, or ``None`` for blank/invalid text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _split_row_ref(token: str) -> tuple[str, str] | None:
    """
    Recognize compact table/ID syntax by splitting the final colon and checking integer text.

    Substrings retain spaces left after outer stripping. IDs need not be positive,
    and tables are not resolved here.

    Example:
        >>> _split_row_ref(" works:12 "), _split_row_ref("works:many")
        (('works', '12'), None)


    :param token: Candidate compact row reference, converted to text.
    :return: Table/ID text pair or ``None`` if compact syntax is not recognized.
    """
    text = str(token).strip()
    if ":" not in text:
        return None
    table_token, id_token = text.rsplit(":", 1)
    if not table_token.strip():
        return None
    if _safe_int(id_token) is None:
        return None
    return table_token, id_token


def _parse_scalar_value(value: str) -> object:
    """
    Parse null/Boolean/integer/decimal spellings, otherwise retain stripped text.

    Boolean words become integer zero/one. Floats are attempted only for text
    containing a dot, so exponent-only spellings such as ``1e3`` remain strings.

    Example:
        >>> [_parse_scalar_value(value) for value in ("null", "yes", "1.5", "1e3")]
        [None, 1, 1.5, '1e3']


    :param value: Raw value token whose surrounding whitespace is discarded.
    :return: Parsed scalar or stripped text when none of the conversion rules applies.
    """
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"none", "null"}:
        return None
    if lowered in {"true", "yes"}:
        return 1
    if lowered in {"false", "no"}:
        return 0
    maybe_int = _safe_int(text)
    if maybe_int is not None:
        return maybe_int
    try:
        if "." in text:
            return float(text)
    except Exception:
        pass
    return text


def _coerce_field_value(raw: str, *, current_value: Any) -> object:
    """
    Interpret edited text using the current value as a heuristic, without enforcing its type.

    Null aliases win even for existing strings, so literal ``null``/``none`` cannot
    be stored through this coercion path. Other strings stay strings. Boolean and
    integer fields recognize Boolean words; float fields accept full float syntax.
    Failed numeric conversion falls back to scalar parsing rather than rejection.

    Example:
        >>> _coerce_field_value("12", current_value="old")
        '12'
        >>> _coerce_field_value("1e3", current_value=0.0)
        1000.0
        >>> _coerce_field_value("null", current_value="old") is None
        True


    :param raw: Replacement text, stripped before all interpretation.
    :param current_value: Existing Python value guiding conversion, not a schema constraint.
    :return: Replacement scalar/text value, which may have a different type from the original.
    """
    text = str(raw).strip()
    lowered = text.lower()
    if lowered in {"none", "null"}:
        return None
    if isinstance(current_value, str):
        return text
    if isinstance(current_value, bool):
        if lowered in {"true", "yes", "1"}:
            return 1
        if lowered in {"false", "no", "0"}:
            return 0
        return _parse_scalar_value(text)
    if isinstance(current_value, int):
        if lowered in {"true", "yes"}:
            return 1
        if lowered in {"false", "no"}:
            return 0
        maybe_int = _safe_int(text)
        if maybe_int is not None:
            return maybe_int
        return _parse_scalar_value(text)
    if isinstance(current_value, float):
        try:
            return float(text)
        except Exception:
            return _parse_scalar_value(text)
    return _parse_scalar_value(text)


def _row_detail_group(column: str, *, id_column: Optional[str]) -> str:
    """
    Classify a column by ordered naming heuristics for interactive edit sections.

    Exact identity wins, then other ID suffixes, capability names, date-like tokens,
    access-related tokens, and identity-like tokens. Unknown names use ``other``.
    Substring matching is intentionally broader than semantic schema metadata.

    Example:
        >>> _row_detail_group("work_id", id_column="work_id")
        'identity'
        >>> _row_detail_group("store_id", id_column="work_id")
        'references'


    :param column: Column name, converted to stripped lowercase text for matching.
    :param id_column: Optional primary identity-column name for the highest-priority match.
    :return: Lowercase section key used to order and label editing prompts.
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


def _pretty_row_detail_group(group: str) -> str:
    """
    Convert a section key into its display label, using title case for unknown keys.

    Example:
        >>> _pretty_row_detail_group(" capabilities "), _pretty_row_detail_group("")
        ('Capabilities', 'Other')


    :param group: Section token, stripped and matched case-insensitively.
    :return: Known section label, title-cased fallback, or ``Other`` for blank text.
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


def _consume_target_tokens(
    args: list[str], *, usage: str
) -> tuple[str, int, list[str]]:
    """
    Consume one compact reference or separate table/ID tokens and preserve the remaining arguments.

    Table names are not resolved and integer IDs are not range-checked here.

    Example:
        >>> _consume_target_tokens(["works:1", "work_title", "New"], usage="set")
        ('works', 1, ['work_title', 'New'])


    :param args: Target reference followed by command-specific tokens, left unchanged.
    :param usage: Usage text for missing reference pieces.
    :return: Table token, parsed integer ID, and a new list of trailing tokens.
    :raises ValueError: If target syntax is incomplete or the split-form ID is not an integer.
    """
    if not args:
        raise ValueError("Usage: {}".format(usage))

    compact = _split_row_ref(args[0])
    if compact is not None:
        row_id = _safe_int(compact[1])
        if row_id is None:
            raise ValueError("Row id must be an integer.")
        return compact[0], row_id, list(args[1:])

    if len(args) < 2:
        raise ValueError("Usage: {}".format(usage))
    row_id = _safe_int(args[1])
    if row_id is None:
        raise ValueError("Row id must be an integer.")
    return str(args[0]), row_id, list(args[2:])


def _fetch_row(browser, *, table: str, row_id: int):
    """
    Query one Core row and extract its values with permissive response-shape handling.

    An absent/``None`` record means not found. Non-dictionary records become empty
    values; dictionary values are shallow-copied, while non-dictionary values pass
    through unchanged. This helper does not fully validate the wire record.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_query.return_value = {"record": {"values": {"work_title": "Old"}}}
        >>> _fetch_row(host, table="works", row_id=1)
        {'work_title': 'Old'}


    :param browser: Host providing Core query dispatch.
    :param table: Resolved table name for the ``rows.get`` payload.
    :param row_id: Row identifier converted to an integer for the query.
    :return: Extracted row values, or ``None`` when the response has no record.
    """
    result = browser.execute_core_query(
        "rows.get",
        payload={"table": table, "row_id": int(row_id)},
    )
    record = (result or {}).get("record")
    if record is None:
        return None
    values = record.get("values", {}) if isinstance(record, dict) else {}
    return dict(values) if isinstance(values, dict) else values


def _update_row_fields(browser, *, table: str, row_id: int, updates: dict[str, object]):
    """
    Send a shallow copy of field updates to Core and unwrap a mapping-shaped result when available.

    If result/record/values are not all mappings, the original command result is
    returned unchanged. This is response adaptation, not confirmation by a new read.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_command.return_value = {"record": {"values": {"work_title": "New"}}}
        >>> _update_row_fields(host, table="works", row_id=1, updates={"work_title": "New"})
        {'work_title': 'New'}


    :param browser: Host exposing Core administrative command dispatch.
    :param table: Table to update, forwarded without local schema validation.
    :param row_id: Target identifier converted to an integer.
    :param updates: Field-value mapping shallow-copied into the update payload.
    :return: Copied returned values when fully mapping-shaped, otherwise Core's original result.
    """
    result = browser.execute_core_command(
        "admin.row.update",
        payload={
            "table": table,
            "row_id": int(row_id),
            "updates": dict(updates),
        },
    )
    if isinstance(result, Mapping):
        record = result.get("record")
        if isinstance(record, Mapping):
            values = record.get("values")
            if isinstance(values, Mapping):
                return dict(values)
    return result


def _delete_row(browser, *, table: str, row_id: int):
    """
    Dispatch one administrative row deletion without prompting or previewing its effects.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_command.return_value = {"deleted": True}
        >>> _delete_row(host, table="works", row_id=1)
        {'deleted': True}


    :param browser: Host supplying Core command dispatch.
    :param table: Target table forwarded to the deletion endpoint.
    :param row_id: Target row identifier, converted to an integer.
    :return: Core's deletion result unchanged; command errors propagate.
    """
    return browser.execute_core_command(
        "admin.row.delete",
        payload={"table": table, "row_id": int(row_id)},
    )


def _describe_delete_impact(browser, *, table: str, row_id: int):
    """
    Request Core's row-deletion impact description without deleting the target.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.execute_core_query.return_value = {"reference_counts": []}
        >>> _describe_delete_impact(host, table="works", row_id=1)
        {'reference_counts': []}


    :param browser: Host exposing Core query dispatch.
    :param table: Table whose row dependencies should be described.
    :param row_id: Target identifier converted to an integer for the impact query.
    :return: Core impact payload unchanged; no atomic link to a later deletion is established here.
    """
    return browser.execute_core_query(
        "admin.row.delete-impact",
        payload={"table": table, "row_id": int(row_id)},
    )


def _emit_delete_preview_samples(
    browser, *, table: str, count: int, sample_rows
) -> None:
    """
    Render every supplied preview sample and report any positive difference from the total count.

    Empty samples return without a remaining-count line. No local sample cap is
    applied; the impact-query provider is responsible for selecting samples.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> _emit_delete_preview_samples(host, table="tags", count=10, sample_rows=[])
        >>> host.emit.assert_not_called()


    :param browser: Host providing single-row formatting and output.
    :param table: Table passed to the sample row formatter.
    :param count: Reported full count used only for the trailing remaining-items line.
    :param sample_rows: Optional iterable materialized and rendered in its original order.
    :return: ``None`` after all sample output, or immediately when no samples are supplied.
    """
    rows = list(sample_rows or ())
    if not rows:
        return
    for row in rows:
        browser.emit("    - {}".format(browser.format_row(table, row)))
    remaining = max(0, int(count) - len(rows))
    if remaining > 0:
        browser.emit("    ... {} more".format(remaining))


def _prompt_edit_value(
    browser, *, label: str, current_value: object
) -> tuple[bool, str]:
    """
    Prompt once with the current value and treat blank input or EOF as keeping it.

    Output is flushed before reading. Nonblank input is stripped; stream failures
    propagate. EOF does not cancel earlier edits collected by the caller.

    Example:
        >>> import io
        >>> from types import SimpleNamespace
        >>> host = SimpleNamespace(input=io.StringIO(" New " + chr(10)), output=io.StringIO())
        >>> _prompt_edit_value(host, label="Title", current_value="Old")
        (False, 'New')


    :param browser: Host providing text input/output streams.
    :param label: Prompt label written before the bracketed current value.
    :param current_value: Existing value displayed as text, or ``<null>`` when ``None``.
    :return: Keep-current flag and replacement text; kept values use an empty replacement string.
    """
    current_text = "<null>" if current_value is None else str(current_value)
    browser.output.write("{} [{}]: ".format(label, current_text))
    browser.output.flush()
    raw = browser.input.readline()
    if raw == "":
        return True, ""
    value = raw.rstrip("\r\n")
    if not value.strip():
        return True, ""
    return False, value.strip()


def _ordered_edit_columns(
    browser, *, table: str, columns: list[str]
) -> list[tuple[str, str, str]]:
    """
    Deduplicate requested columns and order them by edit section, identity, and schema position.

    Display names are zipped to full schema names; missing display entries fall
    back to the full name. Unknown columns are not rejected and sort after known
    positions within their group, retaining input order for ties.

    Example:
        >>> ordered = _ordered_edit_columns(browser, table="works", columns=["work_title", "work_id"])  # doctest: +SKIP


    :param browser: Host supplying full/display columns and the table's identity column.
    :param table: Table used for schema and identity lookups.
    :param columns: Requested column names, deduplicated by exact spelling without mutating the list.
    :return: Ordered section-key/full-column/display-name triples.
    """
    all_columns = browser.get_table_columns(table)
    display_columns = browser.get_table_display_columns(table)
    display_by_column = dict(zip(all_columns, display_columns))
    position = {column: idx for idx, column in enumerate(all_columns)}
    id_column = browser.get_table_id_column(table)
    group_rank = {
        "identity": 0,
        "references": 1,
        "access": 2,
        "capabilities": 3,
        "dates": 4,
        "other": 5,
    }

    unique_columns: list[str] = []
    seen: set[str] = set()
    for column in columns:
        if column in seen:
            continue
        seen.add(column)
        unique_columns.append(column)

    unique_columns.sort(
        key=lambda column: (
            group_rank.get(_row_detail_group(column, id_column=id_column), 99),
            0 if id_column is not None and column == id_column else 1,
            position.get(column, 9999),
        )
    )
    return [
        (
            _row_detail_group(column, id_column=id_column),
            column,
            display_by_column.get(column, column),
        )
        for column in unique_columns
    ]


@dataclass(frozen=True)
class _ResolvedTarget:
    """
    Pair a resolved table and integer ID with a shallow row-value snapshot for editing.

    Frozen attributes do not make ``row_data`` immutable or keep it synchronized
    with later database changes. Direct construction does not validate the row.

    Example:
        >>> target = _ResolvedTarget("works", 1, {"work_title": "Old"})
        >>> target.row_data["work_title"]
        'Old'
    """

    table: str
    row_id: int
    row_data: dict[str, object]


def _resolve_target(
    browser, args: list[str], *, usage: str
) -> tuple[_ResolvedTarget, list[str]]:
    """
    Consume a target reference, resolve its table, and capture current row values through Core.

    Copying malformed values can still raise; no schema validation or lock is
    acquired for later mutation by this adapter.

    Example:
        >>> target, remaining = _resolve_target(browser, ["works:1", "title", "New"], usage="set")  # doctest: +SKIP


    :param browser: Host supplying table resolution and Core row queries.
    :param args: Target reference followed by tokens left for the command.
    :param usage: Usage text for missing reference components.
    :return: Resolved target snapshot and remaining argument list.
    :raises ValueError: If target syntax/resolution fails or Core returns no row record.
    """
    table_token, row_id, remainder = _consume_target_tokens(args, usage=usage)
    table = browser.resolve_table(table_token)
    row_data = _fetch_row(browser, table=table, row_id=row_id)
    if row_data is None:
        raise ValueError("No row found in {} for id {}.".format(table, row_id))
    return _ResolvedTarget(
        table=table, row_id=int(row_id), row_data=dict(row_data)
    ), remainder


class SetCommand(TerminalCommandAPI):
    """
    Set one non-identity field from tokenized text, using its current value to guide coercion.

    ``set`` and ``update`` do not prompt for confirmation. String whitespace is
    stripped and null aliases can clear even an existing string field.

    Example:
        >>> SetCommand().aliases
        ('update',)
    """

    name = "set"
    aliases = ("update",)
    summary = "Update one field on an existing row."
    usage = (
        "set <table> <id> <column> <value...> OR set <table>:<id> <column> <value...>"
    )

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve the target/column, reject the identity field, and send one Core update before reporting it.

        Remaining value tokens are stringified and joined with single spaces.
        Coercion is heuristic, not type validation. If Core returns an unexpected
        result shape or output fails, the update may already have succeeded.

        Example:
            >>> SetCommand().execute(browser, ["works:1", "title", "New", "title"])  # doctest: +SKIP


        :param browser: Host supplying Core access, column resolution, and terminal output.
        :param args: Target reference, column token, and at least one value token.
        :return: ``True`` after the update result has been reported.
        :raises ValueError: If syntax/resolution fails or the selected column is the identity column.
        """
        target, remainder = _resolve_target(browser, args, usage=self.usage)
        if len(remainder) < 2:
            raise ValueError("Usage: {}".format(self.usage))

        column = browser.resolve_table_column(target.table, remainder[0])
        id_column = browser.get_table_id_column(target.table)
        if id_column is not None and column == id_column:
            raise ValueError("Cannot update id column {!r}.".format(id_column))

        raw_value = " ".join(str(token) for token in remainder[1:])
        current_value = target.row_data.get(column)
        new_value = _coerce_field_value(raw_value, current_value=current_value)
        updated = _update_row_fields(
            browser,
            table=target.table,
            row_id=target.row_id,
            updates={column: new_value},
        )

        browser.emit(
            "Updated {}:{} {}={!r}".format(
                target.table,
                target.row_id,
                column,
                updated.get(column),
            )
        )
        return True


class EditCommand(TerminalCommandAPI):
    """
    Prompt for selected non-identity fields, then submit collected changes in one Core update.

    Omitted column arguments select all non-identity columns. Blank input and EOF
    keep the current field; they do not cancel changes collected for previous fields.

    Example:
        >>> EditCommand().name
        'edit'
    """

    name = "edit"
    aliases = ()
    summary = "Interactively edit one row."
    usage = "edit <table> <id> [column ...] OR edit <table>:<id> [column ...]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Order editable fields, prompt once per field, and update values that compare unequal to the snapshot.

        Input is coerced using current Python values; equality decides whether to
        record a change. No update is sent when no fields differ. There is no final
        confirmation or concurrency recheck before the single update request, and
        later output failure does not undo a successful write.

        Example:
            >>> EditCommand().execute(browser, ["works:1", "title"])  # doctest: +SKIP


        :param browser: Host supplying Core/schema access, prompt streams, and output.
        :param args: Target reference followed by optional column tokens to edit.
        :return: ``True`` after saving collected changes or reporting that none were saved.
        :raises ValueError: If target/column resolution fails, identity editing is requested, or no columns remain.
        """
        target, remainder = _resolve_target(browser, args, usage=self.usage)
        id_column = browser.get_table_id_column(target.table)

        if remainder:
            columns = [
                browser.resolve_table_column(target.table, token) for token in remainder
            ]
        else:
            columns = [
                column
                for column in browser.get_table_columns(target.table)
                if column != id_column
            ]

        if id_column is not None and id_column in columns:
            raise ValueError("Cannot edit id column {!r}.".format(id_column))
        if not columns:
            raise ValueError(
                "No editable columns available for table {!r}.".format(target.table)
            )

        ordered_fields = _ordered_edit_columns(
            browser, table=target.table, columns=columns
        )
        browser.emit(
            "Editing {}:{} | Enter keeps current value | type `null` to clear".format(
                target.table,
                target.row_id,
            )
        )

        changes: dict[str, object] = {}
        last_group: Optional[str] = None
        current_row = dict(target.row_data)
        for group_name, column, display_name in ordered_fields:
            if group_name != last_group:
                if last_group is not None:
                    browser.emit("")
                browser.emit(_pretty_row_detail_group(group_name))
                last_group = group_name

            label = display_name
            if display_name != column:
                label = "{} ({})".format(display_name, column)
            keep_current, raw_value = _prompt_edit_value(
                browser,
                label=label,
                current_value=current_row.get(column),
            )
            if keep_current:
                continue
            new_value = _coerce_field_value(
                raw_value, current_value=current_row.get(column)
            )
            if current_row.get(column) == new_value:
                continue
            current_row[column] = new_value
            changes[column] = new_value

        if not changes:
            browser.emit("")
            browser.emit("No changes saved.")
            return True

        _update_row_fields(
            browser,
            table=target.table,
            row_id=target.row_id,
            updates=changes,
        )
        browser.emit("")
        browser.emit(
            "Updated {}:{} ({} field{}): {}".format(
                target.table,
                target.row_id,
                len(changes),
                "" if len(changes) == 1 else "s",
                ", ".join(sorted(changes.keys())),
            )
        )
        return True


class DeleteCommand(TerminalCommandAPI):
    """
    Preview an existing row's deletion impact and request confirmation unless forced.

    ``--force`` skips only confirmation, not impact discovery or preview output.
    Actual dependent-row handling belongs to Core's deletion operation.

    Example:
        >>> DeleteCommand().aliases
        ('remove',)
    """

    name = "delete"
    aliases = ("remove",)
    summary = "Delete one row."
    usage = "delete <table> <id> [--force] OR delete <table>:<id> [--force]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Fetch target and impact, render dependency samples, then conditionally dispatch Core deletion.

        The impact query precedes option validation. Only force aliases are
        accepted; otherwise confirmation defaults to false. Report warnings are
        displayed without blocking deletion. The adapter does not ensure that
        the preview remains current, and post-delete output errors do not restore data.

        Example:
            >>> DeleteCommand().execute(browser, ["works:1"])  # doctest: +SKIP


        :param browser: Host supplying Core access, row formatting, confirmation, and output.
        :param args: Target reference followed by optional case-insensitive ``--force`` or ``-f`` tokens.
        :return: ``True`` after deletion or cancellation, keeping the browser session running.
        :raises ValueError: If the target cannot resolve or an unsupported trailing option is supplied.
        """
        target, remainder = _resolve_target(browser, args, usage=self.usage)
        impact = _describe_delete_impact(
            browser, table=target.table, row_id=target.row_id
        )
        force = False
        for token in remainder:
            normalized = str(token).strip().lower()
            if normalized in {"--force", "-f"}:
                force = True
                continue
            raise ValueError(
                "Unknown option: {!r}. Supported options: --force".format(token)
            )

        browser.emit("Delete preview for {}:{}".format(target.table, target.row_id))
        browser.emit(
            "  {}".format(
                browser.format_row(target.table, impact.get("row", target.row_data))
            )
        )
        interlinked_counts = list(impact.get("interlinked_counts", ()) or ())
        reference_counts = list(impact.get("reference_counts", ()) or ())
        if interlinked_counts:
            browser.emit("Linked rows:")
            for item in interlinked_counts:
                browser.emit("  {}: {}".format(item["table"], item["count"]))
                _emit_delete_preview_samples(
                    browser,
                    table=str(item["table"]),
                    count=int(item["count"]),
                    sample_rows=item.get("sample_rows", ()),
                )
        if reference_counts:
            browser.emit("Direct references:")
            for item in reference_counts:
                browser.emit(
                    "  {}.{}: {}".format(item["table"], item["column"], item["count"])
                )
                _emit_delete_preview_samples(
                    browser,
                    table=str(item["table"]),
                    count=int(item["count"]),
                    sample_rows=item.get("sample_rows", ()),
                )
        if not interlinked_counts and not reference_counts:
            browser.emit("No linked rows or direct references detected.")
        warning = str(impact.get("warning", "") or "").strip()
        if warning:
            browser.emit(warning)

        if not force:
            confirmed = browser.prompt_yes_no(
                "Delete {}:{}?".format(target.table, target.row_id),
                default=False,
            )
            if not confirmed:
                browser.emit("Delete canceled.")
                return True

        _delete_row(browser, table=target.table, row_id=target.row_id)
        browser.emit("Deleted {}:{}.".format(target.table, target.row_id))
        return True


__all__ = [
    "DeleteCommand",
    "EditCommand",
    "SetCommand",
]
