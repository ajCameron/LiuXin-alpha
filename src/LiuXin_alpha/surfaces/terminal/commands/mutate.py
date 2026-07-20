"""Commands for editing existing rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.library.library import Library


def _safe_int(value: str) -> Optional[int]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _split_row_ref(token: str) -> tuple[str, str] | None:
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
    text = str(column).strip().lower()
    if not text:
        return "other"
    if id_column is not None and text == str(id_column).strip().lower():
        return "identity"
    if text.endswith("_id"):
        return "references"
    if text.startswith("supports_") or text.startswith("is_") or "read_only" in text or "eventually_consistent" in text:
        return "capabilities"
    if any(token in text for token in ("timestamp", "datestamp", "date", "year", "seen", "healthcheck")):
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
    mapping = {
        "identity": "Identity",
        "references": "References",
        "access": "Access",
        "capabilities": "Capabilities",
        "dates": "Dates",
        "other": "Other",
    }
    return mapping.get(str(group).strip().lower(), str(group).strip().title() or "Other")


def _consume_target_tokens(args: list[str], *, usage: str) -> tuple[str, int, list[str]]:
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


def _local_library(browser) -> Library:
    return Library(database=browser.catalog, close_database_on_close=False)


def _fetch_row(browser, *, table: str, row_id: int):
    if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
        return browser.execute_core_query(
            "invoke",
            payload={
                "target": "library",
                "method": "get_row",
                "kwargs": {
                    "table": table,
                    "row_id": int(row_id),
                },
            },
        )
    return _local_library(browser).get_row(table=table, row_id=int(row_id))


def _update_row_fields(browser, *, table: str, row_id: int, updates: dict[str, object]):
    if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
        return browser.execute_core_command(
            "invoke",
            payload={
                "target": "library",
                "method": "update_row_fields",
                "kwargs": {
                    "table": table,
                    "row_id": int(row_id),
                    "updates": dict(updates),
                },
            },
        )
    return _local_library(browser).update_row_fields(table=table, row_id=int(row_id), updates=updates)


def _delete_row(browser, *, table: str, row_id: int):
    if hasattr(browser, "supports_core_commands") and bool(browser.supports_core_commands()):
        return browser.execute_core_command(
            "invoke",
            payload={
                "target": "library",
                "method": "delete_row",
                "kwargs": {
                    "table": table,
                    "row_id": int(row_id),
                },
            },
        )
    return _local_library(browser).delete_row(table=table, row_id=int(row_id))


def _describe_delete_impact(browser, *, table: str, row_id: int):
    if hasattr(browser, "supports_core_queries") and bool(browser.supports_core_queries()):
        return browser.execute_core_query(
            "invoke",
            payload={
                "target": "library",
                "method": "describe_row_delete_impact",
                "kwargs": {
                    "table": table,
                    "row_id": int(row_id),
                },
            },
        )
    return _local_library(browser).describe_row_delete_impact(table=table, row_id=int(row_id))


def _emit_delete_preview_samples(browser, *, table: str, count: int, sample_rows) -> None:
    rows = list(sample_rows or ())
    if not rows:
        return
    for row in rows:
        browser.emit("    - {}".format(browser.format_row(table, row)))
    remaining = max(0, int(count) - len(rows))
    if remaining > 0:
        browser.emit("    ... {} more".format(remaining))


def _prompt_edit_value(browser, *, label: str, current_value: object) -> tuple[bool, str]:
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


def _ordered_edit_columns(browser, *, table: str, columns: list[str]) -> list[tuple[str, str, str]]:
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
    table: str
    row_id: int
    row_data: dict[str, object]


def _resolve_target(browser, args: list[str], *, usage: str) -> tuple[_ResolvedTarget, list[str]]:
    table_token, row_id, remainder = _consume_target_tokens(args, usage=usage)
    table = browser.resolve_table(table_token)
    row_data = _fetch_row(browser, table=table, row_id=row_id)
    if row_data is None:
        raise ValueError("No row found in {} for id {}.".format(table, row_id))
    return _ResolvedTarget(table=table, row_id=int(row_id), row_data=dict(row_data)), remainder


class SetCommand(TerminalCommandAPI):
    """Update one field on an existing row."""

    name = "set"
    aliases = ("update",)
    summary = "Update one field on an existing row."
    usage = "set <table> <id> <column> <value...> OR set <table>:<id> <column> <value...>"

    def execute(self, browser, args: list[str]) -> bool:
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
    """Interactively update one or more fields on an existing row."""

    name = "edit"
    aliases = ()
    summary = "Interactively edit one row."
    usage = "edit <table> <id> [column ...] OR edit <table>:<id> [column ...]"

    def execute(self, browser, args: list[str]) -> bool:
        target, remainder = _resolve_target(browser, args, usage=self.usage)
        id_column = browser.get_table_id_column(target.table)

        if remainder:
            columns = [browser.resolve_table_column(target.table, token) for token in remainder]
        else:
            columns = [column for column in browser.get_table_columns(target.table) if column != id_column]

        if id_column is not None and id_column in columns:
            raise ValueError("Cannot edit id column {!r}.".format(id_column))
        if not columns:
            raise ValueError("No editable columns available for table {!r}.".format(target.table))

        ordered_fields = _ordered_edit_columns(browser, table=target.table, columns=columns)
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
            new_value = _coerce_field_value(raw_value, current_value=current_row.get(column))
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
    """Delete one existing row."""

    name = "delete"
    aliases = ("remove",)
    summary = "Delete one row."
    usage = "delete <table> <id> [--force] OR delete <table>:<id> [--force]"

    def execute(self, browser, args: list[str]) -> bool:
        target, remainder = _resolve_target(browser, args, usage=self.usage)
        impact = _describe_delete_impact(browser, table=target.table, row_id=target.row_id)
        force = False
        for token in remainder:
            normalized = str(token).strip().lower()
            if normalized in {"--force", "-f"}:
                force = True
                continue
            raise ValueError("Unknown option: {!r}. Supported options: --force".format(token))

        browser.emit("Delete preview for {}:{}".format(target.table, target.row_id))
        browser.emit("  {}".format(browser.format_row(target.table, impact.get("row", target.row_data))))
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
                browser.emit("  {}.{}: {}".format(item["table"], item["column"], item["count"]))
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
