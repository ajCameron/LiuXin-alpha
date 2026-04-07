"""Commands for creating and inspecting links between rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.interfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> Optional[int]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _resolve_table_token(browser, token: str) -> str:
    return browser.resolve_table(token)


def _split_row_ref(token: str):
    text = str(token).strip()
    if ":" not in text:
        return None
    table_token, id_token = text.rsplit(":", 1)
    if not table_token.strip():
        return None
    if _safe_int(id_token) is None:
        return None
    return table_token, id_token


def _consume_row_ref_tokens(args: list[str], start_idx: int):
    if start_idx >= len(args):
        raise ValueError("Missing row reference.")

    token = args[start_idx]
    compact = _split_row_ref(token)
    if compact is not None:
        return start_idx + 1, compact[0], compact[1]

    if start_idx + 1 >= len(args):
        raise ValueError("Missing row id for table {!r}.".format(token))
    id_token = args[start_idx + 1]
    if str(id_token).strip().lower() == "to":
        raise ValueError("Missing row id for table {!r}.".format(token))
    return start_idx + 2, token, id_token


def _parse_two_row_refs(args: list[str], *, usage: str):
    if len(args) < 2:
        raise ValueError("Usage: {}".format(usage))

    idx, left_table, left_id = _consume_row_ref_tokens(args, 0)
    if idx < len(args) and str(args[idx]).strip().lower() == "to":
        idx += 1
    idx, right_table, right_id = _consume_row_ref_tokens(args, idx)
    return left_table, left_id, right_table, right_id, args[idx:]


def _resolve_row_or_error(browser, table_token: str, id_token: str):
    table = _resolve_table_token(browser, table_token)
    row_id = _safe_int(id_token)
    if row_id is None:
        raise ValueError("Row id must be an integer: {!r}".format(id_token))
    row = browser.db.get_row_from_id(table, row_id)
    if row is None:
        raise ValueError("No row found in {} for id {}.".format(table, row_id))
    return table, row


def _parse_priority(value: str):
    text = str(value).strip().lower()
    if text in {"highest", "lowest", "not_set"}:
        return text
    parsed = _safe_int(text)
    if parsed is None:
        raise ValueError("priority must be an integer or one of: highest, lowest, not_set")
    return parsed


def _parse_scalar_value(value: str):
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


def _parse_set_pair(raw: str) -> tuple[str, object]:
    text = str(raw).strip()
    if "=" not in text:
        raise ValueError("`--set` expects key=value, got {!r}".format(raw))
    key, value = text.split("=", 1)
    key = key.strip().lower()
    if not key:
        raise ValueError("`--set` key cannot be blank.")
    return key, _parse_scalar_value(value)


def _parse_link_options(
    args: list[str],
    *,
    allowed_set_fields: set[str],
) -> tuple[object, Optional[str], dict[str, object]]:
    priority: object = "highest"
    link_type: Optional[str] = None
    custom_set_values: dict[str, object] = {}
    idx = 0
    while idx < len(args):
        token = args[idx]
        if token.startswith("--priority="):
            priority = _parse_priority(token.split("=", 1)[1])
            idx += 1
            continue
        if token == "--priority":
            if idx + 1 >= len(args):
                raise ValueError("Missing value for --priority")
            priority = _parse_priority(args[idx + 1])
            idx += 2
            continue
        if token.startswith("--type="):
            link_type = token.split("=", 1)[1].strip() or None
            idx += 1
            continue
        if token == "--type":
            if idx + 1 >= len(args):
                raise ValueError("Missing value for --type")
            link_type = args[idx + 1].strip() or None
            idx += 2
            continue
        if token.startswith("--set="):
            key, value = _parse_set_pair(token.split("=", 1)[1])
            if key not in allowed_set_fields:
                allowed = ", ".join(sorted(allowed_set_fields)) if allowed_set_fields else "<none>"
                raise ValueError(
                    "Invalid `--set` field {!r}. Valid --set fields for this link are: {}".format(key, allowed)
                )
            custom_set_values[key] = value
            idx += 1
            continue
        if token == "--set":
            if idx + 1 >= len(args):
                raise ValueError("Missing value for --set (expected key=value)")
            key, value = _parse_set_pair(args[idx + 1])
            if key not in allowed_set_fields:
                allowed = ", ".join(sorted(allowed_set_fields)) if allowed_set_fields else "<none>"
                raise ValueError(
                    "Invalid `--set` field {!r}. Valid --set fields for this link are: {}".format(key, allowed)
                )
            custom_set_values[key] = value
            idx += 2
            continue
        raise ValueError(
            "Unknown option: {!r}. Supported options: --priority, --type, --set key=value".format(token)
        )
    return priority, link_type, custom_set_values


class LinkCommand(TerminalCommandAPI):
    """Create a link row between two existing rows."""

    name = "link"
    aliases = ()
    summary = (
        "Link rows: link <table_a> <id_a> <table_b> <id_b> "
        "or link <table_a> <id_a> to <table_b> <id_b>"
    )
    usage = (
        "link <table_a> <id_a> <table_b> <id_b> "
        "[--priority <p>] [--type <t>] [--set <k=v> ...]"
    )

    def execute(self, browser, args: list[str]) -> bool:
        left_table_token, left_id_token, right_table_token, right_id_token, option_tokens = _parse_two_row_refs(
            args,
            usage="{} OR link <table_a>:<id_a> to <table_b>:<id_b> [options]".format(self.usage),
        )

        left_table, left_row = _resolve_row_or_error(browser, left_table_token, left_id_token)
        right_table, right_row = _resolve_row_or_error(browser, right_table_token, right_id_token)

        link_table = browser.db.driver_wrapper.get_link_table_name(left_table, right_table)
        if not link_table:
            raise ValueError("No link table exists between {} and {}.".format(left_table, right_table))

        link_base = browser.db.driver_wrapper.get_column_base(link_table)
        prefix = link_base + "_"
        link_columns = set(browser.db.get_column_headings(link_table))
        suffixes = {col[len(prefix):] for col in link_columns if col.startswith(prefix)}

        left_id_column = browser.db.driver_wrapper.get_id_column(left_table)
        right_id_column = browser.db.driver_wrapper.get_id_column(right_table)
        reserved_suffixes = {
            "id",
            str(left_id_column),
            str(right_id_column),
            "priority",
            "type",
        }
        allowed_set_fields = {suffix for suffix in suffixes if suffix not in reserved_suffixes}

        priority, link_type, custom_set_values = _parse_link_options(
            option_tokens,
            allowed_set_fields=allowed_set_fields,
        )

        existing_rows = browser.db.get_interlink_row(primary_row=left_row, secondary_row=right_row, onelink=False)
        if existing_rows:
            count = len(existing_rows) if isinstance(existing_rows, list) else 1
            browser.emit(
                "Rows are already linked ({} existing link row{}).".format(
                    count,
                    "" if count == 1 else "s",
                )
            )
            return True

        if link_type is not None:
            try:
                browser.db.driver_wrapper.get_link_column(left_table, right_table, "type")
            except Exception:
                raise ValueError(
                    "Link table {} has no `type` column; omit --type for this table pair.".format(link_table)
                )

        kwargs = {"priority": priority}
        if link_type is not None:
            kwargs["type"] = link_type
        kwargs.update(custom_set_values)
        link_row = browser.db.interlink_rows(
            primary_row=left_row,
            secondary_row=right_row,
            **kwargs,
        )
        browser.emit(
            "Link created: table={} id={} ({}:{} <-> {}:{})".format(
                link_table,
                link_row.row_id,
                left_table,
                left_row.row_id,
                right_table,
                right_row.row_id,
            )
        )
        return True


class UnlinkCommand(TerminalCommandAPI):
    """Remove link rows between two existing rows."""

    name = "unlink"
    aliases = ()
    summary = "Unlink two rows: unlink <table_a> <id_a> <table_b> <id_b>"
    usage = "unlink <table_a> <id_a> <table_b> <id_b>"

    def execute(self, browser, args: list[str]) -> bool:
        left_table_token, left_id_token, right_table_token, right_id_token, trailing = _parse_two_row_refs(
            args,
            usage="{} OR unlink <table_a>:<id_a> <table_b>:<id_b>".format(self.usage),
        )
        if trailing:
            raise ValueError("Usage: {}".format(self.usage))

        left_table, left_row = _resolve_row_or_error(browser, left_table_token, left_id_token)
        right_table, right_row = _resolve_row_or_error(browser, right_table_token, right_id_token)

        link_table = browser.db.driver_wrapper.get_link_table_name(left_table, right_table)
        if not link_table:
            raise ValueError("No link table exists between {} and {}.".format(left_table, right_table))

        existing_rows = browser.db.get_interlink_row(primary_row=left_row, secondary_row=right_row, onelink=False)
        if not existing_rows:
            browser.emit("Rows are not linked.")
            return True

        rows = existing_rows if isinstance(existing_rows, list) else [existing_rows]
        for row in rows:
            browser.db.delete(row)

        browser.emit(
            "Unlinked {} row{} from {}.".format(
                len(rows),
                "" if len(rows) == 1 else "s",
                link_table,
            )
        )
        return True


class LinksCommand(TerminalCommandAPI):
    """Inspect linked rows for a target row."""

    name = "links"
    aliases = ()
    summary = "Show linked rows: links <table> <id> [other_table]"
    usage = "links <table> <id> [other_table]"

    def execute(self, browser, args: list[str]) -> bool:
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        idx, source_table_token, source_id_token = _consume_row_ref_tokens(args, 0)
        source_table, source_row = _resolve_row_or_error(browser, source_table_token, source_id_token)

        trailing = args[idx:]
        if len(trailing) > 1:
            raise ValueError("Usage: {}".format(self.usage))

        if trailing:
            candidate_tables = [_resolve_table_token(browser, trailing[0])]
        else:
            candidate_tables = sorted(browser.db.driver_wrapper.get_interlinked_tables(source_table))

        shown_any = False
        for table in candidate_tables:
            if table == source_table:
                continue
            try:
                rows = browser.db.get_interlinked_rows(target_row=source_row, secondary_table=table)
            except Exception:
                continue
            if not rows:
                continue
            shown_any = True
            browser.emit(
                "Linked {} rows for {} id {}: {}".format(
                    table,
                    source_table,
                    source_row.row_id,
                    len(rows),
                )
            )
            for row in rows[: browser.page_size]:
                browser.emit("  {}".format(browser.format_row(table, row)))
            if len(rows) > browser.page_size:
                browser.emit("  ... {} more".format(len(rows) - browser.page_size))

        if not shown_any:
            browser.emit("No linked rows found for {} id {}.".format(source_table, source_row.row_id))
        return True
