"""`store` command group for richer store inspection."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


@dataclass(frozen=True)
class _StoreListOptions:
    limit: int
    offset: int
    kind_filters: Optional[set[str]]
    status_filters: Optional[set[str]]
    protocol_filters: Optional[set[str]]
    name_filter: Optional[str]
    read_only_filter: Optional[bool]
    min_files: Optional[int]
    max_files: Optional[int]
    sort_by: str
    sort_desc: bool


def _read_option_value(args: list[str], idx: int, *, option_name: str) -> tuple[str, int]:
    token = args[idx]
    if "=" in token:
        _, value = token.split("=", 1)
        if value.strip() == "":
            raise ValueError("Option {} requires a non-blank value.".format(option_name))
        return value, idx + 1
    if idx + 1 >= len(args):
        raise ValueError("Option {} requires a value.".format(option_name))
    value = args[idx + 1]
    if str(value).strip() == "":
        raise ValueError("Option {} requires a non-blank value.".format(option_name))
    return value, idx + 2


def _split_csv_tokens(raw: str) -> Optional[set[str]]:
    text = str(raw).strip().lower()
    if not text:
        return None
    values: set[str] = set()
    for part in text.replace(";", ",").split(","):
        token = part.strip().lower()
        if token:
            values.add(token)
    return values or None


def _parse_sort(raw: str) -> tuple[str, bool]:
    text = str(raw).strip().lower()
    if not text:
        raise ValueError("sort value cannot be blank")
    sort_desc = False
    if ":" in text:
        key, direction = text.split(":", 1)
        text = key.strip()
        direction = direction.strip()
        if direction in {"desc", "d", "down"}:
            sort_desc = True
        elif direction in {"asc", "a", "up"}:
            sort_desc = False
        else:
            raise ValueError("Unknown sort direction {!r}. Use asc or desc.".format(direction))
    allowed = {"id", "name", "kind", "protocol", "status", "files"}
    if text not in allowed:
        raise ValueError("Unknown sort key {!r}. Use one of: {}.".format(text, ", ".join(sorted(allowed))))
    return text, sort_desc


def _parse_store_list_options(args: list[str], *, usage: str, default_limit: int) -> _StoreListOptions:
    limit = max(1, int(default_limit))
    offset = 0
    positional: list[str] = []
    kind_filters: Optional[set[str]] = None
    status_filters: Optional[set[str]] = None
    protocol_filters: Optional[set[str]] = None
    name_filter: Optional[str] = None
    read_only_filter: Optional[bool] = None
    min_files: Optional[int] = None
    max_files: Optional[int] = None
    sort_by = "id"
    sort_desc = False

    idx = 0
    while idx < len(args):
        token = str(args[idx]).strip()

        if token == "--kind" or token.startswith("--kind="):
            value, idx = _read_option_value(args, idx, option_name="--kind")
            kind_filters = _split_csv_tokens(value)
            if not kind_filters:
                raise ValueError("Option --kind requires at least one value.")
            continue
        if token == "--status" or token.startswith("--status="):
            value, idx = _read_option_value(args, idx, option_name="--status")
            status_filters = _split_csv_tokens(value)
            if not status_filters:
                raise ValueError("Option --status requires at least one value.")
            continue
        if token == "--protocol" or token.startswith("--protocol="):
            value, idx = _read_option_value(args, idx, option_name="--protocol")
            protocol_filters = _split_csv_tokens(value)
            if not protocol_filters:
                raise ValueError("Option --protocol requires at least one value.")
            continue
        if token in {"--name", "--name-contains"} or token.startswith("--name=") or token.startswith(
            "--name-contains="
        ):
            value, idx = _read_option_value(args, idx, option_name="--name")
            name_filter = str(value).strip().lower()
            if not name_filter:
                raise ValueError("Option --name requires a non-blank value.")
            continue
        if token == "--read-only":
            read_only_filter = True
            idx += 1
            continue
        if token == "--writable":
            read_only_filter = False
            idx += 1
            continue
        if token == "--min-files" or token.startswith("--min-files="):
            value, idx = _read_option_value(args, idx, option_name="--min-files")
            parsed = _safe_int(value)
            if parsed is None:
                raise ValueError("Option --min-files requires an integer value.")
            min_files = max(0, parsed)
            continue
        if token == "--max-files" or token.startswith("--max-files="):
            value, idx = _read_option_value(args, idx, option_name="--max-files")
            parsed = _safe_int(value)
            if parsed is None:
                raise ValueError("Option --max-files requires an integer value.")
            max_files = max(0, parsed)
            continue
        if token == "--sort" or token.startswith("--sort="):
            value, idx = _read_option_value(args, idx, option_name="--sort")
            sort_by, sort_desc = _parse_sort(value)
            continue
        if token == "--desc":
            sort_desc = True
            idx += 1
            continue
        if token == "--asc":
            sort_desc = False
            idx += 1
            continue

        if token.startswith("-"):
            raise ValueError("Unknown option: {!r}".format(token))
        positional.append(token)
        idx += 1

    if len(positional) >= 1:
        maybe_limit = _safe_int(positional[0])
        if maybe_limit is None:
            raise ValueError("limit must be an integer")
        limit = max(1, maybe_limit)
    if len(positional) >= 2:
        maybe_offset = _safe_int(positional[1])
        if maybe_offset is None:
            raise ValueError("offset must be an integer")
        offset = max(0, maybe_offset)
    if len(positional) > 2:
        raise ValueError("Usage: {}".format(usage))
    if min_files is not None and max_files is not None and min_files > max_files:
        raise ValueError("--min-files cannot be greater than --max-files.")

    return _StoreListOptions(
        limit=limit,
        offset=offset,
        kind_filters=kind_filters,
        status_filters=status_filters,
        protocol_filters=protocol_filters,
        name_filter=name_filter,
        read_only_filter=read_only_filter,
        min_files=min_files,
        max_files=max_files,
        sort_by=sort_by,
        sort_desc=sort_desc,
    )


def _parse_limit_offset(args: list[str], *, usage: str, default_limit: int) -> tuple[int, int]:
    limit = max(1, int(default_limit))
    offset = 0
    if len(args) >= 1:
        maybe_limit = _safe_int(args[0])
        if maybe_limit is None:
            raise ValueError("limit must be an integer")
        limit = max(1, maybe_limit)
    if len(args) >= 2:
        maybe_offset = _safe_int(args[1])
        if maybe_offset is None:
            raise ValueError("offset must be an integer")
        offset = max(0, maybe_offset)
    if len(args) > 2:
        raise ValueError("Usage: {}".format(usage))
    return limit, offset


def _row_value(row, column: str, default=""):
    try:
        value = row[column]
    except Exception:
        return default
    if value is None:
        return default
    return value


def _store_id(row) -> int:
    store_id = _row_value(row, "store_id", None)
    if store_id is None:
        raise ValueError("Invalid store row: missing store_id")
    return int(store_id)


def _store_name(row) -> str:
    return str(_row_value(row, "store_name", "")).strip()


def _resolve_store_row(browser, store_ref: str):
    if "stores" not in set(browser.catalog.get_tables()):
        raise ValueError("Database schema does not contain `stores` table.")

    store_id = _safe_int(store_ref)
    if store_id is not None:
        row = browser.catalog.get_row_from_id("stores", store_id)
        if row is None:
            raise ValueError("No store found for id {}.".format(store_id))
        return row

    rows = browser.catalog.search("stores", "store_name", str(store_ref))
    if not rows:
        raise ValueError("No store found for name {!r}.".format(store_ref))
    if len(rows) > 1:
        raise ValueError("Multiple stores found for name {!r}; use store id instead.".format(store_ref))
    return rows[0]


def _collect_store_file_counts(browser) -> dict[int, int]:
    tables = set(browser.catalog.get_tables())
    if "files" not in tables:
        return {}
    counts: dict[int, int] = {}
    for row in browser.catalog.get_all_rows("files", iterator_return=True):
        store_id = _row_value(row, "file_store_id", None)
        if store_id is None:
            continue
        try:
            key = int(store_id)
        except Exception:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def _store_file_rows(browser, store_id: int):
    tables = set(browser.catalog.get_tables())
    if "files" not in tables:
        return []
    rows = list(browser.catalog.search("files", "file_store_id", int(store_id)))
    rows.sort(
        key=lambda row: (
            int(_row_value(row, "file_id", 0) or 0),
            str(_row_value(row, "file_storage_key", "")),
        )
    )
    return rows


def _format_bool_flag(value) -> str:
    if value in {None, ""}:
        return ""
    try:
        return "yes" if int(value) else "no"
    except Exception:
        return "yes" if bool(value) else "no"


class StoreListCommand(TerminalCommandAPI):
    """List stores with useful summary fields."""

    group = "store"
    group_aliases = ("stores",)
    expose_direct = False
    name = "list"
    aliases = ("ls",)
    summary = "List stores with filters and sorting."
    usage = (
        "store list [limit] [offset] [--kind k1,k2] [--status s1,s2] [--protocol p1,p2] "
        "[--name text] [--read-only|--writable] [--min-files N] [--max-files N] "
        "[--sort id|name|kind|protocol|status|files[:asc|:desc]] [--asc|--desc]"
    )

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_store_list_options(args, usage=self.usage, default_limit=browser.page_size)

        class _StoreInfo:
            def __init__(self, row, files: int):
                self.row = row
                self.store_id = _store_id(row)
                self.name = _store_name(row)
                self.kind = str(_row_value(row, "store_kind", "")).strip()
                self.protocol = str(_row_value(row, "store_access_protocol", "")).strip()
                self.status = str(_row_value(row, "store_online_status", "")).strip()
                self.read_only_raw = _row_value(row, "store_is_read_only", "")
                self.read_only = _format_bool_flag(self.read_only_raw)
                self.files = int(files)

        def _coerce_bool(value) -> Optional[bool]:
            if value in {None, ""}:
                return None
            try:
                return bool(int(value))
            except Exception:
                text = str(value).strip().lower()
                if text in {"true", "yes", "y", "1"}:
                    return True
                if text in {"false", "no", "n", "0"}:
                    return False
                return None

        file_counts = _collect_store_file_counts(browser)
        all_infos = [
            _StoreInfo(row, file_counts.get(_store_id(row), 0))
            for row in browser.db.get_all_rows("stores", iterator_return=True)
        ]
        total = len(all_infos)

        filtered: list[_StoreInfo] = []
        for info in all_infos:
            if options.kind_filters is not None and info.kind.lower() not in options.kind_filters:
                continue
            if options.status_filters is not None and info.status.lower() not in options.status_filters:
                continue
            if options.protocol_filters is not None and info.protocol.lower() not in options.protocol_filters:
                continue
            if options.name_filter is not None and options.name_filter not in info.name.lower():
                continue

            read_only_value = _coerce_bool(info.read_only_raw)
            if options.read_only_filter is not None and read_only_value is not None:
                if read_only_value != options.read_only_filter:
                    continue
            if options.read_only_filter is not None and read_only_value is None:
                continue

            if options.min_files is not None and info.files < options.min_files:
                continue
            if options.max_files is not None and info.files > options.max_files:
                continue
            filtered.append(info)

        def _sort_key(info: _StoreInfo):
            if options.sort_by == "name":
                return (info.name.lower(), info.store_id)
            if options.sort_by == "kind":
                return (info.kind.lower(), info.store_id)
            if options.sort_by == "protocol":
                return (info.protocol.lower(), info.store_id)
            if options.sort_by == "status":
                return (info.status.lower(), info.store_id)
            if options.sort_by == "files":
                return (info.files, info.store_id)
            return (info.store_id,)

        filtered.sort(key=_sort_key, reverse=options.sort_desc)
        window = filtered[options.offset : options.offset + options.limit]

        total_text = "{}".format(len(filtered))
        if len(filtered) != total:
            total_text = "{} (filtered from {})".format(len(filtered), total)

        browser.emit(
            "Stores rows {}..{} of {}".format(
                options.offset + 1 if window else 0,
                options.offset + len(window),
                total_text,
            )
        )
        if not window:
            browser.emit("(no rows)")
            return True

        table_rows: list[list[object]] = []
        for info in window:
            browser.emit(
                "store id={} name={} kind={} status={} files={}".format(
                    info.store_id,
                    info.name,
                    info.kind,
                    info.status,
                    info.files,
                )
            )
            table_rows.append(
                [
                    info.store_id,
                    info.name,
                    info.kind,
                    info.protocol,
                    info.read_only,
                    info.status,
                    info.files,
                    str(_row_value(info.row, "store_root_uri", "")),
                ]
            )

        browser.emit(
            browser.render_table(
                ["id", "name", "kind", "protocol", "ro", "status", "files", "root_uri"],
                table_rows,
                max_cell_width=80,
            )
        )
        return True


class StoreShowCommand(TerminalCommandAPI):
    """Show detailed information for one store."""

    group = "store"
    group_aliases = ("stores",)
    expose_direct = False
    name = "show"
    aliases = ("info",)
    summary = "Show full details for one store."
    usage = "store show <store_id|store_name>"

    def execute(self, browser, args: list[str]) -> bool:
        if len(args) != 1:
            raise ValueError("Usage: {}".format(self.usage))

        row = _resolve_store_row(browser, args[0])
        store_id = _store_id(row)
        file_rows = _store_file_rows(browser, store_id)

        browser.emit("Store details")
        browser.emit("")
        browser.emit(browser.render_row_details("stores", row, max_cell_width=120))

        ext_counter: Counter[str] = Counter()
        for file_row in file_rows:
            ext = str(_row_value(file_row, "file_extension", "")).strip().lower()
            if ext:
                ext_counter[ext] += 1
        inventory_rows: list[tuple[str, object]] = [("files_total", len(file_rows))]
        if ext_counter:
            inventory_rows.append(
                ("top_extensions", ", ".join("{}:{}".format(ext, count) for ext, count in ext_counter.most_common(8)))
            )
        browser.emit("")
        browser.emit(browser.render_detail_sections([("Inventory", inventory_rows)], max_cell_width=120))
        return True


class StoreFilesCommand(TerminalCommandAPI):
    """List files belonging to one store."""

    group = "store"
    group_aliases = ("stores",)
    expose_direct = False
    name = "files"
    aliases = ("ls-files", "list-files")
    summary = "List files for one store."
    usage = "store files <store_id|store_name> [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        row = _resolve_store_row(browser, args[0])
        limit, offset = _parse_limit_offset(args[1:], usage=self.usage, default_limit=browser.page_size)
        store_id = _store_id(row)
        all_rows = _store_file_rows(browser, store_id)
        total = len(all_rows)
        window = all_rows[offset : offset + limit]

        browser.emit(
            "Store {} files rows {}..{} of {}".format(
                store_id,
                offset + 1 if window else 0,
                offset + len(window),
                total,
            )
        )
        if not window:
            browser.emit("(no rows)")
            return True

        table_rows: list[list[object]] = []
        for file_row in window:
            table_rows.append(
                [
                    _row_value(file_row, "file_id", ""),
                    _row_value(file_row, "file_storage_key", ""),
                    _row_value(file_row, "file_extension", ""),
                    _row_value(file_row, "file_size_bytes", ""),
                    _row_value(file_row, "file_integrity_status", ""),
                    _row_value(file_row, "file_hash_sha256", ""),
                ]
            )
        browser.emit(
            browser.render_table(
                ["id", "storage_key", "ext", "size_bytes", "integrity", "sha256"],
                table_rows,
                max_cell_width=80,
            )
        )
        return True


__all__ = [
    "StoreListCommand",
    "StoreShowCommand",
    "StoreFilesCommand",
]
