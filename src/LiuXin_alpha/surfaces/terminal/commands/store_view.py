"""
Inspect registered stores and their catalogued files through grouped terminal commands.

Listings compute inventory, filter, and sort in memory before applying display
windows. Counts describe database records, not a fresh filesystem or remote-store
scan. Helpers use the browser's database facade and do not mutate store contents.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> Optional[int]:
    """
    Parse stripped text as an integer, returning ``None`` for ordinary conversion failures.

    Example:
        >>> _safe_int(" 12 "), _safe_int("twelve")
        (12, None)


    :param value: Value stringified and stripped before integer conversion.
    :return: Parsed integer without range checks, or ``None`` if conversion raises an ``Exception``.
    """
    try:
        return int(str(value).strip())
    except Exception:
        return None


@dataclass(frozen=True)
class _StoreListOptions:
    """
    Hold the display window, metadata/count filters, and ordering for an in-memory store listing.

    Optional sets filter kind, status, and protocol by normalized tokens. Name is
    a substring filter; read-only is tri-state; file bounds are inclusive. Frozen
    fields do not make the optional sets immutable. Direct construction does not
    enforce the parser's limits or validate supported sort names.

    Example:
        >>> options = _parse_store_list_options(["5", "--sort=files:desc"], usage="store list", default_limit=20)
        >>> options.limit, options.sort_by, options.sort_desc
        (5, 'files', True)
    """

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


def _read_option_value(
    args: list[str], idx: int, *, option_name: str
) -> tuple[str, int]:
    """
    Consume a nonblank equals-form or following-token value and return the advanced cursor.

    Returned value text is not stripped. The helper does not validate the option
    spelling or reject an option-like token used as its separate value.

    Example:
        >>> _read_option_value(["--kind= local "], 0, option_name="--kind")
        (' local ', 1)


    :param args: Original option/value tokens, left unchanged.
    :param idx: Valid index of the option token; invalid indexing is not caught.
    :param option_name: Option spelling used only in diagnostic messages.
    :return: Value text and index of the first unconsumed token.
    :raises ValueError: If the value is missing or whitespace-only.
    """
    token = args[idx]
    if "=" in token:
        _, value = token.split("=", 1)
        if value.strip() == "":
            raise ValueError(
                "Option {} requires a non-blank value.".format(option_name)
            )
        return value, idx + 1
    if idx + 1 >= len(args):
        raise ValueError("Option {} requires a value.".format(option_name))
    value = args[idx + 1]
    if str(value).strip() == "":
        raise ValueError("Option {} requires a non-blank value.".format(option_name))
    return value, idx + 2


def _split_csv_tokens(raw: str) -> Optional[set[str]]:
    """
    Normalize comma/semicolon-separated filter values into a lowercase set.

    Blank pieces are discarded. Spaces inside a piece remain part of the token,
    and names are not checked against supported store metadata values.

    Example:
        >>> sorted(_split_csv_tokens("LOCAL; http,local"))
        ['http', 'local']


    :param raw: Filter-list text, stripped and lowercased before splitting.
    :return: Unique nonblank tokens, or ``None`` when none remain.
    """
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
    """
    Parse a supported store sort key and optional ascending/descending direction.

    Default direction is ascending. Direction aliases accept asc/a/up and desc/d/down.

    Example:
        >>> _parse_sort(" Files:DOWN ")
        ('files', True)


    :param raw: Key with optional colon-separated direction, matched case-insensitively.
    :return: Normalized key and Boolean indicating descending order.
    :raises ValueError: If blank or outside the supported id/name/kind/protocol/status/files grammar.
    """
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
            raise ValueError(
                "Unknown sort direction {!r}. Use asc or desc.".format(direction)
            )
    allowed = {"id", "name", "kind", "protocol", "status", "files"}
    if text not in allowed:
        raise ValueError(
            "Unknown sort key {!r}. Use one of: {}.".format(
                text, ", ".join(sorted(allowed))
            )
        )
    return text, sort_desc


def _parse_store_list_options(
    args: list[str], *, usage: str, default_limit: int
) -> _StoreListOptions:
    """
    Parse store filters and a display window without reading inventory.

    Limit clamps to one, offset and file bounds to zero. Dash-prefixed positionals
    are rejected as options before those clamps. Repeated filters replace earlier
    values; sort direction follows option order, and a later bare sort key resets
    it to ascending. Metadata filter tokens are not schema-validated.

    Example:
        >>> options = _parse_store_list_options(["0", "--kind=local,http", "--min-files=-2"], usage="store list", default_limit=20)
        >>> options.limit, options.min_files, sorted(options.kind_filters)
        (1, 0, ['http', 'local'])


    :param args: Up to two positional limit/offset values interspersed with supported filters/sort flags.
    :param usage: Usage text used when extra positional arguments remain.
    :param default_limit: Host page size, integer-converted and clamped when initializing the limit.
    :return: Parsed listing options with normalized filter text and ordering.
    :raises ValueError: For invalid options/values, excess positionals, or minimum files exceeding maximum.
    """
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
        if (
            token in {"--name", "--name-contains"}
            or token.startswith("--name=")
            or token.startswith("--name-contains=")
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


def _parse_limit_offset(
    args: list[str], *, usage: str, default_limit: int
) -> tuple[int, int]:
    """
    Parse at most two integer paging values, clamping limit to one and offset to zero.

    Unlike the store-list option parser, this helper accepts negative numeric
    tokens before clamping because it does not classify them as switches.

    Example:
        >>> _parse_limit_offset(["-5", "-2"], usage="store files", default_limit=20)
        (1, 0)


    :param args: Optional limit then offset tokens.
    :param usage: Usage text used when more than two tokens are supplied.
    :param default_limit: Limit used when omitted, integer-converted and clamped to one.
    :return: Positive limit and nonnegative offset.
    :raises ValueError: If a token is not an integer or too many tokens are supplied.
    """
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
    """
    Read a row field, substituting the caller's default for ``None`` or any lookup exception.

    Falsey values other than ``None`` are preserved. A missing value and a failed
    read are deliberately indistinguishable through this helper.

    Example:
        >>> _row_value({"count": 0}, "count", "?"), _row_value({}, "count", "?")
        (0, '?')


    :param row: Object supporting column-name subscription.
    :param column: Field key to read.
    :param default: Value returned for absence or lookup failure, without copying.
    :return: Retrieved non-``None`` value or the supplied default.
    """
    try:
        value = row[column]
    except Exception:
        return default
    if value is None:
        return default
    return value


def _store_id(row) -> int:
    """
    Require a store identity value and convert it to an integer without a positivity check.

    Example:
        >>> _store_id({"store_id": "12"})
        12


    :param row: Store-shaped object with mapping access to ``store_id``.
    :return: Integer store identity.
    :raises ValueError: If the ID is missing or its text cannot be converted to an integer.
    """
    store_id = _row_value(row, "store_id", None)
    if store_id is None:
        raise ValueError("Invalid store row: missing store_id")
    return int(store_id)


def _store_name(row) -> str:
    """
    Return stripped store-name text, using an empty string when lookup fails or yields ``None``.

    Example:
        >>> _store_name({"store_name": " Archive "})
        'Archive'


    :param row: Store-shaped object with mapping access to ``store_name``.
    :return: Display name text; string-conversion errors still propagate.
    """
    return str(_row_value(row, "store_name", "")).strip()


def _resolve_store_row(browser, store_ref: str):
    """
    Resolve an integer-looking reference as a store ID, otherwise require one exact name match.

    Numeric store names cannot be selected by name through this resolver. Names
    are stringified but not stripped before searching; duplicate name matches
    require the user to supply an ID. Read failures propagate.

    Example:
        >>> row = _resolve_store_row(browser, "12")  # doctest: +SKIP


    :param browser: Host exposing store-table enumeration, ID lookup, and name search.
    :param store_ref: Integer ID spelling or exact store name.
    :return: The uniquely resolved existing store row.
    :raises ValueError: If the stores table is absent, the reference is missing, or its name is ambiguous.
    """
    if "stores" not in set(browser.db.get_tables()):
        raise ValueError("Database schema does not contain `stores` table.")

    store_id = _safe_int(store_ref)
    if store_id is not None:
        row = browser.db.get_row_from_id("stores", store_id)
        if row is None:
            raise ValueError("No store found for id {}.".format(store_id))
        return row

    rows = browser.db.search("stores", "store_name", str(store_ref))
    if not rows:
        raise ValueError("No store found for name {!r}.".format(store_ref))
    if len(rows) > 1:
        raise ValueError(
            "Multiple stores found for name {!r}; use store id instead.".format(
                store_ref
            )
        )
    return rows[0]


def _collect_store_file_counts(browser) -> dict[int, int]:
    """
    Scan all file records and count entries with integer-convertible store references.

    A missing files table returns no counts. Missing, unreadable, and invalid
    per-row references are skipped, but table enumeration and iteration errors
    propagate. Counts do not inspect backing files or validate referenced stores.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.db.get_tables.return_value = ["files"]
        >>> host.db.get_all_rows.return_value = [{"file_store_id": "2"}, {"file_store_id": 2}, {}]
        >>> _collect_store_file_counts(host)
        {2: 2}


    :param browser: Host supplying table enumeration and iterable file-row access.
    :return: Store-ID-to-file-record-count mapping for usable references encountered in the scan.
    """
    tables = set(browser.db.get_tables())
    if "files" not in tables:
        return {}
    counts: dict[int, int] = {}
    for row in browser.db.get_all_rows("files", iterator_return=True):
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
    """
    Materialize one store's file records and sort by numeric file ID then storage-key text.

    A missing files table returns an empty list. Missing/falsey IDs sort as zero,
    but a nonnumeric nonempty ID can still raise during sorting. The full result
    is fetched before any caller applies a display window.

    Example:
        >>> rows = _store_file_rows(browser, 12)  # doctest: +SKIP


    :param browser: Host providing schema enumeration and file-store-reference search.
    :param store_id: Store ID integer-converted for the ``file_store_id`` search.
    :return: All matching file rows in deterministic ID/key order, or an empty list without a files table.
    """
    tables = set(browser.db.get_tables())
    if "files" not in tables:
        return []
    rows = list(browser.db.search("files", "file_store_id", int(store_id)))
    rows.sort(
        key=lambda row: (
            int(_row_value(row, "file_id", 0) or 0),
            str(_row_value(row, "file_storage_key", "")),
        )
    )
    return rows


def _format_bool_flag(value) -> str:
    """
    Render a read-only value as blank, yes, or no using integer conversion then Python truthiness.

    This is not the listing filter's textual-Boolean parser: nonempty ``"false"``
    displays as yes. The initial set-membership test can reject unhashable values.

    Example:
        >>> [_format_bool_flag(value) for value in (None, "0", "1", "false")]
        ['', 'no', 'yes', 'yes']


    :param value: Hashable flag value, with ``None``/empty text representing unknown display state.
    :return: Blank for unknown, otherwise yes/no from numeric or fallback truthiness.
    """
    if value in {None, ""}:
        return ""
    try:
        return "yes" if int(value) else "no"
    except Exception:
        return "yes" if bool(value) else "no"


class StoreListCommand(TerminalCommandAPI):
    """
    List registered stores with catalogued file counts, metadata filters, and stable key/ID sorting.

    The command reads all stores and scans file references before filtering and
    paging. It does not perform a bounded backend query or probe store availability.

    Example:
        >>> StoreListCommand().group, StoreListCommand().aliases
        ('store', ('ls',))
    """

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
        """
        Collect store/file facts, filter and sort in memory, then render the selected window.

        Read-only filters exclude unknown/unrecognized flags. Display formatting
        uses a separate truthiness rule and can disagree for textual flags.
        Descending sort reverses the ID tie-breaker as well as the primary key.
        Each shown store receives a text summary and a table row; an empty window
        returns after the range header and no-rows message.

        Example:
            >>> StoreListCommand().execute(browser, ["5", "--sort=files:desc"])  # doctest: +SKIP


        :param browser: Host providing page size, schema/row reads, and terminal output.
        :param args: Store-list window, filter, and sort options.
        :return: ``True`` after rendering, including an empty result window.
        """
        options = _parse_store_list_options(
            args, usage=self.usage, default_limit=browser.page_size
        )

        class _StoreInfo:
            """
            Cache one row's display/filter facts and its previously counted file total.

            The original row is retained for root-URI display. Derived strings and
            counters are snapshots, not live properties of the backing store.

            Example:
                >>> info = _StoreInfo({"store_id": 1, "store_name": "Archive"}, 3)  # doctest: +SKIP
            """

            def __init__(self, row, files: int):
                """
                Extract normalized display fields while retaining the raw read-only flag for filtering.

                Example:
                    >>> info = _StoreInfo(store_row, 3)  # doctest: +SKIP


                :param row: Store-shaped record providing identity and optional display fields.
                :param files: Catalogued file count, converted to an integer.
                :return: ``None`` after initializing the local listing snapshot.
                """
                self.row = row
                self.store_id = _store_id(row)
                self.name = _store_name(row)
                self.kind = str(_row_value(row, "store_kind", "")).strip()
                self.protocol = str(
                    _row_value(row, "store_access_protocol", "")
                ).strip()
                self.status = str(_row_value(row, "store_online_status", "")).strip()
                self.read_only_raw = _row_value(row, "store_is_read_only", "")
                self.read_only = _format_bool_flag(self.read_only_raw)
                self.files = int(files)

        def _coerce_bool(value) -> Optional[bool]:
            """
            Interpret a read-only filter value numerically or through recognized Boolean words.

            Unlike display formatting, unrecognized text becomes unknown. The
            initial membership test requires a hashable value.

            Example:
                >>> _coerce_bool("false")  # doctest: +SKIP
                False


            :param value: Hashable raw flag, with ``None``/empty text treated as unknown.
            :return: Parsed Boolean, or ``None`` for an absent or unrecognized value.
            """
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
            if (
                options.kind_filters is not None
                and info.kind.lower() not in options.kind_filters
            ):
                continue
            if (
                options.status_filters is not None
                and info.status.lower() not in options.status_filters
            ):
                continue
            if (
                options.protocol_filters is not None
                and info.protocol.lower() not in options.protocol_filters
            ):
                continue
            if (
                options.name_filter is not None
                and options.name_filter not in info.name.lower()
            ):
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
            """
            Build the selected metadata/count key with store ID as its deterministic tie-breaker.

            The enclosing options choose the key; reversal is applied by the
            caller rather than encoded here. Text keys are lowercased.

            Example:
                >>> key = _sort_key(info)  # doctest: +SKIP


            :param info: Local store snapshot whose sort fields and identity are read.
            :return: Primary-key/ID tuple, or an ID-only tuple for default ordering.
            """
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
    """
    Show one store's row details and a catalogued inventory summary with common extensions.

    Resolves by ID or unique exact name and fetches all file records before counting
    extensions. The inventory is not a live filesystem or remote-storage scan.

    Example:
        >>> StoreShowCommand().aliases
        ('info',)
    """

    group = "store"
    group_aliases = ("stores",)
    expose_direct = False
    name = "show"
    aliases = ("info",)
    summary = "Show full details for one store."
    usage = "store show <store_id|store_name>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve one store, render its metadata, and summarize total files and up to eight extensions.

        Extension keys are stripped/lowercased and blanks omitted; leading dots are
        retained. Equal-frequency entries follow their first occurrence in the
        ID/key-sorted file collection. Read/render failures propagate.

        Example:
            >>> StoreShowCommand().execute(browser, ["12"])  # doctest: +SKIP


        :param browser: Host providing store/file reads and row/detail-section output.
        :param args: Exactly one store ID or exact unique store name.
        :return: ``True`` after metadata and inventory rendering.
        :raises ValueError: If argument count or store resolution is invalid.
        """
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
                (
                    "top_extensions",
                    ", ".join(
                        "{}:{}".format(ext, count)
                        for ext, count in ext_counter.most_common(8)
                    ),
                )
            )
        browser.emit("")
        browser.emit(
            browser.render_detail_sections(
                [("Inventory", inventory_rows)], max_cell_width=120
            )
        )
        return True


class StoreFilesCommand(TerminalCommandAPI):
    """
    Show a window of catalogued file records for one store, ordered by file ID and storage key.

    All matching records are fetched and sorted before paging; the limit affects
    output rather than database work or filesystem access.

    Example:
        >>> StoreFilesCommand().aliases
        ('ls-files', 'list-files')
    """

    group = "store"
    group_aliases = ("stores",)
    expose_direct = False
    name = "files"
    aliases = ("ls-files", "list-files")
    summary = "List files for one store."
    usage = "store files <store_id|store_name> [limit] [offset]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve a store and render a file window with path, extension, size, integrity, and hash fields.

        Paging defaults to the host page size and offset zero; explicit negative
        values are clamped by the paging helper. Displayed file facts are taken
        from records without rechecking the backing files.

        Example:
            >>> StoreFilesCommand().execute(browser, ["12", "10", "20"])  # doctest: +SKIP


        :param browser: Host providing store/file reads, page-size defaults, and table output.
        :param args: Store reference followed by optional integer limit and offset.
        :return: ``True`` after rendering the window or an empty-result message.
        :raises ValueError: If the reference or paging syntax is invalid.
        """
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        row = _resolve_store_row(browser, args[0])
        limit, offset = _parse_limit_offset(
            args[1:], usage=self.usage, default_limit=browser.page_size
        )
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
