"""
Parse row references and expose relationship creation, removal, and inspection commands.

These adapters use the browser's database facade for relation operations. Parsing
does not establish a transaction, and multi-row deletion has no rollback here.
Inspection skips failed linked-table reads, so empty output is not proof of absence.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


def _safe_int(value: str) -> Optional[int]:
    """
    Parse stripped text as an integer, using ``None`` for blank or noninteger values.

    String conversion occurs outside the exception handler and can still raise.

    Example:
        >>> _safe_int(" 12 "), _safe_int(""), _safe_int("twelve")
        (12, None, None)


    :param value: Value converted to text before whitespace stripping and integer parsing.
    :return: Parsed integer without range validation, or ``None`` for blank/invalid integer text.
    """
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _resolve_table_token(browser, token: str) -> str:
    """
    Delegate table-name interpretation to the browser's public resolver.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.resolve_table.return_value = "works"
        >>> _resolve_table_token(host, "work")
        'works'


    :param browser: Host exposing ``resolve_table`` with its schema and alias policy.
    :param token: Table token forwarded unchanged.
    :return: Host-resolved table name; resolver errors propagate.
    """
    return browser.resolve_table(token)


def _split_row_ref(token: str):
    """
    Recognize a compact table/integer reference by splitting at its final colon.

    Both substrings retain spaces left after stripping the whole token. Integer
    validation does not convert the returned ID text or require a positive ID.

    Example:
        >>> _split_row_ref(" works : 12 "), _split_row_ref("works:many")
        (('works ', ' 12'), None)


    :param token: Candidate compact reference, converted to text and outer-stripped.
    :return: Table/ID text pair, or ``None`` for missing colon, blank table, or invalid integer ID.
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


def _consume_row_ref_tokens(args: list[str], start_idx: int):
    """
    Consume one compact reference or two separate table/ID tokens.

    Split-form IDs are left unvalidated except that ``to`` cannot occupy the ID
    position. The caller must supply a nonnegative cursor; negative indices are
    not rejected here.

    Example:
        >>> _consume_row_ref_tokens(["works:1", "to", "notes", "2"], 0)
        (1, 'works', '1')


    :param args: Reference and subsequent argument tokens, left unchanged.
    :param start_idx: Nonnegative index of the next reference's first token.
    :return: Next cursor position, table token, and ID token.
    :raises ValueError: If no reference or required split-form ID is available.
    """
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
    """
    Consume two mixed compact/split references with an optional ``to`` separator.

    Trailing tokens are returned for command-specific option parsing. Tables and
    split-form IDs are not resolved or validated by this helper.

    Example:
        >>> _parse_two_row_refs(["works:1", "to", "notes", "2", "--type=x"], usage="link")
        ('works', '1', 'notes', '2', ['--type=x'])


    :param args: Two row references, optional separator, and any trailing tokens.
    :param usage: Usage text included when fewer than two tokens are supplied.
    :return: Left table/ID, right table/ID, and a new list of remaining tokens.
    :raises ValueError: If either required reference cannot be consumed.
    """
    if len(args) < 2:
        raise ValueError("Usage: {}".format(usage))

    idx, left_table, left_id = _consume_row_ref_tokens(args, 0)
    if idx < len(args) and str(args[idx]).strip().lower() == "to":
        idx += 1
    idx, right_table, right_id = _consume_row_ref_tokens(args, idx)
    return left_table, left_id, right_table, right_id, args[idx:]


def _resolve_row_or_error(browser, table_token: str, id_token: str):
    """
    Resolve a table and retrieve one existing row by integer ID.

    Example:
        >>> table, row = _resolve_row_or_error(browser, "work", "1")  # doctest: +SKIP


    :param browser: Host supplying table resolution and database row lookup.
    :param table_token: Table spelling interpreted by the host's resolver.
    :param id_token: Integer ID text; positivity is not checked here.
    :return: Resolved table name and retrieved row object.
    :raises ValueError: If the table cannot resolve, the ID is invalid, or the row is absent.
    """
    table = _resolve_table_token(browser, table_token)
    row_id = _safe_int(id_token)
    if row_id is None:
        raise ValueError("Row id must be an integer: {!r}".format(id_token))
    row = browser.db.get_row_from_id(table, row_id)
    if row is None:
        raise ValueError("No row found in {} for id {}.".format(table, row_id))
    return table, row


def _parse_priority(value: str):
    """
    Normalize a symbolic relation priority or parse an unrestricted integer priority.

    Example:
        >>> _parse_priority(" HIGHEST "), _parse_priority("-2")
        ('highest', -2)


    :param value: Priority text, stripped and lowercased before interpretation.
    :return: Integer or one of ``highest``, ``lowest``, and ``not_set``.
    :raises ValueError: If neither an accepted symbol nor an integer is supplied.
    """
    text = str(value).strip().lower()
    if text in {"highest", "lowest", "not_set"}:
        return text
    parsed = _safe_int(text)
    if parsed is None:
        raise ValueError(
            "priority must be an integer or one of: highest, lowest, not_set"
        )
    return parsed


def _parse_scalar_value(value: str):
    """
    Interpret a relation-field token as null, integer Boolean, integer, decimal float, or text.

    Float parsing is attempted only when a dot is present; ``1e3`` therefore stays
    text. Unrecognized values retain their stripped spelling rather than failing.

    Example:
        >>> [_parse_scalar_value(value) for value in ("null", "YES", "2", "1.5", "1e3")]
        [None, 1, 2, 1.5, '1e3']


    :param value: Field-value token, with surrounding whitespace discarded.
    :return: Parsed scalar, or stripped text if no conversion applies.
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


def _parse_set_pair(raw: str) -> tuple[str, object]:
    """
    Split the first equals sign into a lowercase field key and a parsed scalar value.

    Empty values are allowed and become empty strings. Field authorization belongs
    to the calling option parser, not this helper.

    Example:
        >>> _parse_set_pair(" Enabled = yes "), _parse_set_pair("comment=a=b")
        (('enabled', 1), ('comment', 'a=b'))


    :param raw: One ``key=value`` token, outer-stripped before splitting.
    :return: Normalized key and scalar value.
    :raises ValueError: If the equals sign is absent or the key is blank.
    """
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
    """
    Parse priority, type, and permitted extra relation fields from trailing options.

    Each option accepts an equals or next-token form. Repetitions overwrite earlier
    values. Priority defaults to ``highest``, blank type becomes ``None``, and set
    field keys are lowercased before checking the supplied allowlist.

    Example:
        >>> _parse_link_options(["--priority=lowest", "--set=enabled=yes"], allowed_set_fields={"enabled"})
        ('lowest', None, {'enabled': 1})


    :param args: Trailing link-option tokens; option spellings are case-sensitive.
    :param allowed_set_fields: Exact permitted normalized suffixes for custom ``--set`` assignments.
    :return: Priority, optional type text, and dictionary of additional relation-field values.
    :raises ValueError: For unknown options, missing values, invalid priorities, or disallowed set fields.
    """
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
                allowed = (
                    ", ".join(sorted(allowed_set_fields))
                    if allowed_set_fields
                    else "<none>"
                )
                raise ValueError(
                    "Invalid `--set` field {!r}. Valid --set fields for this link are: {}".format(
                        key, allowed
                    )
                )
            custom_set_values[key] = value
            idx += 1
            continue
        if token == "--set":
            if idx + 1 >= len(args):
                raise ValueError("Missing value for --set (expected key=value)")
            key, value = _parse_set_pair(args[idx + 1])
            if key not in allowed_set_fields:
                allowed = (
                    ", ".join(sorted(allowed_set_fields))
                    if allowed_set_fields
                    else "<none>"
                )
                raise ValueError(
                    "Invalid `--set` field {!r}. Valid --set fields for this link are: {}".format(
                        key, allowed
                    )
                )
            custom_set_values[key] = value
            idx += 2
            continue
        raise ValueError(
            "Unknown option: {!r}. Supported options: --priority, --type, --set key=value".format(
                token
            )
        )
    return priority, link_type, custom_set_values


class LinkCommand(TerminalCommandAPI):
    """
    Link two existing rows with optional priority, type, and schema-permitted extra fields.

    An existing relation is reported without updating it, regardless of requested
    option values. Compact and split references may be mixed around an optional ``to``.

    Example:
        >>> LinkCommand().name
        'link'
    """

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
        """
        Resolve both endpoints, validate options against the link schema, and create a missing relation.

        Reserved identity/priority/type suffixes cannot be assigned through ``--set``.
        Option parsing precedes the existing-link check; type-column validation
        follows it. Creation uses ``browser.db.interlink_rows`` and output follows
        the write, with no rollback if rendering fails.

        Example:
            >>> LinkCommand().execute(browser, ["works:1", "tags:2", "--priority=highest"])  # doctest: +SKIP


        :param browser: Host supplying schema metadata, row/relation access, and output.
        :param args: Two row references and optional priority/type/extra-field options.
        :return: ``True`` after creating a relation or reporting an existing one.
        :raises ValueError: If references/options are invalid or no suitable relation schema exists.
        """
        (
            left_table_token,
            left_id_token,
            right_table_token,
            right_id_token,
            option_tokens,
        ) = _parse_two_row_refs(
            args,
            usage="{} OR link <table_a>:<id_a> to <table_b>:<id_b> [options]".format(
                self.usage
            ),
        )

        left_table, left_row = _resolve_row_or_error(
            browser, left_table_token, left_id_token
        )
        right_table, right_row = _resolve_row_or_error(
            browser, right_table_token, right_id_token
        )

        link_table = browser.db.driver_wrapper.get_link_table_name(
            left_table, right_table
        )
        if not link_table:
            raise ValueError(
                "No link table exists between {} and {}.".format(
                    left_table, right_table
                )
            )

        link_base = browser.db.driver_wrapper.get_column_base(link_table)
        prefix = link_base + "_"
        link_columns = set(browser.db.get_column_headings(link_table))
        suffixes = {
            col[len(prefix) :] for col in link_columns if col.startswith(prefix)
        }

        left_id_column = browser.db.driver_wrapper.get_id_column(left_table)
        right_id_column = browser.db.driver_wrapper.get_id_column(right_table)
        reserved_suffixes = {
            "id",
            str(left_id_column),
            str(right_id_column),
            "priority",
            "type",
        }
        allowed_set_fields = {
            suffix for suffix in suffixes if suffix not in reserved_suffixes
        }

        priority, link_type, custom_set_values = _parse_link_options(
            option_tokens,
            allowed_set_fields=allowed_set_fields,
        )

        existing_rows = browser.db.get_interlink_row(
            primary_row=left_row, secondary_row=right_row, onelink=False
        )
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
                browser.db.driver_wrapper.get_link_column(
                    left_table, right_table, "type"
                )
            except Exception:
                raise ValueError(
                    "Link table {} has no `type` column; omit --type for this table pair.".format(
                        link_table
                    )
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
    """
    Remove all currently returned interlink rows for two existing endpoints.

    Endpoint rows are retained. Deletions are performed individually and are not
    rolled back by this command if a later deletion or output operation fails.

    Example:
        >>> UnlinkCommand().usage
        'unlink <table_a> <id_a> <table_b> <id_b>'
    """

    name = "unlink"
    aliases = ()
    summary = "Unlink two rows: unlink <table_a> <id_a> <table_b> <id_b>"
    usage = "unlink <table_a> <id_a> <table_b> <id_b>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Validate two references, find their relation rows, and delete each through the database facade.

        No trailing options are accepted. An absent relation produces a message
        without deleting anything; schema/read/delete failures otherwise propagate.

        Example:
            >>> UnlinkCommand().execute(browser, ["works:1", "tags:2"])  # doctest: +SKIP


        :param browser: Host providing endpoint lookup, relation metadata, deletion, and output.
        :param args: Exactly two compact/split row references, optionally separated by ``to``.
        :return: ``True`` after deletion or an already-unlinked message.
        :raises ValueError: If references are invalid, trailing tokens remain, or no link table exists.
        """
        left_table_token, left_id_token, right_table_token, right_id_token, trailing = (
            _parse_two_row_refs(
                args,
                usage="{} OR unlink <table_a>:<id_a> <table_b>:<id_b>".format(
                    self.usage
                ),
            )
        )
        if trailing:
            raise ValueError("Usage: {}".format(self.usage))

        left_table, left_row = _resolve_row_or_error(
            browser, left_table_token, left_id_token
        )
        right_table, right_row = _resolve_row_or_error(
            browser, right_table_token, right_id_token
        )

        link_table = browser.db.driver_wrapper.get_link_table_name(
            left_table, right_table
        )
        if not link_table:
            raise ValueError(
                "No link table exists between {} and {}.".format(
                    left_table, right_table
                )
            )

        existing_rows = browser.db.get_interlink_row(
            primary_row=left_row, secondary_row=right_row, onelink=False
        )
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
    """
    Display rows related to one source, optionally restricted to a second table.

    All returned rows are fetched before page-size display truncation. Same-table
    relations are skipped, and linked-row read failures are silently omitted.

    Example:
        >>> LinksCommand().usage
        'links <table> <id> [other_table]'
    """

    name = "links"
    aliases = ()
    summary = "Show linked rows: links <table> <id> [other_table]"
    usage = "links <table> <id> [other_table]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve the source and candidate tables, then print nonempty readable linked-row groups.

        Without a filter, candidate tables are sorted. Each group retains database
        row order and uses the host page size for display only. If every group is
        empty, skipped, or failed, the final message says no linked rows were found;
        it does not establish successful reads from all candidates.

        Example:
            >>> LinksCommand().execute(browser, ["works:1", "tags"])  # doctest: +SKIP


        :param browser: Host supplying schema/relation reads, row formatting, and output.
        :param args: One compact/split source reference followed by at most one table filter.
        :return: ``True`` after displaying groups or the no-results message.
        :raises ValueError: If the source/filter cannot resolve or too many trailing tokens remain.
        """
        if not args:
            raise ValueError("Usage: {}".format(self.usage))

        idx, source_table_token, source_id_token = _consume_row_ref_tokens(args, 0)
        source_table, source_row = _resolve_row_or_error(
            browser, source_table_token, source_id_token
        )

        trailing = args[idx:]
        if len(trailing) > 1:
            raise ValueError("Usage: {}".format(self.usage))

        if trailing:
            candidate_tables = [_resolve_table_token(browser, trailing[0])]
        else:
            candidate_tables = sorted(
                browser.db.driver_wrapper.get_interlinked_tables(source_table)
            )

        shown_any = False
        for table in candidate_tables:
            if table == source_table:
                continue
            try:
                rows = browser.db.get_interlinked_rows(
                    target_row=source_row, secondary_table=table
                )
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
            browser.emit(
                "No linked rows found for {} id {}.".format(
                    source_table, source_row.row_id
                )
            )
        return True
