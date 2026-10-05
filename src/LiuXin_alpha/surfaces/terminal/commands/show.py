"""
Render metadata linked to one target row, with specialized tag text and generic row displays.

Kind-specific commands require a readable relation; ``show all`` skips failed
linked-table reads and self-table relations. Page size restricts output, not the
amount of relation data fetched. Bulk target selectors are not supported.
"""

from __future__ import annotations

from LiuXin_alpha.surfaces.metadata_facets import (
    resolve_tag_or_label_table_token,
    tag_row_text,
)
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.commands.link import _split_row_ref


def _safe_int(value: str):
    """
    Convert an ID token with ``int``, returning ``None`` for ordinary conversion errors.

    Example:
        >>> _safe_int("12"), _safe_int("1,2")
        (12, None)


    :param value: Value passed directly to integer conversion without positivity validation.
    :return: Parsed integer or ``None`` when conversion raises an ``Exception``.
    """
    try:
        return int(value)
    except Exception:
        return None


def _looks_like_id_selector(token: str) -> bool:
    """
    Recognize comma/range-shaped integer syntax for a more specific single-row error message.

    This does not expand IDs, enforce limits, or check range ordering. A leading
    minus sign is interpreted as a range separator, so a lone negative ID fails
    this heuristic even though the command's integer parser can accept it.

    Example:
        >>> _looks_like_id_selector("3-1,7"), _looks_like_id_selector("-2")
        (True, False)


    :param token: Candidate selector text, outer-stripped before comma/range splitting.
    :return: Whether each nonblank segment has integer or two-endpoint range syntax.
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
            if _safe_int(left.strip()) is None or _safe_int(right.strip()) is None:
                return False
        else:
            if _safe_int(part) is None:
                return False
    return True


def resolve_show_kind_table(browser, token: str) -> str:
    """
    Resolve metadata-kind aliases, then fall back to the browser's general table resolver.

    Tag/label aliases prefer their named table when available. Common kind aliases
    precede general resolution. All exceptions from that final resolver become an
    unknown-kind ``ValueError``; table enumeration errors propagate separately.
    ``all`` is a separate command, not a special result of this helper.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.db.get_tables.return_value = ["tags", "labels"]
        >>> resolve_show_kind_table(host, "label")
        'labels'


    :param browser: Host exposing table enumeration and general table resolution.
    :param token: Nonblank kind/table token, stripped and lowercased.
    :return: Concrete linked-table name selected by alias or host resolution.
    :raises ValueError: If the token is blank or general resolution fails.
    """
    text = str(token).strip().lower()
    if not text:
        raise ValueError("Linked kind/table cannot be blank.")

    tables = set(browser.db.get_tables())

    tag_or_label_table = resolve_tag_or_label_table_token(text, tables)
    if tag_or_label_table is not None:
        return tag_or_label_table

    aliases = {
        "note": "notes",
        "notes": "notes",
        "genre": "genres",
        "genres": "genres",
        "subject": "subjects",
        "subjects": "subjects",
        "language": "languages",
        "languages": "languages",
        "lang": "languages",
        "series": "series",
    }
    alias_target = aliases.get(text)
    if alias_target in tables:
        return alias_target

    try:
        return browser.resolve_table(text)
    except Exception:
        raise ValueError(
            "Unknown linked kind/table {!r}. Try: tags, notes, genres, subjects, language, series, or all.".format(
                token
            )
        )


def _parse_target_row(browser, args: list[str], *, usage: str):
    """
    Resolve and load exactly one target from compact or split table/ID syntax.

    Recognizable lists/ranges receive an explicit unsupported-selector error.
    Integer IDs are not required to be positive here. Reads do not alter browser
    selection or paging state.

    Example:
        >>> table, row_id, row = _parse_target_row(browser, ["works:1"], usage="show tags <target>")  # doctest: +SKIP


    :param browser: Host providing table resolution and database row lookup.
    :param args: Exactly one compact reference or exactly two table/ID tokens.
    :param usage: Usage text used when token counts or compact syntax are unsuitable.
    :return: Resolved table name, integer ID, and existing target row.
    :raises ValueError: If syntax, single-ID requirement, resolution, or target existence checks fail.
    """
    if not args:
        raise ValueError("Usage: {}".format(usage))

    if len(args) == 1:
        compact_candidate = str(args[0]).strip()
        if ":" in compact_candidate:
            table_token, id_token = compact_candidate.rsplit(":", 1)
            if (
                table_token.strip()
                and _looks_like_id_selector(id_token)
                and _safe_int(id_token) is None
            ):
                raise ValueError(
                    "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
                )

    compact = _split_row_ref(args[0])
    if compact is not None:
        if len(args) != 1:
            raise ValueError("Usage: {}".format(usage))
        table_token, id_token = compact
    else:
        if (
            len(args) == 2
            and _looks_like_id_selector(args[1])
            and _safe_int(args[1]) is None
        ):
            raise ValueError(
                "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
            )
        if len(args) != 2:
            raise ValueError("Usage: {}".format(usage))
        table_token, id_token = args[0], args[1]

    target_table = browser.resolve_table(table_token)
    target_id = _safe_int(id_token)
    if target_id is None:
        if _looks_like_id_selector(id_token):
            raise ValueError(
                "`show` supports a single row id only. Selectors like `1,2,3` or `10-20` are not supported."
            )
        raise ValueError("Row id must be an integer.")

    target_row = browser.db.get_row_from_id(target_table, target_id)
    if target_row is None:
        raise ValueError(
            "No row found in {} for id {}.".format(target_table, target_id)
        )

    return target_table, target_id, target_row


def _get_linked_rows(browser, *, target_table: str, target_row, linked_table: str):
    """
    Require a relation table and fetch rows linked to the supplied target.

    The database receives the target as its primary row. No output limit or
    fallback is applied; schema and relation-read failures propagate.

    Example:
        >>> rows = _get_linked_rows(browser, target_table="works", target_row=work, linked_table="tags")  # doctest: +SKIP


    :param browser: Host exposing relation-table metadata and interlinked-row lookup.
    :param target_table: Target table used to validate the relation schema.
    :param target_row: Existing row passed as the primary relation endpoint.
    :param linked_table: Table from which related rows should be fetched.
    :return: Database-provided linked-row collection, including an empty collection for no matches.
    :raises ValueError: If no relation table exists for the target/linked table pair.
    """
    link_table = browser.db.driver_wrapper.get_link_table_name(
        linked_table, target_table
    )
    if not link_table:
        raise ValueError(
            "No link table exists between {} and {}.".format(linked_table, target_table)
        )
    return browser.db.get_interlinked_rows(
        primary_row=target_row, secondary_table=linked_table
    )


def _render_default_rows(
    browser, *, target_table: str, target_id: int, linked_table: str, rows
):
    """
    Print a relation-group count and page-size prefix of formatted rows, or a no-links message.

    Display preserves input order and uses the host's page size without clamping.
    The supplied collection has already been fetched; truncation only affects output.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock(page_size=20)
        >>> _render_default_rows(host, target_table="works", target_id=1, linked_table="notes", rows=[])
        >>> host.emit.assert_called_once_with("No linked notes for works:1.")


    :param browser: Host providing page size, single-row formatting, and text output.
    :param target_table: Target table name used in the group heading.
    :param target_id: Target identifier used in messages.
    :param linked_table: Related table name used for headings and row formatting.
    :param rows: Sized, sliceable collection of related rows in display order.
    :return: ``None`` after emitting the group or no-links message.
    """
    if not rows:
        browser.emit(
            "No linked {} for {}:{}.".format(linked_table, target_table, target_id)
        )
        return
    browser.emit(
        "Linked {} for {}:{}: {}".format(
            linked_table, target_table, target_id, len(rows)
        )
    )
    for row in rows[: browser.page_size]:
        browser.emit("  {}".format(browser.format_row(linked_table, row)))
    if len(rows) > browser.page_size:
        browser.emit("  ... {} more".format(len(rows) - browser.page_size))


class _ShowLinkedBaseCommand(TerminalCommandAPI):
    """
    Share single-target resolution and relation fetching for kind-specific show commands.

    Subclasses select ``linked_table_token`` and may override row presentation.
    Missing schemas or failed reads propagate instead of being skipped as in ``show all``.

    Example:
        >>> ShowNotesCommand().group, ShowNotesCommand().linked_table_token
        ('show', 'notes')
    """

    group = "show"
    expose_direct = False
    linked_table_token = ""

    def execute(self, browser, args: list[str]) -> bool:
        """
        Load one target, resolve this command's linked table, and pass all related rows to its renderer.

        Example:
            >>> ShowNotesCommand().execute(browser, ["works:1"])  # doctest: +SKIP


        :param browser: Host supplying target/schema/relation reads and rendering operations.
        :param args: Single target in compact or split table/ID form.
        :return: ``True`` after rendering, including an empty relation collection.
        """
        target_table, target_id, target_row = _parse_target_row(
            browser, args, usage=self.usage
        )
        linked_table = resolve_show_kind_table(browser, self.linked_table_token)
        rows = _get_linked_rows(
            browser,
            target_table=target_table,
            target_row=target_row,
            linked_table=linked_table,
        )
        self.render_rows(
            browser,
            target_table=target_table,
            target_id=target_id,
            linked_table=linked_table,
            rows=rows,
        )
        return True

    def render_rows(
        self, browser, *, target_table: str, target_id: int, linked_table: str, rows
    ) -> None:
        """
        Delegate related-row display to the common count-and-row-preview renderer.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock(page_size=20)
            >>> ShowNotesCommand().render_rows(host, target_table="works", target_id=1, linked_table="notes", rows=[])
            >>> host.emit.assert_called_once_with("No linked notes for works:1.")


        :param browser: Host providing page size, row formatting, and output.
        :param target_table: Target table label for the heading.
        :param target_id: Target identifier for the heading.
        :param linked_table: Related table used for the heading and each row formatter call.
        :param rows: Complete sized/sliceable related-row collection to preview.
        :return: ``None`` after the common renderer finishes.
        """
        _render_default_rows(
            browser,
            target_table=target_table,
            target_id=target_id,
            linked_table=linked_table,
            rows=rows,
        )


class ShowTagsCommand(_ShowLinkedBaseCommand):
    """
    Show readable tag-like text linked to one row, with a generic row fallback when none is available.

    All command aliases use the ``tags`` table token, so tags win when both tags and
    labels exist. Display deduplicates by stripped lowercase text, not search hashes.

    Example:
        >>> ShowTagsCommand().aliases
        ('tag', 'labels', 'label')
    """

    name = "tags"
    aliases = ("tag", "labels", "label")
    summary = "Show tags: show tags <table:id>"
    usage = "show tags <table:id> OR show tags <table> <id>"
    linked_table_token = "tags"

    def render_rows(
        self, browser, *, target_table: str, target_id: int, linked_table: str, rows
    ) -> None:
        """
        Extract nonblank tag/label text, deduplicate it, and display a page-size prefix.

        Empty input produces a no-tags message. If no row has readable text, use
        the generic renderer; if some do, textless rows are omitted. First spelling
        and input order survive deduplication. The entire collection is inspected
        before applying the output limit.

        Example:
            >>> from unittest.mock import Mock
            >>> host = Mock(page_size=20)
            >>> ShowTagsCommand().render_rows(
            ...     host, target_table="works", target_id=1, linked_table="tags",
            ...     rows=[{"tag": "History"}, {"tag": "history"}]
            ... )
            >>> [call.args[0] for call in host.emit.call_args_list]
            ['Tags for works:1 (1)', '  - History']


        :param browser: Host providing page size, output, and generic formatting when needed.
        :param target_table: Target table name included in headings.
        :param target_id: Target identifier included in headings.
        :param linked_table: Actual related table passed to the generic fallback.
        :param rows: Complete related-row collection with tag/label-shaped mapping access.
        :return: ``None`` after text, fallback rows, or an absence message is emitted.
        """
        if not rows:
            browser.emit("No linked tags for {}:{}.".format(target_table, target_id))
            return

        labels: list[str] = []
        for row in rows:
            text = tag_row_text(row)
            if text:
                labels.append(text)

        if not labels:
            _render_default_rows(
                browser,
                target_table=target_table,
                target_id=target_id,
                linked_table=linked_table,
                rows=rows,
            )
            return

        unique_labels: list[str] = []
        seen: set[str] = set()
        for label in labels:
            key = label.strip().lower()
            if key in seen:
                continue
            seen.add(key)
            unique_labels.append(label)

        browser.emit(
            "Tags for {}:{} ({})".format(target_table, target_id, len(unique_labels))
        )
        for label in unique_labels[: browser.page_size]:
            browser.emit("  - {}".format(label))
        if len(unique_labels) > browser.page_size:
            browser.emit("  ... {} more".format(len(unique_labels) - browser.page_size))


class ShowNotesCommand(_ShowLinkedBaseCommand):
    """
    Show a page-size preview of note rows linked to a single target.

    Uses generic row formatting rather than the tag-specific text display.

    Example:
        >>> ShowNotesCommand().linked_table_token
        'notes'
    """

    name = "notes"
    aliases = ("note",)
    summary = "Show notes: show notes <table:id>"
    usage = "show notes <table:id> OR show notes <table> <id>"
    linked_table_token = "notes"


class ShowGenresCommand(_ShowLinkedBaseCommand):
    """
    Show a page-size preview of genre rows linked to a single target.

    Relation retrieval precedes output truncation and propagates read failures.

    Example:
        >>> ShowGenresCommand().linked_table_token
        'genres'
    """

    name = "genres"
    aliases = ("genre",)
    summary = "Show genres: show genres <table:id>"
    usage = "show genres <table:id> OR show genres <table> <id>"
    linked_table_token = "genres"


class ShowSubjectsCommand(_ShowLinkedBaseCommand):
    """
    Show linked subject rows for one target using the shared row-preview renderer.

    The command accepts one integer row ID, not the bulk selectors used by attachment commands.

    Example:
        >>> ShowSubjectsCommand().linked_table_token
        'subjects'
    """

    name = "subjects"
    aliases = ("subject",)
    summary = "Show subjects: show subjects <table:id>"
    usage = "show subjects <table:id> OR show subjects <table> <id>"
    linked_table_token = "subjects"


class ShowLanguageCommand(_ShowLinkedBaseCommand):
    """
    Show existing language relations for one target through language/languages/lang aliases.

    Records are read and formatted without modifying the language table or relations.

    Example:
        >>> ShowLanguageCommand().aliases
        ('languages', 'lang')
    """

    name = "language"
    aliases = ("languages", "lang")
    summary = "Show language(s): show language <table:id>"
    usage = "show language <table:id> OR show language <table> <id>"
    linked_table_token = "languages"


class ShowSeriesCommand(_ShowLinkedBaseCommand):
    """
    Show a page-size preview of series records linked to one target row.

    This displays related records through generic formatting, not a specialized
    per-link series-index report.

    Example:
        >>> ShowSeriesCommand().linked_table_token
        'series'
    """

    name = "series"
    aliases = ()
    summary = "Show series: show series <table:id>"
    usage = "show series <table:id> OR show series <table> <id>"
    linked_table_token = "series"


class ShowAllCommand(TerminalCommandAPI):
    """
    Inspect non-self linked tables for one target, showing each nonempty readable group.

    Despite its name, ``all`` does not guarantee complete output: each table has
    a page-size display limit, and failing relation reads are silently skipped.

    Example:
        >>> ShowAllCommand().aliases
        ('*',)
    """

    group = "show"
    expose_direct = False
    name = "all"
    aliases = ("*",)
    summary = "Show all links: show all <table:id>"
    usage = "show all <table:id> OR show all <table> <id>"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Visit sorted related tables, skip failed/empty reads, and render each remaining group.

        The target's own table is omitted. A no-linked-rows message also occurs
        when every candidate read failed, so it is not verified absence. Initial
        target/schema discovery and rendering exceptions still propagate.

        Example:
            >>> ShowAllCommand().execute(browser, ["works:1"])  # doctest: +SKIP


        :param browser: Host supplying target/schema/relation reads and row-preview output.
        :param args: One compact or split target table/ID reference.
        :return: ``True`` after displaying groups or the no-linked-rows message.
        """
        target_table, target_id, target_row = _parse_target_row(
            browser, args, usage=self.usage
        )
        candidate_tables = sorted(
            browser.db.driver_wrapper.get_interlinked_tables(target_table)
        )
        shown_any = False
        for linked_table in candidate_tables:
            if linked_table == target_table:
                continue
            try:
                rows = browser.db.get_interlinked_rows(
                    target_row=target_row, secondary_table=linked_table
                )
            except Exception:
                continue
            if not rows:
                continue
            shown_any = True
            _render_default_rows(
                browser,
                target_table=target_table,
                target_id=target_id,
                linked_table=linked_table,
                rows=rows,
            )
        if not shown_any:
            browser.emit("No linked rows for {}:{}.".format(target_table, target_id))
        return True
