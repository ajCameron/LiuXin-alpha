"""
Attach notes, tags, genres, subjects, languages, and series to selected existing rows.

Selectors are expanded and all targets are read before mutation. Source records
may be reused or created through Core. Bulk recovery is compensating cleanup, not
a transaction: only tracked changes can be reversed, and source-resolution errors
occur outside the per-link recovery handler. Languages are lookup-only.
"""

from __future__ import annotations

from typing import Any, Optional

from LiuXin_alpha.surfaces.metadata_facets import (
    build_tag_row_payload,
    preferred_tag_table,
    search_tag_rows,
)
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.commands.link import _resolve_table_token
from LiuXin_alpha.metadata.standardization import (
    make_series_phash,
    make_tag_search_term,
    make_title_search_term,
    standardize_genre,
    standardize_language,
    standardize_series,
)
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort


MAX_SELECTOR_TARGETS = 2000
MAX_SELECTOR_RANGE_SPAN = 2000


def _safe_int(value: str):
    """
    Convert selector text with ``int``, representing ordinary conversion errors as ``None``.

    Example:
        >>> _safe_int("2"), _safe_int("two")
        (2, None)


    :param value: Value passed directly to integer conversion.
    :return: Integer without range validation, or ``None`` when conversion raises an ``Exception``.
    """
    try:
        return int(value)
    except Exception:
        return None


def _expand_id_selector(
    selector: str,
    *,
    max_targets: Optional[int] = None,
    max_range_span: Optional[int] = None,
) -> list[int]:
    """
    Expand comma-separated IDs and inclusive ascending ranges, preserving first-encounter order.

    Repeated IDs are removed. Each range's full span is checked before deduplication,
    while the overall limit counts unique IDs. Defaults come from the two module
    limits, currently 2,000 each. Zero and plus-prefixed integers are accepted;
    leading minus signs are treated as range separators rather than negative IDs.

    Example:
        >>> _expand_id_selector("3,1-3,5")
        [3, 1, 2, 5]


    :param selector: Nonblank comma-separated integer or ``start-end`` segments.
    :param max_targets: Maximum unique IDs, or ``None`` for the module default.
    :param max_range_span: Maximum inclusive size of one range, or ``None`` for the module default.
    :return: Expanded unique integer IDs in encounter order.
    :raises ValueError: For blank/malformed segments, descending ranges, or exceeded limits.
    """
    if max_targets is None:
        max_targets = MAX_SELECTOR_TARGETS
    if max_range_span is None:
        max_range_span = MAX_SELECTOR_RANGE_SPAN

    text = str(selector).strip()
    if not text:
        raise ValueError("Row id selector cannot be blank.")

    values: list[int] = []
    seen: set[int] = set()
    for raw_part in text.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError(
                "Invalid id selector {!r}: empty segment.".format(selector)
            )

        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            start = _safe_int(start_raw.strip())
            end = _safe_int(end_raw.strip())
            if start is None or end is None:
                raise ValueError(
                    "Invalid id range {!r} in selector {!r}.".format(part, selector)
                )
            if start > end:
                raise ValueError(
                    "Invalid id range {!r} in selector {!r}: range start must be <= range end.".format(
                        part, selector
                    )
                )
            span = end - start + 1
            if span > max_range_span:
                raise ValueError(
                    "Id range {!r} in selector {!r} is too large ({} > max {}).".format(
                        part, selector, span, max_range_span
                    )
                )
            candidate_ids = range(start, end + 1)
        else:
            single = _safe_int(part)
            if single is None:
                raise ValueError("Row id must be an integer, got {!r}.".format(part))
            candidate_ids = (single,)

        for value in candidate_ids:
            if value in seen:
                continue
            seen.add(value)
            values.append(value)
            if len(values) > max_targets:
                raise ValueError(
                    "Id selector {!r} expands to too many ids ({} > max {}).".format(
                        selector, len(values), max_targets
                    )
                )

    if not values:
        raise ValueError("Row id selector cannot be blank.")
    return values


def _parse_target_rows(browser, args: list[str], *, usage: str):
    """
    Consume a table/selector reference and load every selected target before returning any.

    Compact syntax splits at the last colon; split syntax consumes two tokens.
    Missing IDs are collected for one error after all lookups. The selector limits
    apply before reads, and following option/value tokens are left to the caller.

    Example:
        >>> table, targets, consumed = _parse_target_rows(  # doctest: +SKIP
        ...     browser, ["works:1,3-5", "history"], usage="on tag <target> <value>"
        ... )


    :param browser: Host supplying table resolution and row lookup.
    :param args: Compact ``table:selector`` or separate table/selector followed by command values.
    :param usage: Usage text for incomplete target references.
    :return: Resolved table, ordered ID/row pairs, and count of consumed reference tokens.
    :raises ValueError: If syntax/selector/table resolution fails or any selected row is missing.
    """
    if not args:
        raise ValueError("Usage: {}".format(usage))

    compact_token = str(args[0]).strip()
    if ":" in compact_token:
        target_table_token, selector = compact_token.rsplit(":", 1)
        if not target_table_token.strip():
            raise ValueError("Usage: {}".format(usage))
        target_table = _resolve_table_token(browser, target_table_token)
        target_ids = _expand_id_selector(selector)
        consumed = 1
    else:
        if len(args) < 2:
            raise ValueError("Usage: {}".format(usage))
        target_table = _resolve_table_token(browser, args[0])
        target_ids = _expand_id_selector(args[1])
        consumed = 2

    target_rows: list[tuple[int, object]] = []
    missing_ids: list[int] = []
    for target_id in target_ids:
        target_row = browser.db.get_row_from_id(target_table, target_id)
        if target_row is None:
            missing_ids.append(target_id)
        else:
            target_rows.append((target_id, target_row))

    if missing_ids:
        if len(missing_ids) == 1:
            raise ValueError(
                "No row found in {} for id {}.".format(target_table, missing_ids[0])
            )
        missing_text = ", ".join(str(v) for v in missing_ids)
        raise ValueError(
            "No rows found in {} for ids {}.".format(target_table, missing_text)
        )

    return target_table, target_rows, consumed


def _create_row(browser, table: str, values: dict[str, object]):
    """
    Create one row through Core's administrative endpoint and adapt its returned record.

    Record adaptation can fail after the write; this helper does not undo creation.

    Example:
        >>> row = _create_row(browser, "notes", {"note": "Check edition"})  # doctest: +SKIP


    :param browser: Host providing Core command dispatch and model record adaptation.
    :param table: Table name forwarded in the create payload without local validation.
    :param values: Field mapping forwarded unchanged as the row's values.
    :return: Row wrapper produced by ``browser.model.row_from_record``.
    :raises RuntimeError: If Core returns no non-``None`` record field.
    """
    result = browser.execute_core_command(
        "admin.row.create",
        payload={"table": table, "values": values},
    )
    record = (result or {}).get("record")
    if record is None:
        raise RuntimeError("Core did not return the created row.")
    return browser.model.row_from_record(record)


def _resolve_or_create_note_row(browser, note_text: str, *, create: bool):
    """
    Reuse the first exact note-text match, optionally creating a note when none exists.

    No trimming or normalization is performed here. Schema/search/create errors
    propagate rather than being interpreted as a missing note.

    Example:
        >>> resolved = _resolve_or_create_note_row(browser, "Check edition", create=False)  # doctest: +SKIP


    :param browser: Host exposing note-table reads and Core row creation.
    :param note_text: Exact value searched in the ``note`` column and used for creation.
    :param create: Whether a missing match may be inserted.
    :return: ``("notes", row)`` for a match/new row, or ``None`` for a lookup-only miss.
    :raises ValueError: If the notes table is absent.
    """
    tables = set(browser.db.get_tables())
    if "notes" not in tables:
        raise ValueError("Database schema does not contain `notes` table.")
    rows = browser.db.search("notes", "note", note_text)
    if rows:
        return "notes", rows[0]
    if not create:
        return None
    row = _create_row(browser, "notes", {"note": note_text})
    return "notes", row


def _resolve_or_create_tag_row(browser, tag_text: str, *, create: bool):
    """
    Reuse or create a schema-compatible tag-like row, preferring tags over labels.

    Search and creation fields come from the shared facet helpers. The first
    matching row is reused; normalized matching may be broader than literal equality.

    Example:
        >>> resolved = _resolve_or_create_tag_row(browser, "history", create=False)  # doctest: +SKIP


    :param browser: Host supplying table/column/search access and Core row creation.
    :param tag_text: Display text passed to facet search and optional creation payload construction.
    :param create: Whether a lookup miss may create a row in the selected tag-like table.
    :return: Selected table and matched/created row, or ``None`` for a lookup-only miss.
    :raises ValueError: If neither tags nor labels exists or the selected schema cannot form a payload.
    """
    table = preferred_tag_table(browser.db)
    if table is None:
        raise ValueError("Database schema has neither `labels` nor `tags` table.")

    rows = search_tag_rows(browser.db, table, tag_text)
    if rows:
        return table, rows[0]
    if not create:
        return None

    columns = set(browser.db.get_column_headings(table))
    row_dict = build_tag_row_payload(table, columns, tag_text)
    row = _create_row(browser, table, row_dict)
    return table, row


def _resolve_or_create_genre_row(browser, genre_text: str, *, create: bool):
    """
    Match a genre using its available hash/sort/text column, optionally creating a row.

    Search chooses exactly one column: ``genre_phash``, then ``genre_sort``, then
    raw ``genre``. A miss does not retry less-normalized columns. New rows retain
    the original genre text and add standardized fields supported by the schema.

    Example:
        >>> resolved = _resolve_or_create_genre_row(browser, "Science fiction", create=False)  # doctest: +SKIP


    :param browser: Host providing schema/search reads and Core row creation.
    :param genre_text: Original genre text from which sort and search-hash values are derived.
    :param create: Whether a lookup miss may create a genre row.
    :return: ``("genres", row)`` for a match/new row, or ``None`` when lookup-only finds none.
    :raises ValueError: If the genres table is absent.
    """
    tables = set(browser.db.get_tables())
    if "genres" not in tables:
        raise ValueError("Database schema does not contain `genres` table.")
    columns = set(browser.db.get_column_headings("genres"))

    genre_sort = standardize_genre(genre_text)
    genre_phash = make_title_search_term(genre_sort)
    if "genre_phash" in columns:
        search_column = "genre_phash"
        search_value = genre_phash
    elif "genre_sort" in columns:
        search_column = "genre_sort"
        search_value = genre_sort
    else:
        search_column = "genre"
        search_value = genre_text

    rows = browser.db.search("genres", search_column, search_value)
    if rows:
        return "genres", rows[0]
    if not create:
        return None

    row_dict: dict[str, object] = {"genre": genre_text}
    if "genre_sort" in columns:
        row_dict["genre_sort"] = genre_sort
    if "genre_phash" in columns:
        row_dict["genre_phash"] = genre_phash
    row = _create_row(browser, "genres", row_dict)
    return "genres", row


def _resolve_or_create_subject_row(browser, subject_text: str, *, create: bool):
    """
    Match a subject through its available sort/hash/text column, optionally inserting it.

    Search uses ``subject_sort`` before ``subject_phash`` before raw ``subject``;
    only the chosen column is queried. Sort and hash both use the title-search
    normalization, while a new row retains original display text.

    Example:
        >>> resolved = _resolve_or_create_subject_row(browser, "Astronomy", create=False)  # doctest: +SKIP


    :param browser: Host supplying schema/search access and Core row creation.
    :param subject_text: Display text and normalization input for the subject.
    :param create: Whether a missing match may be inserted.
    :return: ``("subjects", row)`` for a match/new row, or ``None`` on a lookup-only miss.
    :raises ValueError: If the subjects table is absent.
    """
    tables = set(browser.db.get_tables())
    if "subjects" not in tables:
        raise ValueError("Database schema does not contain `subjects` table.")
    columns = set(browser.db.get_column_headings("subjects"))

    subject_sort = make_title_search_term(subject_text)
    subject_phash = subject_sort
    if "subject_sort" in columns:
        search_column = "subject_sort"
        search_value = subject_sort
    elif "subject_phash" in columns:
        search_column = "subject_phash"
        search_value = subject_phash
    else:
        search_column = "subject"
        search_value = subject_text

    rows = browser.db.search("subjects", search_column, search_value)
    if rows:
        return "subjects", rows[0]
    if not create:
        return None

    row_dict: dict[str, object] = {"subject": subject_text}
    if "subject_sort" in columns:
        row_dict["subject_sort"] = subject_sort
    if "subject_phash" in columns:
        row_dict["subject_phash"] = subject_phash
    row = _create_row(browser, "subjects", row_dict)
    return "subjects", row


def _resolve_language_row(browser, language_text: str, *, create: bool):
    """
    Find an existing language using ordered spelling variants and known name/code columns.

    Raw, lower/title-case, and standardized spellings are tried in encounter order.
    Each column-search exception is treated as no match, allowing missing legacy
    columns but also hiding read failures. This function never creates languages:
    ``create=True`` changes an unresolved result into an error instead.

    Example:
        >>> resolved = _resolve_language_row(browser, "en", create=False)  # doctest: +SKIP


    :param browser: Host supplying language-table enumeration and searches.
    :param language_text: Nonblank language name or code to strip and standardize.
    :param create: Whether an unresolved language must raise instead of returning ``None``.
    :return: ``("languages", first_matching_row)`` or ``None`` for an allowed miss.
    :raises ValueError: If the table/input is absent or an unresolved language is required.
    """
    tables = set(browser.db.get_tables())
    if "languages" not in tables:
        raise ValueError("Database schema does not contain `languages` table.")

    text = str(language_text).strip()
    if not text:
        raise ValueError("Language cannot be blank.")

    normalized = standardize_language(text)
    candidates = []
    for value in (
        text,
        text.lower(),
        text.title(),
        normalized,
        str(normalized).lower(),
        str(normalized).title(),
    ):
        v = str(value).strip()
        if v and v not in candidates:
            candidates.append(v)

    search_columns = [
        "language_code",
        "language_iso639_1",
        "language_iso639_2_b",
        "language_iso639_2_t",
        "language_bcp47_primary",
        "language",
    ]
    for candidate in candidates:
        for column in search_columns:
            try:
                rows = browser.db.search("languages", column, candidate)
            except Exception:
                rows = []
            if rows:
                return "languages", rows[0]

    if not create:
        return None
    raise ValueError(
        "Unknown language {!r}. Use an existing language name or ISO code (languages table is read-only).".format(
            language_text
        )
    )


def _resolve_or_create_series_row(browser, series_text: str, *, create: bool):
    """
    Try available series hash/name/sort lookups in order, then optionally create normalized fields.

    Search order is hash, normalized name, sort key, and standardized display name.
    Unlike genre/subject matching, later columns are tried after a miss. Search
    exceptions also become misses, so creation can follow failed reads.

    Example:
        >>> resolved = _resolve_or_create_series_row(browser, "Voyages", create=False)  # doctest: +SKIP


    :param browser: Host supplying series schema/search reads and Core row creation.
    :param series_text: Name standardized before matching and creation.
    :param create: Whether exhaustion of the search order may insert a series row.
    :return: ``("series", row)`` for a match/new row, or ``None`` after a lookup-only miss.
    :raises ValueError: If the series table is absent.
    """
    tables = set(browser.db.get_tables())
    if "series" not in tables:
        raise ValueError("Database schema does not contain `series` table.")

    columns = set(browser.db.get_column_headings("series"))
    series_name = standardize_series(series_text)
    series_sort = generate_title_sort(series_name)
    series_name_norm = make_title_search_term(series_name)
    series_phash = make_series_phash("", series_name)

    search_order: list[tuple[str, str]] = []
    if "series_phash" in columns:
        search_order.append(("series_phash", series_phash))
    if "series_name_norm" in columns:
        search_order.append(("series_name_norm", series_name_norm))
    if "series_sort" in columns:
        search_order.append(("series_sort", series_sort))
    search_order.append(("series", series_name))

    for column, value in search_order:
        try:
            rows = browser.db.search("series", column, value)
        except Exception:
            rows = []
        if rows:
            return "series", rows[0]
    if not create:
        return None

    row_dict: dict[str, object] = {"series": series_name}
    if "series_sort" in columns:
        row_dict["series_sort"] = series_sort
    if "series_name_norm" in columns:
        row_dict["series_name_norm"] = series_name_norm
    if "series_phash" in columns:
        row_dict["series_phash"] = series_phash
    row = _create_row(browser, "series", row_dict)
    return "series", row


def _resolve_source_row(browser, kind: str, value: str, *, create: bool):
    """
    Dispatch a supported metadata kind to its resolver and add the display kind label.

    Kind names are exact singular tokens, not case-normalized aliases. Language
    resolution remains lookup-only even when creation is requested.

    Example:
        >>> source = _resolve_source_row(browser, "tag", "history", create=False)  # doctest: +SKIP


    :param browser: Host passed to the selected metadata resolver.
    :param kind: One of note, tag, genre, subject, language, or series.
    :param value: Kind-specific text interpreted by the selected resolver.
    :param create: Permit creation where supported, or require a known language.
    :return: Table/row/kind-label triple, or ``None`` when the selected resolver allows a miss.
    :raises ValueError: If the kind is unsupported or its resolver rejects the value/schema.
    """
    if kind == "note":
        resolved = _resolve_or_create_note_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("note",)
        return None
    if kind == "tag":
        resolved = _resolve_or_create_tag_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("tag",)
        return None
    if kind == "genre":
        resolved = _resolve_or_create_genre_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("genre",)
        return None
    if kind == "subject":
        resolved = _resolve_or_create_subject_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("subject",)
        return None
    if kind == "language":
        resolved = _resolve_language_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("language",)
        return None
    if kind == "series":
        resolved = _resolve_or_create_series_row(browser, value, create=create)
        if resolved is not None:
            return resolved + ("series",)
        return None
    raise ValueError("Unsupported `on` kind: {!r}".format(kind))


def _parse_tag_values(raw_values: list[str]) -> list[str]:
    """
    Split comma-separated tag tokens and retain the first spelling of each normalized value.

    Blank pieces are ignored. Deduplication uses tag-search normalization rather
    than literal text equality; whitespace within a nonblank piece is retained.

    Example:
        >>> _parse_tag_values(["history,history", "science"])
        ['history', 'science']


    :param raw_values: Tokens containing one or more comma-separated tag values each.
    :return: Stripped nonblank display values in first-encounter order.
    :raises ValueError: If no nonblank tag pieces remain.
    """
    values: list[str] = []
    seen_norm: set[str] = set()
    for raw in raw_values:
        for piece in str(raw).split(","):
            value = piece.strip()
            if not value:
                continue
            norm = make_tag_search_term(value)
            if norm in seen_norm:
                continue
            seen_norm.add(norm)
            values.append(value)
    if not values:
        raise ValueError("Tag value cannot be blank.")
    return values


def _parse_on_options_and_value_tokens(raw_tokens: list[str]) -> tuple[bool, list[str]]:
    """
    Consume only leading ``--best-effort`` switches and return the remaining value tokens.

    Repeated switches are harmless. The first other token stops parsing; unknown
    options and later switches are treated as value text rather than rejected.

    Example:
        >>> _parse_on_options_and_value_tokens(["--best-effort", "history", "--best-effort"])
        (True, ['history', '--best-effort'])


    :param raw_tokens: Tokens after the target reference, with options allowed only at the beginning.
    :return: Whether any leading best-effort switch occurred and a new list of remaining tokens.
    """
    best_effort = False
    idx = 0
    while idx < len(raw_tokens):
        token = str(raw_tokens[idx]).strip()
        if token == "--best-effort":
            best_effort = True
            idx += 1
            continue
        break
    return best_effort, raw_tokens[idx:]


def _rollback_on_bulk_changes(
    browser,
    *,
    created_link_rows: list[object],
    created_source_rows: list[object],
) -> list[str]:
    """
    Attempt reverse-order deletion of tracked new links and then apparently unreferenced sources.

    Link deletion errors are collected. Sources are retained when a non-self
    relationship lookup reports rows, but failed linked-row reads are treated as
    empty and same-table links are ignored. This is not a safe-reference proof.
    Metadata-enumeration errors can escape and interrupt cleanup. No transaction
    protects either the original changes or these compensating deletions.

    Example:
        >>> from unittest.mock import Mock
        >>> _rollback_on_bulk_changes(Mock(), created_link_rows=[], created_source_rows=[])
        []


    :param browser: Host supplying Core row deletion and source relationship inspection.
    :param created_link_rows: Recorded new link rows, processed in reverse order.
    :param created_source_rows: Recorded new metadata rows considered for deletion after link cleanup.
    :return: Collected deletion-error messages; an empty list does not prove all original effects were undone.
    """
    errors: list[str] = []

    for link_row in reversed(created_link_rows):
        try:
            browser.execute_core_command(
                "admin.row.delete",
                payload={
                    "table": str(link_row.table),
                    "row_id": int(link_row.row_id),
                },
            )
        except Exception as exc:
            errors.append(
                "link rollback failed for {}:{} ({})".format(
                    link_row.table, link_row.row_id, exc
                )
            )

    for source_row in reversed(created_source_rows):
        has_links = False
        for table in browser.db.driver_wrapper.get_interlinked_tables(source_row.table):
            if table == source_row.table:
                continue
            try:
                linked_rows = browser.db.get_interlinked_rows(
                    primary_row=source_row, secondary_table=table
                )
            except Exception:
                linked_rows = []
            if linked_rows:
                has_links = True
                break
        if has_links:
            continue
        try:
            browser.execute_core_command(
                "admin.row.delete",
                payload={
                    "table": str(source_row.table),
                    "row_id": int(source_row.row_id),
                },
            )
        except Exception as exc:
            errors.append(
                "source rollback failed for {}:{} ({})".format(
                    source_row.table, source_row.row_id, exc
                )
            )

    return errors


def _link_one_value(
    browser,
    *,
    target_table: str,
    target_row,
    target_id: int,
    source_table: str,
    source_row,
    kind_label: str,
) -> tuple[bool, Optional[object]]:
    """
    Link one source/target pair unless a relation already exists, then find a row for cleanup tracking.

    A successful Core link may still return no discoverable link row. The write
    precedes both lookup and output, so later failure can leave an untracked effect.
    The pre-check is not an atomic uniqueness guarantee.

    Example:
        >>> created, link_row = _link_one_value(  # doctest: +SKIP
        ...     browser, target_table="works", target_row=work, target_id=1,
        ...     source_table="tags", source_row=tag, kind_label="tag"
        ... )


    :param browser: Host supplying schema/relation reads, Core relation creation, and output.
    :param target_table: Target table for schema lookup, command payload, and messages.
    :param target_row: Existing target row whose ``row_id`` identifies the relation endpoint.
    :param target_id: Target ID used in messages, not substituted for the row object's ID.
    :param source_table: Metadata source table for lookup and command dispatch.
    :param source_row: Existing source row supplying its schema ID value and ``row_id``.
    :param kind_label: Metadata kind label used in user-facing messages.
    :return: ``(False, None)`` for an existing relation, otherwise true plus a possibly missing link row.
    :raises ValueError: If no relation table exists for the source/target pair.
    """
    source_id_column = browser.db.driver_wrapper.get_id_column(source_table)
    source_id = source_row[source_id_column]

    link_table = browser.db.driver_wrapper.get_link_table_name(
        source_table, target_table
    )
    if not link_table:
        raise ValueError(
            "No link table exists between {} and {} for `{}`.".format(
                source_table,
                target_table,
                kind_label,
            )
        )

    existing_link = browser.db.get_interlink_row(
        primary_row=source_row, secondary_row=target_row, onelink=False
    )
    if existing_link:
        browser.emit(
            "{} already linked: {}={} -> {}:{}".format(
                kind_label.capitalize(),
                source_id_column,
                source_id,
                target_table,
                target_id,
            )
        )
        return False, None

    browser.execute_core_command(
        "admin.relation.link",
        payload={
            "table": source_table,
            "row_id": int(source_row.row_id),
            "related_table": target_table,
            "related_row_id": int(target_row.row_id),
        },
    )
    link_row = _find_interlink_row(
        browser,
        source_row=source_row,
        target_row=target_row,
    )
    browser.emit(
        "{} linked: {}={} -> {}:{}".format(
            kind_label.capitalize(),
            source_id_column,
            source_id,
            target_table,
            target_id,
        )
    )
    return True, link_row


def _find_interlink_row(browser, *, source_row, target_row) -> Optional[object]:
    """
    Search both endpoint directions for the first available interlink row, swallowing lookup errors.

    List results contribute their first element; empty lists allow the reverse
    lookup. A miss is indistinguishable from failed reads in both directions.

    Example:
        >>> from unittest.mock import Mock
        >>> host = Mock()
        >>> host.db.get_interlink_row.side_effect = [[], ["relation-row"]]
        >>> _find_interlink_row(host, source_row=object(), target_row=object())
        'relation-row'


    :param browser: Host exposing database interlink-row lookup.
    :param source_row: First endpoint, tried as primary before reversing the pair.
    :param target_row: Second endpoint, tried as secondary before reversing the pair.
    :return: First row found, or ``None`` when neither direction yields a record.
    """
    for primary_row, secondary_row in (
        (source_row, target_row),
        (target_row, source_row),
    ):
        try:
            link_row = browser.db.get_interlink_row(
                primary_row=primary_row,
                secondary_row=secondary_row,
                onelink=False,
            )
        except Exception:
            link_row = None
        if isinstance(link_row, list):
            if link_row:
                return link_row[0]
            continue
        if link_row is not None:
            return link_row
    return None


class _OnBaseCommand(TerminalCommandAPI):
    """
    Share target expansion, source reuse/creation, and per-link recovery for metadata attachment.

    Subclasses select ``kind`` and publish command metadata. Default error handling
    attempts compensating cleanup; best-effort mode keeps successful changes and
    reports per-link failures. Neither mode guarantees atomic bulk attachment.

    Example:
        >>> OnTagCommand().group, OnTagCommand().kind
        ('on', 'tag')
    """

    group = "on"
    expose_direct = False
    kind = ""
    usage = ""

    def execute(self, browser, args: list[str]) -> bool:
        """
        Resolve all targets and attach each requested metadata value, tracking available new rows.

        Tags split/deduplicate multiple values; other kinds join tokens into one
        nonblank value. Source resolution and creation occur outside the per-link
        handler, so failure there bypasses compensation for earlier values. Link
        failures either continue in best-effort mode or trigger attempted cleanup
        followed by ``ValueError``. Missing tracking rows and cleanup errors can
        leave effects behind despite the rollback wording of the emitted message.

        Example:
            >>> OnTagCommand().execute(browser, ["works:1-3", "--best-effort", "history"])  # doctest: +SKIP


        :param browser: Host providing row/schema reads, Core mutations, and terminal output.
        :param args: Target selector, optional leading best-effort switches, and metadata value tokens.
        :return: ``True`` after completion, including completion with reported best-effort errors.
        :raises ValueError: For invalid targets/values/kinds or a per-link failure in default mode.
        """
        target_table, target_rows, consumed = _parse_target_rows(
            browser, args, usage=self.usage
        )
        best_effort, value_tokens = _parse_on_options_and_value_tokens(args[consumed:])

        if self.kind == "tag":
            values = _parse_tag_values(value_tokens)
        else:
            value = " ".join(value_tokens).strip()
            if not value:
                raise ValueError("Value cannot be blank.")
            values = [value]

        created_link_rows: list[object] = []
        created_source_rows: list[object] = []
        seen_created_sources: set[tuple[str, int]] = set()
        errors: list[str] = []

        for value in values:
            resolved_existing = _resolve_source_row(
                browser, self.kind, value, create=False
            )
            if resolved_existing is not None:
                resolved = resolved_existing
                created_source = False
            else:
                resolved = _resolve_source_row(browser, self.kind, value, create=True)
                created_source = True
            if resolved is None:
                # Defensive only: create=True should always either resolve or raise.
                raise ValueError(
                    "Unable to resolve source row for {}={!r}.".format(self.kind, value)
                )
            source_table, source_row, kind_label = resolved
            if created_source:
                source_id_col = browser.db.driver_wrapper.get_id_column(source_table)
                source_id = int(source_row[source_id_col])
                source_key = (source_table, source_id)
                if source_key not in seen_created_sources:
                    seen_created_sources.add(source_key)
                    created_source_rows.append(source_row)

            for target_id, target_row in target_rows:
                try:
                    created_link, link_row = _link_one_value(
                        browser,
                        target_table=target_table,
                        target_row=target_row,
                        target_id=target_id,
                        source_table=source_table,
                        source_row=source_row,
                        kind_label=kind_label,
                    )
                    if created_link and link_row is not None:
                        created_link_rows.append(link_row)
                except Exception as exc:
                    op_desc = "{}={!r} -> {}:{}".format(
                        self.kind, value, target_table, target_id
                    )
                    if best_effort:
                        browser.emit(
                            "ERROR (best-effort): {} ({})".format(op_desc, exc)
                        )
                        errors.append("{} ({})".format(op_desc, exc))
                        continue

                    rollback_errors = _rollback_on_bulk_changes(
                        browser,
                        created_link_rows=created_link_rows,
                        created_source_rows=created_source_rows,
                    )
                    if rollback_errors:
                        browser.emit(
                            "Rollback encountered {} issue(s):".format(
                                len(rollback_errors)
                            )
                        )
                        for rollback_error in rollback_errors:
                            browser.emit("  - {}".format(rollback_error))
                    raise ValueError(
                        "Bulk `on` aborted on {} and rolled back {} link(s).".format(
                            op_desc,
                            len(created_link_rows),
                        )
                    ) from exc

        if errors:
            browser.emit("Completed with {} best-effort error(s).".format(len(errors)))
        return True


class OnNoteCommand(_OnBaseCommand):
    """
    Attach one exact note-text value to every selected target, creating a note if needed.

    Unlike ``note-on``, resolution reuses the first exact match without first
    checking every matching note for a link to a particular target.

    Example:
        >>> OnNoteCommand().kind, OnNoteCommand().aliases
        ('note', ('notes',))
    """

    name = "note"
    aliases = ("notes",)
    summary = (
        "Attach note(s): on note <table> <id|selector> [--best-effort] <note text>"
    )
    usage = "on note <table> <id|id,id|start-end> [--best-effort] <note text>"
    kind = "note"


class OnTagCommand(_OnBaseCommand):
    """
    Attach normalized, deduplicated tag values to selected rows using the preferred tag-like table.

    Values can be separate tokens or comma-separated pieces. Tags are preferred
    over labels when both tables exist; matching/creation uses the shared schema helpers.

    Example:
        >>> OnTagCommand().aliases
        ('tags', 'label', 'labels')
    """

    name = "tag"
    aliases = ("tags", "label", "labels")
    summary = "Attach tag(s): on tag <table> <id|selector> [--best-effort] <tag...>"
    usage = "on tag <table> <id|id,id|start-end> [--best-effort] <tag...>"
    kind = "tag"


class OnGenreCommand(_OnBaseCommand):
    """
    Attach one genre value to selected rows using the genre resolver's available normalization column.

    A missing genre may be created before linking; shared bulk cleanup limitations apply.

    Example:
        >>> OnGenreCommand().kind
        'genre'
    """

    name = "genre"
    aliases = ("genres",)
    summary = "Attach genre: on genre <table> <id|selector> [--best-effort] <genre>"
    usage = "on genre <table> <id|id,id|start-end> [--best-effort] <genre>"
    kind = "genre"


class OnSubjectCommand(_OnBaseCommand):
    """
    Attach one subject value to selected rows, reusing or creating through the subject resolver.

    Remaining value tokens form a single space-joined subject, not multiple subjects.

    Example:
        >>> OnSubjectCommand().kind
        'subject'
    """

    name = "subject"
    aliases = ("subjects",)
    summary = (
        "Attach subject: on subject <table> <id|selector> [--best-effort] <subject>"
    )
    usage = "on subject <table> <id|id,id|start-end> [--best-effort] <subject>"
    kind = "subject"


class OnLanguageCommand(_OnBaseCommand):
    """
    Attach a known language name/code to selected rows without creating language records.

    Unknown languages are rejected by the lookup-only language resolver.

    Example:
        >>> OnLanguageCommand().aliases
        ('languages', 'lang')
    """

    name = "language"
    aliases = ("languages", "lang")
    summary = "Attach language: on language <table> <id|selector> [--best-effort] <language|code>"
    usage = "on language <table> <id|id,id|start-end> [--best-effort] <language|code>"
    kind = "language"


class OnSeriesCommand(_OnBaseCommand):
    """
    Attach one standardized series name to selected rows, creating the series on a lookup miss.

    The resolver tries available hash/name/sort columns before creation; this
    command does not expose per-target series-index options.

    Example:
        >>> OnSeriesCommand().kind
        'series'
    """

    name = "series"
    aliases = ()
    summary = "Attach series: on series <table> <id|selector> [--best-effort] <series>"
    usage = "on series <table> <id|id,id|start-end> [--best-effort] <series>"
    kind = "series"
