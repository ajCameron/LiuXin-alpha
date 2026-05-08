"""`on` command group for attaching metadata to existing rows."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.databases.row import Row
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
    """Expand an id selector like `1`, `1,2,3`, `10-20`, or mixed forms."""
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
            raise ValueError("Invalid id selector {!r}: empty segment.".format(selector))

        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            start = _safe_int(start_raw.strip())
            end = _safe_int(end_raw.strip())
            if start is None or end is None:
                raise ValueError("Invalid id range {!r} in selector {!r}.".format(part, selector))
            if start > end:
                raise ValueError(
                    "Invalid id range {!r} in selector {!r}: range start must be <= range end.".format(part, selector)
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

    target_rows: list[tuple[int, Row]] = []
    missing_ids: list[int] = []
    for target_id in target_ids:
        target_row = browser.db.get_row_from_id(target_table, target_id)
        if target_row is None:
            missing_ids.append(target_id)
        else:
            target_rows.append((target_id, target_row))

    if missing_ids:
        if len(missing_ids) == 1:
            raise ValueError("No row found in {} for id {}.".format(target_table, missing_ids[0]))
        missing_text = ", ".join(str(v) for v in missing_ids)
        raise ValueError("No rows found in {} for ids {}.".format(target_table, missing_text))

    return target_table, target_rows, consumed


def _resolve_or_create_note_row(browser, note_text: str, *, create: bool):
    tables = set(browser.db.get_tables())
    if "notes" not in tables:
        raise ValueError("Database schema does not contain `notes` table.")
    rows = browser.db.search("notes", "note", note_text)
    if rows:
        return "notes", rows[0]
    if not create:
        return None
    row = Row.from_idless_row_dict(
        browser.db,
        row_dict={"note": note_text},
        table="notes",
    )
    return "notes", row


def _resolve_or_create_tag_row(browser, tag_text: str, *, create: bool):
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
    row = Row.from_idless_row_dict(browser.db, row_dict=row_dict, table=table)
    return table, row


def _resolve_or_create_genre_row(browser, genre_text: str, *, create: bool):
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
    row = Row.from_idless_row_dict(browser.db, row_dict=row_dict, table="genres")
    return "genres", row


def _resolve_or_create_subject_row(browser, subject_text: str, *, create: bool):
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
    row = Row.from_idless_row_dict(browser.db, row_dict=row_dict, table="subjects")
    return "subjects", row


def _resolve_language_row(browser, language_text: str, *, create: bool):
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
    row = Row.from_idless_row_dict(browser.db, row_dict=row_dict, table="series")
    return "series", row


def _resolve_source_row(browser, kind: str, value: str, *, create: bool):
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
    """Parse command options and return (best_effort, remaining value tokens)."""
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
    created_link_rows: list[Row],
    created_source_rows: list[Row],
) -> list[str]:
    errors: list[str] = []

    for link_row in reversed(created_link_rows):
        try:
            browser.db.delete(link_row)
        except Exception as exc:
            errors.append("link rollback failed for {}:{} ({})".format(link_row.table, link_row.row_id, exc))

    for source_row in reversed(created_source_rows):
        has_links = False
        for table in browser.db.driver_wrapper.get_interlinked_tables(source_row.table):
            if table == source_row.table:
                continue
            try:
                linked_rows = browser.db.get_interlinked_rows(primary_row=source_row, secondary_table=table)
            except Exception:
                linked_rows = []
            if linked_rows:
                has_links = True
                break
        if has_links:
            continue
        try:
            browser.db.delete(source_row)
        except Exception as exc:
            errors.append("source rollback failed for {}:{} ({})".format(source_row.table, source_row.row_id, exc))

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
) -> tuple[bool, Optional[Row]]:
    source_id_column = browser.db.driver_wrapper.get_id_column(source_table)
    source_id = source_row[source_id_column]

    link_table = browser.db.driver_wrapper.get_link_table_name(source_table, target_table)
    if not link_table:
        raise ValueError(
            "No link table exists between {} and {} for `{}`.".format(
                source_table,
                target_table,
                kind_label,
            )
        )

    existing_link = browser.db.get_interlink_row(primary_row=source_row, secondary_row=target_row, onelink=False)
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

    link_row = browser.db.interlink_rows(primary_row=source_row, secondary_row=target_row)
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


class _OnBaseCommand(TerminalCommandAPI):
    """Common execution logic for `on <kind> ...` subcommands."""

    group = "on"
    expose_direct = False
    kind = ""
    usage = ""

    def execute(self, browser, args: list[str]) -> bool:
        target_table, target_rows, consumed = _parse_target_rows(browser, args, usage=self.usage)
        best_effort, value_tokens = _parse_on_options_and_value_tokens(args[consumed:])

        if self.kind == "tag":
            values = _parse_tag_values(value_tokens)
        else:
            value = " ".join(value_tokens).strip()
            if not value:
                raise ValueError("Value cannot be blank.")
            values = [value]

        created_link_rows: list[Row] = []
        created_source_rows: list[Row] = []
        seen_created_sources: set[tuple[str, int]] = set()
        errors: list[str] = []

        for value in values:
            resolved_existing = _resolve_source_row(browser, self.kind, value, create=False)
            if resolved_existing is not None:
                resolved = resolved_existing
                created_source = False
            else:
                resolved = _resolve_source_row(browser, self.kind, value, create=True)
                created_source = True
            if resolved is None:
                # Defensive only: create=True should always either resolve or raise.
                raise ValueError("Unable to resolve source row for {}={!r}.".format(self.kind, value))
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
                    op_desc = "{}={!r} -> {}:{}".format(self.kind, value, target_table, target_id)
                    if best_effort:
                        browser.emit("ERROR (best-effort): {} ({})".format(op_desc, exc))
                        errors.append("{} ({})".format(op_desc, exc))
                        continue

                    rollback_errors = _rollback_on_bulk_changes(
                        browser,
                        created_link_rows=created_link_rows,
                        created_source_rows=created_source_rows,
                    )
                    if rollback_errors:
                        browser.emit("Rollback encountered {} issue(s):".format(len(rollback_errors)))
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
    """Attach a note row to one or more target rows."""

    name = "note"
    aliases = ("notes",)
    summary = "Attach note(s): on note <table> <id|selector> [--best-effort] <note text>"
    usage = "on note <table> <id|id,id|start-end> [--best-effort] <note text>"
    kind = "note"


class OnTagCommand(_OnBaseCommand):
    """Attach one or more tags/labels to one or more target rows."""

    name = "tag"
    aliases = ("tags", "label", "labels")
    summary = "Attach tag(s): on tag <table> <id|selector> [--best-effort] <tag...>"
    usage = "on tag <table> <id|id,id|start-end> [--best-effort] <tag...>"
    kind = "tag"


class OnGenreCommand(_OnBaseCommand):
    """Attach a genre row to one or more target rows."""

    name = "genre"
    aliases = ("genres",)
    summary = "Attach genre: on genre <table> <id|selector> [--best-effort] <genre>"
    usage = "on genre <table> <id|id,id|start-end> [--best-effort] <genre>"
    kind = "genre"


class OnSubjectCommand(_OnBaseCommand):
    """Attach a subject row to one or more target rows."""

    name = "subject"
    aliases = ("subjects",)
    summary = "Attach subject: on subject <table> <id|selector> [--best-effort] <subject>"
    usage = "on subject <table> <id|id,id|start-end> [--best-effort] <subject>"
    kind = "subject"


class OnLanguageCommand(_OnBaseCommand):
    """Attach an existing language row to one or more target rows."""

    name = "language"
    aliases = ("languages", "lang")
    summary = "Attach language: on language <table> <id|selector> [--best-effort] <language|code>"
    usage = "on language <table> <id|id,id|start-end> [--best-effort] <language|code>"
    kind = "language"


class OnSeriesCommand(_OnBaseCommand):
    """Attach a series row to one or more target rows."""

    name = "series"
    aliases = ()
    summary = "Attach series: on series <table> <id|selector> [--best-effort] <series>"
    usage = "on series <table> <id|id,id|start-end> [--best-effort] <series>"
    kind = "series"
