"""
Build compact row previews, grouped detail sections, and width-aware ASCII tables for the terminal browser.

Schema headings determine tabular/detail order; compact previews prefer meaningful
fields before falling back to raw representations. Mapping lookup failures remain
visible, while the compact-preview helper tolerates failures from non-Mapping row
objects. Formatting is separate from emission and does not fetch linked entities.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from LiuXin_alpha.surfaces.terminal.presentation import (
    pretty_row_detail_group as _pretty_row_detail_group,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    preview_row_text as _preview_row_text,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    render_ascii_table as _render_ascii_table,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    row_detail_group as _row_detail_group,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    shorten_column_headers as _shorten_column_headers,
)
from LiuXin_alpha.surfaces.terminal.presentation import (
    truncate as _truncate,
)

from .contracts import BrowserState
from .models import RowRecord

_PREFERRED_ROW_FIELDS = {
    "works": ("work_title", "work_canonical_title", "work_sort_title"),
    "stores": ("store_name", "store_kind", "store_root_uri"),
    "labels": ("label_text", "label", "label_text_norm"),
    "tags": ("tag", "tag_phash", "label_text", "label"),
    "notes": ("note", "note_text", "note_body"),
    "folders": ("folder_name", "folder_relpath"),
    "files": ("file_name", "file_original_name", "file_storage_key"),
    "creators": (
        "creator",
        "creator_name",
        "agent_canonical_name",
        "agent_name",
    ),
    "agents": ("agent_canonical_name", "agent_name", "agent_sort_name"),
    "series": ("series_name", "series_title", "series_sort_title"),
    "genres": ("genre", "genre_text", "genre_name"),
    "subjects": ("subject", "subject_text", "subject_name"),
    "languages": ("language", "language_name", "language_code"),
    "expressions": (
        "expression_label",
        "expression_title_override",
        "expression_type",
    ),
    "manifestations": (
        "manifestation_label",
        "manifestation_title",
        "manifestation_type",
    ),
    "items": ("item_source_name", "item_inventory_code", "item_location"),
}


def _lookup_row_value(row: RowRecord, column: str) -> tuple[bool, object]:
    """
    Distinguish missing keys from present None values, tolerating access failures only for non-Mapping rows.

    Mappings use membership and get without catches. Other rows try membership
    followed by indexing, then retry indexing directly if membership is false or
    that first path raises. A successful direct lookup counts as present even if
    membership disagreed; an ordinary final lookup error becomes absence.

    Example:
        >>> _lookup_row_value({"title": None}, "title")
        (True, None)
        >>> _lookup_row_value({}, "title")
        (False, None)


    :param row: Mapping or membership/index-access row object to inspect without mutation.
    :param column: Exact key used for membership, get, and item access.
    :return: Presence flag and retrieved value, or (False, None) when absent or the tolerated lookup paths fail.
    """
    if isinstance(row, Mapping):
        if column in row:
            return True, row.get(column)
        return False, None
    try:
        if column in row:
            return True, row[column]
    except Exception:
        pass
    try:
        return True, row[column]
    except Exception:
        return False, None


@dataclass
class _RowSummary:
    """
    Accumulate ordered, distinct rendered preview strings for one row summary.

    Each instance gets independent mutable parts and seen containers. Uniqueness
    is based on flattened/truncated preview text, not original values. Callers
    supplying or mutating those containers are responsible for keeping them in sync.

    Example:
        >>> summary = _RowSummary()
        >>> summary.add_value(" A "); summary.add_value("A")
        >>> summary.parts
        ['A']
    """

    parts: list[str] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)

    def add_value(self, raw_value: object) -> None:
        """
        Append a nonblank 64-character preview only when its rendered text has not already been seen.

        Falsey values, including zero and False, produce no preview. Line breaks
        flatten to spaces and outer whitespace is removed before truncation and
        deduplication, so different long values can share the same preview.

        Example:
            >>> summary = _RowSummary()
            >>> summary.add_value(0); summary.add_value(" Title ")
            >>> summary.parts
            ['Title']


        :param raw_value: Candidate field value processed by the shared preview_row_text helper.
        :return: None after optionally updating both seen and parts; conversion errors propagate.
        """
        text = _preview_row_text(raw_value)
        if not text:
            return
        if text in self.seen:
            return
        self.seen.add(text)
        self.parts.append(text)


class RowsMixin[HostT](BrowserState[HostT]):
    """
    Provide row summaries and table/detail rendering using the browser's schema, width, and output services.

    The composition root supplies state and other owners supply ID-column lookup
    and emission. Character budgets are not terminal display-cell measurements;
    shared renderers may omit columns to fit a narrow output width.

    Example:
        >>> print(browser.render_row_details("works", row))  # doctest: +SKIP
    """

    def format_row(self, table: str, row: RowRecord) -> str:
        """
        Delegate compact row presentation without emitting text or changing browser state.

        Example:
            >>> text = browser.format_row("works", row)  # doctest: +SKIP


        :param table: Table name selecting preferred summary fields and fallback schema headings.
        :param row: Row record whose ID and values should be summarized.
        :return: Compact ID/value preview or fallback column/value representation from _format_row.
        """
        return self._format_row(table, row)

    def format_rows_as_table(
        self, table: str, rows: Sequence[RowRecord], *, max_cell_width: int = 60
    ) -> str:
        """
        Render schema-ordered cells with shortened headings and the browser's terminal-width budget.

        Missing columns become None cells. Membership/indexing failures propagate
        here rather than using the tolerant compact-summary lookup helper. Extra
        row keys are ignored; shared rendering may truncate cells or drop columns.

        Example:
            >>> table_text = browser.format_rows_as_table("works", rows)  # doctest: +SKIP


        :param table: Resolved table whose schema heading order defines displayed columns.
        :param rows: Ordered row records inspected by membership and item access for each heading.
        :param max_cell_width: Initial data-cell character budget, including any truncation marker.
        :return: Multiline ASCII table or the shared renderer's no-columns/insufficient-width message.
        """
        columns = self.get_table_columns(table)
        display_columns = _shorten_column_headers(columns, table_name=table)
        table_rows: list[list[object]] = []
        for row in rows:
            table_rows.append(
                [row[column] if column in row else None for column in columns]
            )
        return _render_ascii_table(
            display_columns,
            table_rows,
            max_cell_width=max_cell_width,
            max_table_width=self.get_terminal_width(),
        )

    def build_row_detail_sections(
        self, table: str, row: RowRecord
    ) -> list[tuple[str, list[tuple[object, object]]]]:
        """
        Group schema fields by display purpose, retaining raw values and prioritizing an exact ID heading within its group.

        Groups order as identity, references, access, capabilities, dates, and other;
        original schema order breaks ties. Labels are shortened schema headings.
        Missing row keys become None, extra keys are ignored, and schema/access
        errors propagate. Classification uses naming heuristics, not semantic metadata.

        Example:
            >>> sections = browser.build_row_detail_sections("works", row)  # doctest: +SKIP


        :param table: Table providing ordered headings and the optional ID-column hint.
        :param row: Record supplying raw values by exact heading key.
        :return: Ordered human-readable section titles paired with lists of display-label/raw-value pairs.
        """
        columns = self.get_table_columns(table)
        display_columns = _shorten_column_headers(columns, table_name=table)
        id_column = self._table_id_column(table)
        group_rank = {
            "identity": 0,
            "references": 1,
            "access": 2,
            "capabilities": 3,
            "dates": 4,
            "other": 5,
        }
        grouped_rows: dict[str, list[tuple[object, object]]] = {}
        ordered = list(enumerate(columns))
        ordered.sort(
            key=lambda pair: (
                group_rank.get(_row_detail_group(pair[1], id_column=id_column), 99),
                0 if id_column is not None and pair[1] == id_column else 1,
                pair[0],
            )
        )
        for idx, column in ordered:
            raw_group_name = _row_detail_group(column, id_column=id_column)
            grouped_rows.setdefault(raw_group_name, []).append(
                (display_columns[idx], row[column] if column in row else None)
            )

        sections: list[tuple[str, list[tuple[object, object]]]] = []
        ordered_groups = sorted(
            grouped_rows.keys(), key=lambda name: group_rank.get(name, 99)
        )
        for raw_group_name in ordered_groups:
            sections.append(
                (_pretty_row_detail_group(raw_group_name), grouped_rows[raw_group_name])
            )
        return sections

    def render_row_details(
        self, table: str, row: RowRecord, *, max_cell_width: int = 120
    ) -> str:
        """
        Build and render a row's grouped schema fields as column/value tables without emitting them.

        Example:
            >>> text = browser.render_row_details("works", row, max_cell_width=80)  # doctest: +SKIP


        :param table: Resolved table whose headings and ID hint determine detail sections.
        :param row: Row values to group and render, including None for missing schema keys.
        :param max_cell_width: Initial character budget applied to detail table cells before terminal-width fitting.
        :return: Newline-joined titled detail tables, or an empty string when there are no fields.
        """
        return self.render_detail_sections(
            self.build_row_detail_sections(table, row),
            key_header="column",
            value_header="value",
            max_cell_width=max_cell_width,
        )

    def render_table(
        self,
        headers: Sequence[object],
        rows: Sequence[Sequence[object]],
        *,
        max_cell_width: int = 60,
    ) -> str:
        """
        Stringify arbitrary headers and render value rows through the shared terminal-width-aware ASCII formatter.

        Short rows are padded, extra cells are discarded, and width fitting may
        remove rightmost columns. This owner does not print or mutate the input sequences.

        Example:
            >>> text = browser.render_table(["field", "value"], [["title", "Example"]])  # doctest: +SKIP


        :param headers: Ordered heading objects stringified before rendering.
        :param rows: Ordered value sequences whose cells align with the header count.
        :param max_cell_width: Initial data-cell character limit, including any ellipsis.
        :return: Multiline ASCII representation constrained by the browser's reported terminal width.
        """
        display_headers = [str(header) for header in headers]
        return _render_ascii_table(
            display_headers,
            rows,
            max_cell_width=max_cell_width,
            max_table_width=self.get_terminal_width(),
        )

    def render_detail_sections(
        self,
        sections: Sequence[tuple[str, Sequence[tuple[object, object]]]],
        *,
        key_header: str = "field",
        value_header: str = "value",
        max_cell_width: int = 120,
    ) -> str:
        """
        Render nonempty detail sections as titled two-column tables with blank lines between sections.

        Keys are stringified and section titles are stringified/stripped. Empty
        sections disappear entirely; a blank title omits only that heading, not
        its table. Rendering completes before this helper returns any text.

        Example:
            >>> text = browser.render_detail_sections([("Identity", [("title", "Example")])])  # doctest: +SKIP


        :param sections: Ordered section titles and key/value row sequences.
        :param key_header: Header label for each section's key column.
        :param value_header: Header label for each section's value column.
        :param max_cell_width: Initial per-cell character budget passed to render_table.
        :return: Newline-joined section blocks, or an empty string if every section is empty.
        """
        blocks: list[str] = []
        for section_title, rows in sections:
            normalized_rows = [(str(key), value) for key, value in rows]
            if not normalized_rows:
                continue
            if blocks:
                blocks.append("")
            title = str(section_title).strip()
            if title:
                blocks.append(title)
            blocks.append(
                self.render_table(
                    [key_header, value_header],
                    normalized_rows,
                    max_cell_width=max_cell_width,
                )
            )
        return "\n".join(blocks)

    def emit_detail_sections(
        self,
        sections: Sequence[tuple[str, Sequence[tuple[object, object]]]],
        *,
        title: str | None = None,
        key_header: str = "field",
        value_header: str = "value",
        max_cell_width: int = 120,
    ) -> None:
        """
        Render all sections first, then emit an optional overall title and the nonempty rendered body.

        A truthy overall title is emitted unchanged even with no body. A blank
        separator follows it only when body text exists. Output failures can leave
        a partially emitted result; this helper does not roll back the stream.

        Example:
            >>> browser.emit_detail_sections(sections, title="Work details")  # doctest: +SKIP


        :param sections: Ordered titled key/value rows passed to the detail renderer.
        :param title: Optional overall heading emitted before the body when truthy.
        :param key_header: Per-section key-column label forwarded to rendering.
        :param value_header: Per-section value-column label forwarded to rendering.
        :param max_cell_width: Initial per-cell character budget before terminal-width fitting.
        :return: None after applicable emissions; rendering errors occur before any emission by this helper.
        """
        rendered = self.render_detail_sections(
            sections,
            key_header=key_header,
            value_header=value_header,
            max_cell_width=max_cell_width,
        )
        if title:
            self.emit(title)
            if rendered:
                self.emit("")
        if rendered:
            self.emit(rendered)

    def _format_row(self, table: str, row: RowRecord) -> str:
        """
        Prefer table-specific preview fields, then heuristic schema fields, and finally a full row representation.

        ID-column lookup errors are suppressed. A usable preview yields at most
        three distinct snippets, prefixed by a non-None/nonempty ID when available;
        that ID must support the membership test's hashing. Preferred values prevent
        fallback-field scanning even when fewer than three snippets were collected.
        Row access follows _lookup_row_value; other formatting/schema errors propagate.

        Example:
            >>> summary = browser._format_row("works", row)  # doctest: +SKIP


        :param table: Table selecting preferred columns and schema-driven fallback behavior.
        :param row: Record to inspect without mutating it or reading linked records.
        :return: ID/preview parts joined by vertical bars, or full present-column representations when no preview is usable.
        """
        id_column = None
        try:
            id_column = self.db.driver_wrapper.get_id_column(table)
        except Exception:
            id_column = None

        summary = _RowSummary()

        raw_id = None
        if id_column is not None:
            has_id, raw_id = _lookup_row_value(row, id_column)
            if not has_id:
                raw_id = None

        for column in _PREFERRED_ROW_FIELDS.get(table, ()):
            present, value = _lookup_row_value(row, column)
            if present:
                summary.add_value(value)

        if not summary.parts:
            self._fallback_row_summary(table, row, id_column, summary)

        if summary.parts:
            parts: list[str] = []
            if raw_id not in {None, ""}:
                parts.append(f"#{raw_id}")
            parts.extend(summary.parts[:3])
            return " | ".join(parts)

        return self._format_full_row(table, row, id_column)

    def _fallback_row_summary(
        self, table: str, row: RowRecord, id_column: str | None, summary: _RowSummary
    ) -> None:
        """
        Add non-ID schema values by preferred keyword rank and original-name lexical order until three previews exist.

        Keyword priority favors names/titles/labels before text, paths, types, and
        codes. Absent/blank/duplicate previews do not consume slots. The three-part
        check follows each present value, so callers should start with fewer than
        three parts; no preexisting excess is trimmed or reconstructed.

        Example:
            >>> browser._fallback_row_summary("works", row, "work_id", summary)  # doctest: +SKIP


        :param table: Table supplying the complete schema heading list to rank.
        :param row: Record inspected through the compact-summary lookup helper.
        :param id_column: Exact heading to skip, or None to allow every column.
        :param summary: Mutable preview accumulator extended in place; normally empty on entry.
        :return: None after reaching three previews or exhausting the ranked headings.
        """
        keyword_priority = (
            "name",
            "title",
            "label",
            "note",
            "text",
            "path",
            "uri",
            "kind",
            "type",
            "location",
            "code",
        )
        columns = list(self.db.get_column_headings(table))
        ordered_columns = sorted(
            columns,
            key=lambda key: (
                min(
                    (
                        idx
                        for idx, token in enumerate(keyword_priority)
                        if token in str(key).lower()
                    ),
                    default=len(keyword_priority),
                ),
                str(key),
            ),
        )
        for column in ordered_columns:
            if id_column is not None and column == id_column:
                continue
            present, value = _lookup_row_value(row, column)
            if not present:
                continue
            summary.add_value(value)
            if len(summary.parts) >= 3:
                break

    def _format_full_row(
        self, table: str, row: RowRecord, id_column: str | None
    ) -> str:
        """
        Render present schema fields as abbreviated repr values, placing a separately requested ID field first.

        The ID uses a 24-character target; other values use the shared truncator's
        default of 80. Present None values remain visible as repr text. The supplied
        ID may appear even when absent from schema headings; missing fields are skipped.

        Example:
            >>> text = browser._format_full_row("works", row, "work_id")  # doctest: +SKIP


        :param table: Resolved table whose schema order controls non-ID output fields.
        :param row: Record accessed through the Mapping-sensitive lookup helper.
        :param id_column: Optional exact ID key emitted before other schema columns when present.
        :return: Vertical-bar-separated column=repr pieces, or an empty string when no requested field is present.
        """
        columns = self.db.get_column_headings(table)
        pieces: list[str] = []
        if id_column is not None:
            present, value = _lookup_row_value(row, id_column)
            if present:
                pieces.append(f"{id_column}={_truncate(value, width=24)}")

        for column in columns:
            if id_column is not None and column == id_column:
                continue
            present, value = _lookup_row_value(row, column)
            if not present:
                continue
            pieces.append(f"{column}={_truncate(value)}")

        return " | ".join(pieces)
