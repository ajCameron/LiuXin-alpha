"""Row summaries, detail sections, and tabular terminal presentation."""

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
    """Ordered unique previews collected before falling back to full row details."""

    parts: list[str] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)

    def add_value(self, raw_value: object) -> None:
        text = _preview_row_text(raw_value)
        if not text:
            return
        if text in self.seen:
            return
        self.seen.add(text)
        self.parts.append(text)


class RowsMixin[HostT](BrowserState[HostT]):
    """Row summaries, detail sections, and tabular terminal presentation."""

    def format_row(self, table: str, row: RowRecord) -> str:
        """Format one row for terminal output."""
        return self._format_row(table, row)

    def format_rows_as_table(
        self, table: str, rows: Sequence[RowRecord], *, max_cell_width: int = 60
    ) -> str:
        """Render rows as an ASCII table using schema column order."""
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
        """Build grouped vertical detail sections for one row."""
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
        """Render one row as grouped vertical detail tables."""
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
        """Render arbitrary headers/rows as an ASCII table."""
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
        """Render one or more titled two-column detail sections."""
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
        """Write titled detail sections to the current output stream."""
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
