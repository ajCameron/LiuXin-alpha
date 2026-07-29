"""Non-visual database backend for the Tkinter GUI surface."""

from __future__ import annotations

import itertools
import json

from typing import Any, Mapping, TYPE_CHECKING

from .metadata_editing import format_metadata_write_result, parse_metadata_edit_payload
from .state import RowPage, TableSchema, TableSummary, TkGuiConfig, coerce_positive_int

if TYPE_CHECKING:
    from .session import TkGuiSession


def _row_mapping(row: object) -> dict[str, object]:
    row_dict = getattr(row, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return dict(row_dict)
    if isinstance(row, Mapping):
        return dict(row)
    return {}


def _short_text(value: object, *, width: int = 96) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = " ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


class TkGuiBackend:
    """Non-visual database access layer for the Tkinter GUI."""

    def __init__(self, db: Any, *, session: "TkGuiSession | None" = None) -> None:
        self.db = db
        self.session = session
        self._tables_and_columns: dict[str, tuple[str, ...]] | None = None

    @classmethod
    def open_database(cls, config: TkGuiConfig) -> "TkGuiBackend":
        from .session import TkGuiSession

        session = TkGuiSession.open_database(config)
        return cls.from_session(session)

    @classmethod
    def from_session(cls, session: "TkGuiSession") -> "TkGuiBackend":
        return cls(session.database, session=session)

    def close(self) -> None:
        if self.session is not None:
            self.session.close()
            return
        close = getattr(self.db, "close", None)
        if callable(close):
            close()

    def core_health(self) -> dict[str, Any]:
        if self.session is None:
            return {}
        return self.session.health()

    def core_status_text(self) -> str:
        if self.session is None:
            return "core unavailable"
        return self.session.core_status_text()

    def read_source_status_text(self) -> str:
        if self.session is None:
            return "source direct"
        return self.session.read_source_status_text()

    def supports_metadata_writes(self) -> bool:
        return self.session is not None

    def refresh_read_source(self) -> bool:
        if self.session is None:
            return False
        refreshed = self.session.refresh_read_source()
        self.db = self.session.database
        self._tables_and_columns = None
        return bool(refreshed)

    def configure_read_source(
        self,
        *,
        mode: str | None = None,
        cache_type: str | None = None,
        allow_database_fallback: bool | None = None,
    ) -> bool:
        if self.session is None:
            return False
        changed = self.session.select_read_source(
            mode=mode,
            cache_type=cache_type,
            allow_database_fallback=allow_database_fallback,
        )
        self.db = self.session.database
        self._tables_and_columns = None
        return bool(changed)

    def tables_and_columns(self) -> dict[str, tuple[str, ...]]:
        if self._tables_and_columns is None:
            getter = getattr(self.db, "get_tables_and_columns", None)
            if callable(getter):
                raw = getter()
            else:
                raw = {name: () for name in getattr(self.db, "get_tables", lambda: [])()}
            self._tables_and_columns = {
                str(table): tuple(str(column) for column in columns)
                for table, columns in dict(raw or {}).items()
            }
        return dict(self._tables_and_columns)

    def table_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.tables_and_columns()))

    def table_summaries(self, *, include_counts: bool = False) -> tuple[TableSummary, ...]:
        summaries: list[TableSummary] = []
        for table in self.table_names():
            count: int | None = None
            if include_counts:
                try:
                    count = int(self.db.get_record_count(table))
                except Exception:
                    count = 0
            summaries.append(TableSummary(name=table, record_count=count))
        return tuple(summaries)

    def table_schema(self, table: str, *, include_count: bool = False) -> TableSchema:
        table = str(table)
        count: int | None = None
        if include_count:
            try:
                count = int(self.db.get_record_count(table))
            except Exception:
                count = 0
        return TableSchema(
            table=table,
            columns=self.columns(table),
            id_column=self.id_column(table),
            record_count=count,
        )

    def table_schema_lines(self, table: str, *, include_count: bool = False) -> tuple[str, ...]:
        return self.table_schema(table, include_count=include_count).display_lines()

    def columns(self, table: str) -> tuple[str, ...]:
        return tuple(self.tables_and_columns().get(str(table), ()))

    def id_column(self, table: str) -> str:
        wrapper = getattr(self.db, "driver_wrapper", None)
        getter = getattr(wrapper, "get_id_column", None)
        if callable(getter):
            try:
                return str(getter(str(table)))
            except Exception:
                pass
        for column in self.columns(table):
            if column == f"{table.rstrip('s')}_id" or column.endswith("_id"):
                return column
        columns = self.columns(table)
        return columns[0] if columns else "id"

    def row_label(self, table: str, row: object) -> str:
        mapping = _row_mapping(row)
        id_column = self.id_column(table)
        row_id = mapping.get(id_column, getattr(row, "row_id", ""))
        title_columns = (
            "title",
            "work_title",
            "work_canonical_title",
            "item_source_name",
            "file_name",
            "creator",
            "agent_canonical_name",
            "tag_name",
            "label_name",
            "series_name",
        )
        for column in title_columns:
            value = mapping.get(column)
            if value not in (None, ""):
                return f"{row_id}: {_short_text(value, width=80)}"
        return str(row_id)

    def page_rows(
        self,
        table: str,
        *,
        offset: int = 0,
        limit: int = 100,
        search_column: str = "",
        search_text: str = "",
    ) -> RowPage:
        table = str(table)
        columns = self.columns(table)
        offset = max(0, int(offset))
        limit = coerce_positive_int(limit, default=100, maximum=1000)
        search_column = str(search_column or "").strip()
        search_text = str(search_text or "").strip()

        if search_column and search_text:
            rows = list(self.db.search(table, search_column, search_text))
            total_count = len(rows)
            page = tuple(rows[offset : offset + limit])
        else:
            try:
                total_count = int(self.db.get_record_count(table))
            except Exception:
                total_count = 0
            row_iter = self.db.get_all_rows(table)
            page = tuple(itertools.islice(row_iter, offset, offset + limit))

        return RowPage(
            table=table,
            columns=columns,
            rows=page,
            offset=offset,
            limit=limit,
            total_count=total_count,
            search_column=search_column,
            search_text=search_text,
        )

    def row_values(self, table: str, row: object) -> tuple[str, ...]:
        mapping = _row_mapping(row)
        return tuple(_short_text(mapping.get(column)) for column in self.columns(table))

    def row_detail_lines(self, table: str, row: object) -> tuple[str, ...]:
        mapping = _row_mapping(row)
        columns = self.columns(table) or tuple(mapping)
        lines = []
        for column in columns:
            value = mapping.get(column)
            text = "" if value is None else str(value)
            lines.append(f"{column}: {text}")
        return tuple(lines)

    def row_item_id(self, table: str, row: object) -> int | None:
        mapping = _row_mapping(row)
        if "item_id" in mapping and mapping.get("item_id") not in (None, ""):
            try:
                return int(mapping["item_id"])
            except Exception:
                return None
        if str(table) == "items":
            row_id = getattr(row, "row_id", None)
            if row_id not in (None, ""):
                try:
                    return int(row_id)
                except Exception:
                    return None
        return None

    def metadata_text_for_row(self, table: str, row: object) -> str:
        item_id = self.row_item_id(table, row)
        if item_id is None:
            return "No item_id is available for this row."
        try:
            if self.session is None:
                raise RuntimeError("Core session is unavailable.")
            metadata = self.session.execute_query(
                "metadata.get",
                {"item_id": item_id},
            )
            return json.dumps(
                metadata,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                default=str,
            )
        except Exception as exc:
            return f"Could not hydrate metadata for item_id {item_id}: {exc}"

    def write_metadata_for_row(
        self,
        table: str,
        row: object,
        *,
        values: Mapping[str, Any],
        fields: tuple[str, ...] | list[str] | None = None,
        kind: str = "liuxin",
        replace: bool = True,
    ) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("Tk GUI metadata writes require a core-backed session.")
        item_id = self.row_item_id(table, row)
        if item_id is None:
            raise ValueError("No item_id is available for this row.")
        result = self.session.write_metadata_values(
            item_id=item_id,
            values=dict(values),
            fields=fields,
            kind=kind,
            replace=replace,
        )
        self.db = self.session.database
        self._tables_and_columns = None
        return result

    def replace_metadata_field_for_row(
        self,
        table: str,
        row: object,
        *,
        field: str,
        text: str,
        kind: str = "liuxin",
    ) -> dict[str, Any]:
        field_name, values = parse_metadata_edit_payload(field, text)
        return self.write_metadata_for_row(
            table,
            row,
            values=values,
            fields=(field_name,),
            kind=kind,
            replace=True,
        )

    def metadata_write_result_text(self, result: Mapping[str, Any] | None) -> str:
        return format_metadata_write_result(result)

    def replace_tags_for_row(self, table: str, row: object, tags: list[str] | tuple[str, ...]) -> dict[str, Any]:
        return self.write_metadata_for_row(
            table,
            row,
            values={"tags": list(tags)},
            fields=("tags",),
            replace=True,
        )


__all__ = ["TkGuiBackend"]
