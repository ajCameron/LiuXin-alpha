"""State models for the Tkinter GUI surface."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def coerce_positive_int(
    value: object,
    *,
    default: int,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    try:
        coerced = int(value)
    except Exception:
        coerced = int(default)
    coerced = max(int(minimum), coerced)
    if maximum is not None:
        coerced = min(int(maximum), coerced)
    return coerced


@dataclass(frozen=True)
class TkGuiConfig:
    database: Path
    db_type: str = "sqlite"
    title: str = "LiuXin"
    page_size: int = 100
    max_page_size: int = 500
    enable_storage_manager: bool = False
    enable_maintenance: bool = False
    repair_bootstrap_rows: bool = False


@dataclass(frozen=True)
class TableSummary:
    name: str
    record_count: int | None = None


@dataclass(frozen=True)
class TableSchema:
    table: str
    columns: tuple[str, ...]
    id_column: str = ""
    record_count: int | None = None

    @property
    def column_count(self) -> int:
        return len(self.columns)

    def display_lines(self) -> tuple[str, ...]:
        lines = [f"table: {self.table}"]
        if self.record_count is not None:
            lines.append(f"rows: {self.record_count}")
        if self.id_column:
            lines.append(f"id column: {self.id_column}")
        lines.append(f"columns: {self.column_count}")
        lines.extend(f"- {column}" for column in self.columns)
        return tuple(lines)


@dataclass(frozen=True)
class RowPage:
    table: str
    columns: tuple[str, ...]
    rows: tuple[object, ...]
    offset: int
    limit: int
    total_count: int
    search_column: str = ""
    search_text: str = ""

    @property
    def next_offset(self) -> int:
        return min(max(0, self.total_count), self.offset + self.limit)

    @property
    def previous_offset(self) -> int:
        return max(0, self.offset - self.limit)

    @property
    def has_next(self) -> bool:
        return self.next_offset < self.total_count

    @property
    def has_previous(self) -> bool:
        return self.offset > 0


__all__ = [
    "RowPage",
    "TableSchema",
    "TableSummary",
    "TkGuiConfig",
    "coerce_positive_int",
]
