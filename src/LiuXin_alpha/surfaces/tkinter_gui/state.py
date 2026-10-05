"""
Model Tk application state.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise state through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

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
    """
    Coerce a value to a positive integer or use a safe default.

    Example:
        Exercise coerce positive int through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py


    :param value: Value normalized, stored, formatted or returned.
    :param default: Value supplied for default under the utility contract.
    :param minimum: Value supplied for minimum under the utility contract.
    :param maximum: Value supplied for maximum under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Validated startup choices consumed by the Tkinter GUI controller.

    Example:
        Exercise TkGuiConfig through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    database: Path | None = None
    core_endpoint: str | None = None
    core_timeout: float = 10.0
    db_type: str = "sqlite"
    title: str = "LiuXin"
    page_size: int = 100
    max_page_size: int = 500
    read_source_mode: str = "direct"
    cache_type: str = "schema_backed"
    allow_cache_database_fallback: bool = True
    enable_storage_manager: bool = False
    enable_maintenance: bool = False
    repair_bootstrap_rows: bool = False


@dataclass(frozen=True)
class TableSummary:
    """
    Compact table identity and optional row count for sidebar display.

    Example:
        Exercise TableSummary through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    name: str
    record_count: int | None = None


@dataclass(frozen=True)
class TableSchema:
    """
    Presentation-safe table schema returned to the Tkinter view layer.

    Example:
        Exercise TableSchema through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    table: str
    columns: tuple[str, ...]
    id_column: str = ""
    record_count: int | None = None

    @property
    def column_count(self) -> int:
        """
        Perform the column count operation under explicit file-format and conversion rules.

        Example:
            Exercise TableSchema.column count through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.columns)

    def display_lines(self) -> tuple[str, ...]:
        """
        Perform the display lines operation under explicit file-format and conversion rules.

        Example:
            Exercise TableSchema.display lines through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Immutable page of rows plus navigation and search context.

    Example:
        Exercise RowPage through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

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
        """
        Perform the next offset operation under explicit file-format and conversion rules.

        Example:
            Exercise RowPage.next offset through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return min(max(0, self.total_count), self.offset + self.limit)

    @property
    def previous_offset(self) -> int:
        """
        Perform the previous offset operation under explicit file-format and conversion rules.

        Example:
            Exercise RowPage.previous offset through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return max(0, self.offset - self.limit)

    @property
    def has_next(self) -> bool:
        """
        Return whether has next holds for the supplied ebook data.

        Example:
            Exercise RowPage.has next through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.next_offset < self.total_count

    @property
    def has_previous(self) -> bool:
        """
        Return whether has previous holds for the supplied ebook data.

        Example:
            Exercise RowPage.has previous through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.offset > 0


__all__ = [
    "RowPage",
    "TableSchema",
    "TableSummary",
    "TkGuiConfig",
    "coerce_positive_int",
]
