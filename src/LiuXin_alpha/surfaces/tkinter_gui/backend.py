"""
Bridge Tk views to library operations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise backend through a consuming regression::

        python -m pytest -q tests/surfaces/test_tkinter_gui.py
"""

from __future__ import annotations

import itertools
import json

from typing import Any, Mapping, TYPE_CHECKING

from .metadata_editing import format_metadata_write_result, parse_metadata_edit_payload
from .state import RowPage, TableSchema, TableSummary, TkGuiConfig, coerce_positive_int

if TYPE_CHECKING:
    from .session import TkGuiSession


def _row_mapping(row: object) -> dict[str, object]:
    """
    Perform the row mapping operation under explicit file-format and conversion rules.

    Example:
        Exercise  row mapping through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py


    :param row: Value supplied for row under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    row_dict = getattr(row, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return dict(row_dict)
    if isinstance(row, Mapping):
        return dict(row)
    return {}


def _short_text(value: object, *, width: int = 96) -> str:
    """
    Perform the short text operation under explicit file-format and conversion rules.

    Example:
        Exercise  short text through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py


    :param value: Value normalized, stored, formatted or returned.
    :param width: Value supplied for width under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = " ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


class TkGuiBackend:
    """
    Non-visual database access layer for the Tkinter GUI.

    Example:
        Exercise TkGuiBackend through a consuming regression::

            python -m pytest -q tests/surfaces/test_tkinter_gui.py
    """

    def __init__(self, db: Any, *, session: "TkGuiSession | None" = None) -> None:
        """
        Initialize and validate the tkguibackend state.

        Example:
            Exercise TkGuiBackend.  init   through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param db: Value supplied for db under the utility contract.
        :param session: Value supplied for session under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.db = db
        self.session = session
        self._tables_and_columns: dict[str, tuple[str, ...]] | None = None

    @classmethod
    def open_database(cls, config: TkGuiConfig) -> "TkGuiBackend":
        """
        Perform the open database operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.open database through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param config: Value supplied for config under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from .session import TkGuiSession

        session = TkGuiSession.open_database(config)
        return cls.from_session(session)

    @classmethod
    def from_session(cls, session: "TkGuiSession") -> "TkGuiBackend":
        """
        Perform the from session operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.from session through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param session: Value supplied for session under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return cls(session.database, session=session)

    def close(self) -> None:
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.close through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.session is not None:
            self.session.close()
            return
        close = getattr(self.db, "close", None)
        if callable(close):
            close()

    def core_health(self) -> dict[str, Any]:
        """
        Perform the core health operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.core health through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.session is None:
            return {}
        return self.session.health()

    def core_status_text(self) -> str:
        """
        Perform the core status text operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.core status text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.session is None:
            return "core unavailable"
        return self.session.core_status_text()

    def read_source_status_text(self) -> str:
        """
        Read source status text under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiBackend.read source status text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.session is None:
            return "source direct"
        return self.session.read_source_status_text()

    def supports_metadata_writes(self) -> bool:
        """
        Perform the supports metadata writes operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.supports metadata writes through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.session is not None

    def refresh_read_source(self) -> bool:
        """
        Perform the refresh read source operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.refresh read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the configure read source operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.configure read source through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param mode: Open or adapter mode controlling read/write behavior.
        :param cache_type: Value supplied for cache type under the utility contract.
        :param allow_database_fallback: Value supplied for allow database fallback under the
            utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the tables and columns operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.tables and columns through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the table names operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.table names through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return tuple(sorted(self.tables_and_columns()))

    def table_summaries(self, *, include_counts: bool = False) -> tuple[TableSummary, ...]:
        """
        Perform the table summaries operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.table summaries through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param include_counts: Value supplied for include counts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the table schema operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.table schema through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param include_count: Value supplied for include count under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the table schema lines operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.table schema lines through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param include_count: Value supplied for include count under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table_schema(table, include_count=include_count).display_lines()

    def columns(self, table: str) -> tuple[str, ...]:
        """
        Perform the columns operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.columns through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return tuple(self.tables_and_columns().get(str(table), ()))

    def id_column(self, table: str) -> str:
        """
        Perform the id column operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.id column through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the row label operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.row label through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the page rows operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.page rows through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :param limit: Value supplied for limit under the utility contract.
        :param search_column: Value supplied for search column under the utility contract.
        :param search_text: Value supplied for search text under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the row values operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.row values through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mapping = _row_mapping(row)
        return tuple(_short_text(mapping.get(column)) for column in self.columns(table))

    def row_detail_lines(self, table: str, row: object) -> tuple[str, ...]:
        """
        Perform the row detail lines operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.row detail lines through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mapping = _row_mapping(row)
        columns = self.columns(table) or tuple(mapping)
        lines = []
        for column in columns:
            value = mapping.get(column)
            text = "" if value is None else str(value)
            lines.append(f"{column}: {text}")
        return tuple(lines)

    def row_item_id(self, table: str, row: object) -> int | None:
        """
        Perform the row item id operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.row item id through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the metadata text for row operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.metadata text for row through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Write metadata for row under the format's safety and compatibility rules.

        Example:
            Exercise TkGuiBackend.write metadata for row through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :param values: Value supplied for values under the utility contract.
        :param fields: Value supplied for fields under the utility contract.
        :param kind: Value supplied for kind under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the replace metadata field for row operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.replace metadata field for row through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :param field: Metadata or template field addressed by the operation.
        :param text: Text parsed, normalized or rendered.
        :param kind: Value supplied for kind under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the metadata write result text operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.metadata write result text through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param result: Value supplied for result under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return format_metadata_write_result(result)

    def replace_tags_for_row(self, table: str, row: object, tags: list[str] | tuple[str, ...]) -> dict[str, Any]:
        """
        Perform the replace tags for row operation under explicit file-format and conversion rules.

        Example:
            Exercise TkGuiBackend.replace tags for row through a consuming regression::

                python -m pytest -q tests/surfaces/test_tkinter_gui.py


        :param table: Value supplied for table under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :param tags: Value supplied for tags under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.write_metadata_for_row(
            table,
            row,
            values={"tags": list(tags)},
            fields=("tags",),
            replace=True,
        )


__all__ = ["TkGuiBackend"]
