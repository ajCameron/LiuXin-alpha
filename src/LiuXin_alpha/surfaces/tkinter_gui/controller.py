"""Tkinter application controller for the LiuXin GUI surface."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from .backend import TkGuiBackend
from .metadata_editing import METADATA_EDIT_FIELDS
from .state import RowPage, TableSchema, TkGuiConfig
from .tasks import TkGuiTaskResult, TkGuiTaskRunner
from .views import (
    DatabaseToolbar,
    DetailInspector,
    MetadataPanel,
    RowGrid,
    StatusBar,
    TableSidebar,
)


def open_tk_modules():
    """
    Import Tkinter modules lazily and report platform availability clearly.


    :return:
    """
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("Tkinter is not available in this Python environment.") from exc
    return tk, ttk, filedialog, messagebox


class TkGuiApplication:
    """Tkinter widgets and event handling."""

    def __init__(self, root: Any, *, config: TkGuiConfig, backend: Optional[TkGuiBackend] = None) -> None:
        tk, ttk, filedialog, messagebox = open_tk_modules()
        self.tk = tk
        self.ttk = ttk
        self.filedialog = filedialog
        self.messagebox = messagebox
        self.root = root
        self.config = config
        self.backend = backend
        self.task_runner = TkGuiTaskRunner(after=getattr(root, "after", None), max_workers=1)
        self.current_page: RowPage | None = None
        self.current_row: object | None = None
        self._open_generation = 0
        self._table_generation = 0
        self._page_generation = 0
        self._metadata_generation = 0
        self._closing = False
        self._busy_tasks: set[str] = set()

        self.database_var = tk.StringVar(
            value=str(config.core_endpoint or config.database or "")
        )
        self.status_var = tk.StringVar(value="Ready")
        self.table_filter_var = tk.StringVar(value="")
        self.search_column_var = tk.StringVar(value="")
        self.search_text_var = tk.StringVar(value="")
        self.read_source_var = tk.StringVar(value=str(config.read_source_mode or "direct"))
        self.cache_type_var = tk.StringVar(value=str(config.cache_type or "schema_backed"))
        self.metadata_edit_field_var = tk.StringVar(value=METADATA_EDIT_FIELDS[0])
        self.metadata_edit_value_var = tk.StringVar(value="")
        self._table_summaries = ()

        self._build_widgets()
        self._update_control_state()
        self.task_runner.start_polling()
        if self.backend is None and config.database is not None:
            self.open_database(Path(config.database))
        else:
            self.refresh_tables()

    def _build_widgets(self) -> None:
        tk = self.tk
        ttk = self.ttk
        self.root.title(self.config.title)
        self.root.geometry("1180x760")
        self.root.minsize(860, 540)
        protocol = getattr(self.root, "protocol", None)
        if callable(protocol):
            protocol("WM_DELETE_WINDOW", self.close_window)

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Toolbar.TFrame", padding=(8, 6))
        style.configure("Status.TLabel", padding=(8, 4))

        self.toolbar = DatabaseToolbar(
            self.root,
            ttk=ttk,
            database_var=self.database_var,
            read_source_var=self.read_source_var,
            cache_type_var=self.cache_type_var,
            on_open=self.choose_database,
            on_reload=self.reload_database,
            on_refresh_source=self.refresh_read_source,
        )
        self.toolbar.frame.pack(side=tk.TOP, fill=tk.X)

        main_pane = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_pane.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        left = ttk.Frame(main_pane, padding=(8, 8))
        main_pane.add(left, weight=1)
        self.table_sidebar = TableSidebar(
            left,
            tk=tk,
            ttk=ttk,
            filter_var=self.table_filter_var,
            on_filter_changed=self.refresh_table_list,
            on_selected=self.on_table_selected,
        )

        right = ttk.Frame(main_pane, padding=(8, 8))
        main_pane.add(right, weight=4)
        self.row_grid = RowGrid(
            right,
            tk=tk,
            ttk=ttk,
            search_column_var=self.search_column_var,
            search_text_var=self.search_text_var,
            on_search=self.search_current_table,
            on_clear=self.clear_search,
            on_previous=self.previous_page,
            on_next=self.next_page,
            on_row_selected=self.on_row_selected,
        )

        detail_pane = ttk.Notebook(right)
        detail_pane.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False, pady=(8, 0))
        self.detail_panel = DetailInspector(detail_pane, tk=tk, ttk=ttk)
        self.metadata_panel = MetadataPanel(
            detail_pane,
            tk=tk,
            ttk=ttk,
            on_hydrate=self.hydrate_selected_metadata,
            edit_field_var=self.metadata_edit_field_var,
            edit_value_var=self.metadata_edit_value_var,
            on_replace=self.replace_selected_metadata_field,
        )
        detail_pane.add(self.detail_panel.frame, text="Details")
        detail_pane.add(self.metadata_panel.frame, text="Metadata")

        self.status_bar = StatusBar(self.root, tk=tk, ttk=ttk, status_var=self.status_var)
        self.status_bar.label.pack(side=tk.BOTTOM, fill=tk.X)

    def choose_database(self) -> None:
        filename = self.filedialog.askopenfilename(
            title="Open LiuXin database",
            filetypes=(("Database files", "*.sqlite *.db *.test_db"), ("All files", "*.*")),
        )
        if filename:
            self.open_database(Path(filename))

    def reload_database(self) -> None:
        self.open_database(Path(self.database_var.get()))

    def _status_with_core(self, message: str) -> str:
        parts = [str(message)]
        if self.backend is None:
            return " | ".join(parts)
        try:
            core_status = self.backend.core_status_text()
        except Exception:
            core_status = ""
        if core_status and core_status != "core unavailable":
            parts.append(core_status)
        try:
            source_status = self.backend.read_source_status_text()
        except Exception:
            source_status = ""
        if source_status:
            parts.append(source_status)
        return " | ".join(parts)

    def _show_task_error(self, title: str, result: TkGuiTaskResult) -> None:
        if self._closing:
            return
        self.status_var.set(f"{title} failed")
        self.messagebox.showerror(f"{title} failed", result.error or "Unknown error")

    def _set_busy(self, task_name: str, busy: bool) -> None:
        if busy:
            self._busy_tasks.add(str(task_name))
        else:
            self._busy_tasks.discard(str(task_name))
        self._update_control_state()

    def _is_busy(self, *task_names: str) -> bool:
        if not task_names:
            return bool(self._busy_tasks)
        return any(str(name) in self._busy_tasks for name in task_names)

    def _selected_row_has_item_id(self) -> bool:
        if self.current_page is None or self.current_row is None or self.backend is None:
            return False
        return self.backend.row_item_id(self.current_page.table, self.current_row) is not None

    def _selected_row_supports_metadata_write(self) -> bool:
        if not self._selected_row_has_item_id() or self.backend is None:
            return False
        return self.backend.supports_metadata_writes()

    def _update_control_state(self) -> None:
        opening = self._is_busy("open_database")
        loading_tables = self._is_busy("load_tables")
        loading_rows = self._is_busy("load_rows")
        hydrating = self._is_busy("hydrate_metadata")
        writing_metadata = self._is_busy("write_metadata")
        refreshing_source = self._is_busy("refresh_read_source")
        has_backend = self.backend is not None
        page = self.current_page
        page_ready = (
            has_backend
            and page is not None
            and not opening
            and not loading_tables
            and not loading_rows
            and not refreshing_source
        )

        self.toolbar.set_busy(opening or refreshing_source)
        self.toolbar.set_source_refresh_enabled(has_backend and not opening and not refreshing_source)
        self.table_sidebar.set_enabled(has_backend and not opening and not loading_tables and not refreshing_source)
        self.row_grid.set_controls_enabled(
            page_ready,
            has_previous=bool(page.has_previous) if page is not None else False,
            has_next=bool(page.has_next) if page is not None else False,
        )
        self.metadata_panel.set_hydrate_enabled(
            page_ready and self._selected_row_has_item_id() and not hydrating and not writing_metadata
        )
        self.metadata_panel.set_edit_enabled(
            page_ready
            and self._selected_row_supports_metadata_write()
            and not hydrating
            and not writing_metadata
        )

    @staticmethod
    def _schema_text(schema: TableSchema | None) -> str:
        if schema is None:
            return ""
        return "\n".join(schema.display_lines())

    def open_database(self, database_path: Path) -> None:
        if self._closing:
            return
        self._set_busy("open_database", True)
        self._open_generation += 1
        token = self._open_generation
        old_backend = self.backend
        config = TkGuiConfig(
            database=Path(database_path),
            core_endpoint=None,
            core_timeout=self.config.core_timeout,
            db_type=self.config.db_type,
            title=self.config.title,
            page_size=self.config.page_size,
            max_page_size=self.config.max_page_size,
            enable_storage_manager=self.config.enable_storage_manager,
            enable_maintenance=self.config.enable_maintenance,
            repair_bootstrap_rows=self.config.repair_bootstrap_rows,
            read_source_mode=self.read_source_var.get(),
            cache_type=self.cache_type_var.get(),
            allow_cache_database_fallback=self.config.allow_cache_database_fallback,
        )
        self.config = config
        self.database_var.set(str(config.database))
        self.read_source_var.set(str(config.read_source_mode))
        self.cache_type_var.set(str(config.cache_type))
        self.backend = None
        self.current_page = None
        self.current_row = None
        self._table_summaries = ()
        self.refresh_table_list()
        self.table_sidebar.set_schema_text("")
        self.row_grid.clear_rows()
        self.detail_panel.set_text("")
        self.metadata_panel.set_text("")
        self.status_var.set(f"Opening {config.database}...")
        self._update_control_state()

        def _open_backend() -> TkGuiBackend:
            if old_backend is not None:
                old_backend.close()
            return TkGuiBackend.open_database(config)

        def _opened(result: TkGuiTaskResult) -> None:
            backend = result.result
            if token != self._open_generation or self._closing:
                if isinstance(backend, TkGuiBackend):
                    if self.task_runner.closed:
                        backend.close()
                    else:
                        self.task_runner.submit("close_stale_database", backend.close)
                return
            self.backend = backend
            self.status_var.set(self._status_with_core(f"Opened {config.database}"))
            self.refresh_tables()

        self.task_runner.submit(
            "open_database",
            _open_backend,
            on_success=_opened,
            on_error=lambda result: self._show_task_error("Open", result),
            on_done=lambda _result: self._set_busy("open_database", False),
        )

    def refresh_tables(self) -> None:
        if self.backend is None:
            return
        self._set_busy("load_tables", True)
        self._table_generation += 1
        token = self._table_generation
        backend = self.backend
        self.status_var.set(self._status_with_core("Loading tables..."))

        def _tables_loaded(result: TkGuiTaskResult) -> None:
            if token != self._table_generation or backend is not self.backend or self._closing:
                return
            self._set_busy("load_tables", False)
            self._table_summaries = tuple(result.result or ())
            self.refresh_table_list()
            self.table_sidebar.set_schema_text("")
            if self._table_summaries:
                self.table_sidebar.select_first()
                self.on_table_selected()

        self.task_runner.submit(
            "load_tables",
            backend.table_summaries,
            on_success=_tables_loaded,
            on_error=lambda result: self._show_task_error("Load tables", result),
            on_done=lambda _result: self._set_busy("load_tables", False),
        )

    def refresh_table_list(self) -> None:
        self.table_sidebar.set_tables(
            self._table_summaries,
            filter_text=self.table_filter_var.get(),
        )

    def selected_table(self) -> str | None:
        return self.table_sidebar.selected_table()

    def on_table_selected(self, _event: object | None = None) -> None:
        table = self.selected_table()
        if table is None:
            return
        self.load_table(table, offset=0)

    def load_table(
        self,
        table: str,
        *,
        offset: int = 0,
        search_column: str = "",
        search_text: str = "",
    ) -> None:
        if self.backend is None:
            return
        self._set_busy("load_rows", True)
        self._page_generation += 1
        token = self._page_generation
        backend = self.backend
        self.current_page = None
        self.current_row = None
        self.row_grid.clear_rows()
        self.table_sidebar.set_schema_text("Loading schema...")
        self.detail_panel.set_text("")
        self.metadata_panel.set_text("")
        self.status_var.set(self._status_with_core(f"Loading {table}..."))

        def _load_page_and_schema() -> tuple[RowPage, TableSchema]:
            page = backend.page_rows(
                table,
                offset=offset,
                limit=self.config.page_size,
                search_column=search_column,
                search_text=search_text,
            )
            schema = backend.table_schema(table, include_count=False)
            return page, schema

        def _page_loaded(result: TkGuiTaskResult) -> None:
            if token != self._page_generation or backend is not self.backend or self._closing:
                return
            page, schema = result.result
            self.current_page = page
            self.current_row = None
            self.render_rows(page)
            self.table_sidebar.set_schema_text(self._schema_text(schema))
            self.row_grid.set_search_columns(page.columns)
            if not self.search_column_var.get() and page.columns:
                self.search_column_var.set(page.columns[0])
            end = min(page.total_count, page.offset + len(page.rows))
            start = page.offset + 1 if page.rows else 0
            self.status_var.set(
                self._status_with_core(f"{page.table}: rows {start}-{end} of {page.total_count}")
            )

        self.task_runner.submit(
            "load_rows",
            _load_page_and_schema,
            on_success=_page_loaded,
            on_error=lambda result: self._show_task_error("Load", result),
            on_done=lambda _result: self._set_busy("load_rows", False),
        )

    def refresh_read_source(self) -> None:
        if self.backend is None or self._closing:
            return
        self._set_busy("refresh_read_source", True)
        backend = self.backend
        read_source_mode = self.read_source_var.get()
        cache_type = self.cache_type_var.get()
        allow_fallback = self.config.allow_cache_database_fallback
        self.status_var.set(self._status_with_core("Refreshing read source..."))

        def _configure_or_refresh_source() -> tuple[bool, bool]:
            changed = backend.configure_read_source(
                mode=read_source_mode,
                cache_type=cache_type,
                allow_database_fallback=allow_fallback,
            )
            refreshed = False if changed else backend.refresh_read_source()
            return changed, refreshed

        def _source_refreshed(result: TkGuiTaskResult) -> None:
            if backend is not self.backend or self._closing:
                return
            changed, refreshed = result.result
            if backend.session is not None:
                self.config = backend.session.config
                self.read_source_var.set(str(self.config.read_source_mode))
                self.cache_type_var.set(str(self.config.cache_type))
            if changed:
                message = "Read source updated"
            elif refreshed:
                message = "Read source refreshed"
            else:
                message = "Read source is already live"
            self.status_var.set(self._status_with_core(message))
            self.refresh_tables()

        self.task_runner.submit(
            "refresh_read_source",
            _configure_or_refresh_source,
            on_success=_source_refreshed,
            on_error=lambda result: self._show_task_error("Refresh source", result),
            on_done=lambda _result: self._set_busy("refresh_read_source", False),
        )

    def render_rows(self, page: RowPage) -> None:
        self.row_grid.render_rows(page, backend=self.backend)

    def on_row_selected(self, _event: object | None = None) -> None:
        if self.current_page is None or self.backend is None:
            return
        index = self.row_grid.selected_index()
        if index is None:
            return
        try:
            row = self.current_page.rows[index]
        except Exception:
            return
        self.current_row = row
        detail = "\n".join(self.backend.row_detail_lines(self.current_page.table, row))
        self.detail_panel.set_text(detail)
        item_id = self.backend.row_item_id(self.current_page.table, row)
        if item_id is None:
            self.metadata_panel.set_text("No item_id is available for this row.")
        else:
            self.metadata_panel.set_text(f"item_id {item_id} selected")
        self._update_control_state()

    def search_current_table(self) -> None:
        table = self.selected_table()
        if table is None:
            return
        self.load_table(
            table,
            offset=0,
            search_column=self.search_column_var.get(),
            search_text=self.search_text_var.get(),
        )

    def clear_search(self) -> None:
        self.search_text_var.set("")
        table = self.selected_table()
        if table is not None:
            self.load_table(table, offset=0)

    def previous_page(self) -> None:
        if self.current_page is None:
            return
        self.load_table(
            self.current_page.table,
            offset=self.current_page.previous_offset,
            search_column=self.current_page.search_column,
            search_text=self.current_page.search_text,
        )

    def next_page(self) -> None:
        if self.current_page is None or not self.current_page.has_next:
            return
        self.load_table(
            self.current_page.table,
            offset=self.current_page.next_offset,
            search_column=self.current_page.search_column,
            search_text=self.current_page.search_text,
        )

    def hydrate_selected_metadata(self) -> None:
        if self.current_page is None or self.current_row is None or self.backend is None:
            return
        if not self._selected_row_has_item_id():
            return
        self._set_busy("hydrate_metadata", True)
        self._metadata_generation += 1
        token = self._metadata_generation
        backend = self.backend
        page = self.current_page
        row = self.current_row
        self.status_var.set("Hydrating metadata...")
        update_idletasks = getattr(self.root, "update_idletasks", None)
        if callable(update_idletasks):
            update_idletasks()

        def _metadata_loaded(result: TkGuiTaskResult) -> None:
            if (
                token != self._metadata_generation
                or backend is not self.backend
                or page is not self.current_page
                or row is not self.current_row
                or self._closing
            ):
                return
            self.metadata_panel.set_text(str(result.result))
            self.status_var.set(self._status_with_core("Metadata hydrated"))

        self.task_runner.submit(
            "hydrate_metadata",
            backend.metadata_text_for_row,
            page.table,
            row,
            on_success=_metadata_loaded,
            on_error=lambda result: self._show_task_error("Hydrate metadata", result),
            on_done=lambda _result: self._set_busy("hydrate_metadata", False),
        )

    def replace_selected_metadata_field(self) -> None:
        if self.current_page is None or self.current_row is None or self.backend is None:
            return
        if not self._selected_row_supports_metadata_write():
            return
        self._set_busy("write_metadata", True)
        self._metadata_generation += 1
        token = self._metadata_generation
        backend = self.backend
        page = self.current_page
        row = self.current_row
        field = self.metadata_edit_field_var.get()
        text = self.metadata_edit_value_var.get()
        self.status_var.set(self._status_with_core("Writing metadata..."))
        update_idletasks = getattr(self.root, "update_idletasks", None)
        if callable(update_idletasks):
            update_idletasks()

        def _metadata_written(result: TkGuiTaskResult) -> None:
            if (
                token != self._metadata_generation
                or backend is not self.backend
                or page is not self.current_page
                or row is not self.current_row
                or self._closing
            ):
                return
            result_text = backend.metadata_write_result_text(result.result)
            self.metadata_panel.set_text(result_text)
            changed = bool((result.result or {}).get("changed"))
            self.status_var.set(self._status_with_core("Metadata written" if changed else "Metadata unchanged"))

        self.task_runner.submit(
            "write_metadata",
            backend.replace_metadata_field_for_row,
            page.table,
            row,
            field=field,
            text=text,
            on_success=_metadata_written,
            on_error=lambda result: self._show_task_error("Write metadata", result),
            on_done=lambda _result: self._set_busy("write_metadata", False),
        )

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._open_generation += 1
        self._table_generation += 1
        self._page_generation += 1
        self._metadata_generation += 1
        self.task_runner.wait_for_idle(timeout_s=10.0)
        backend = self.backend
        self.backend = None
        if backend is not None and not self.task_runner.closed:
            try:
                self.task_runner.submit("close_database", backend.close)
            except RuntimeError:
                backend.close()
        elif backend is not None:
            backend.close()
        self.task_runner.close(wait=True, cancel_pending=False)

    def close_window(self) -> None:
        self.close()
        destroy = getattr(self.root, "destroy", None)
        if callable(destroy):
            destroy()


__all__ = ["TkGuiApplication", "open_tk_modules"]
