from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.tkinter_gui.app import build_arg_parser, config_from_args
from LiuXin_alpha.surfaces.tkinter_gui.backend import TkGuiBackend
from LiuXin_alpha.surfaces.tkinter_gui.session import TkGuiSession
from LiuXin_alpha.surfaces.tkinter_gui.state import TkGuiConfig
from LiuXin_alpha.surfaces.tkinter_gui.tasks import TkGuiTaskRunner


class _FakeDriverWrapper:
    def get_id_column(self, table: str) -> str:
        return {
            "items": "item_id",
            "tags": "tag_id",
        }.get(table, f"{table}_id")


class _FakeRow:
    def __init__(self, table: str, row_dict: dict[str, object]) -> None:
        self.table = table
        self.row_dict = dict(row_dict)
        self.row_id = self.row_dict.get(_FakeDriverWrapper().get_id_column(table))


class _FakeDatabase:
    driver_wrapper = _FakeDriverWrapper()

    def __init__(self) -> None:
        self.closed = False
        self.tables = {
            "items": ("item_id", "title", "creator"),
            "tags": ("tag_id", "tag_name"),
        }
        self.rows = {
            "items": [
                _FakeRow("items", {"item_id": 1, "title": "Arabian Frights", "creator": "Jane Author"}),
                _FakeRow("items", {"item_id": 2, "title": "Permutation City", "creator": "Greg Egan"}),
            ],
            "tags": [
                _FakeRow("tags", {"tag_id": 3, "tag_name": "Science Fiction"}),
            ],
        }

    def get_tables_and_columns(self):
        return dict(self.tables)

    def get_record_count(self, table: str) -> int:
        return len(self.rows[str(table)])

    def get_all_rows(self, table: str):
        return iter(self.rows[str(table)])

    def search(self, table: str, column: str, search_term: str):
        needle = str(search_term).lower()
        return [
            row
            for row in self.rows[str(table)]
            if needle in str(row.row_dict.get(str(column), "")).lower()
        ]

    def close(self) -> None:
        self.closed = True


def test_tkinter_backend_lists_tables_and_pages_rows() -> None:
    backend = TkGuiBackend(_FakeDatabase())

    assert backend.table_summaries()[0].name == "items"
    assert backend.table_summaries()[0].record_count is None
    assert backend.table_summaries(include_counts=True)[0].record_count == 2
    assert backend.columns("items") == ("item_id", "title", "creator")
    schema = backend.table_schema("items", include_count=True)
    assert schema.table == "items"
    assert schema.id_column == "item_id"
    assert schema.record_count == 2
    assert schema.display_lines() == (
        "table: items",
        "rows: 2",
        "id column: item_id",
        "columns: 3",
        "- item_id",
        "- title",
        "- creator",
    )
    assert backend.table_schema_lines("items", include_count=False)[0] == "table: items"

    page = backend.page_rows("items", offset=0, limit=1)
    assert page.total_count == 2
    assert page.has_next is True
    assert backend.row_values("items", page.rows[0]) == ("1", "Arabian Frights", "Jane Author")
    assert backend.row_item_id("items", page.rows[0]) == 1


def test_tkinter_backend_search_and_details() -> None:
    backend = TkGuiBackend(_FakeDatabase())

    page = backend.page_rows("items", search_column="title", search_text="permutation")

    assert len(page.rows) == 1
    assert backend.row_label("items", page.rows[0]) == "2: Permutation City"
    assert "title: Permutation City" in backend.row_detail_lines("items", page.rows[0])


def test_tkinter_backend_metadata_message_for_non_item_rows() -> None:
    backend = TkGuiBackend(_FakeDatabase())
    tag_row = backend.page_rows("tags").rows[0]

    assert backend.row_item_id("tags", tag_row) is None
    assert backend.metadata_text_for_row("tags", tag_row) == "No item_id is available for this row."


def test_tkinter_backend_closes_database() -> None:
    db = _FakeDatabase()
    backend = TkGuiBackend(db)

    backend.close()

    assert db.closed is True


def test_tkinter_session_exposes_core_runtime_and_proxies() -> None:
    db = _FakeDatabase()
    session = TkGuiSession.from_database(
        db,
        config=TkGuiConfig(database=Path("library.sqlite")),
    )

    health = session.health()
    describe = session.describe_api(include_targets=False)

    assert health["shutdown"] is False
    assert "health" in health["registered_query_handlers"]
    assert "api.describe" in {entry["name"] for entry in describe["queries"]}
    assert session.library_proxy.health()["shutdown"] is False
    assert session.invoke_query(target="database", method="get_record_count", args=("items",)) == 2

    session.close()

    assert session.closed is True
    assert session.core_runtime.is_shutdown is True
    assert db.closed is True


def test_tkinter_backend_can_wrap_core_session() -> None:
    db = _FakeDatabase()
    session = TkGuiSession.from_database(
        db,
        config=TkGuiConfig(database=Path("library.sqlite")),
    )
    backend = TkGuiBackend.from_session(session)

    assert backend.core_health()["shutdown"] is False
    assert backend.core_status_text().startswith("core ")
    assert backend.page_rows("items", limit=1).total_count == 2

    backend.close()

    assert session.closed is True
    assert db.closed is True


def test_tkinter_task_runner_delivers_success_and_done_callbacks() -> None:
    runner = TkGuiTaskRunner()
    successes: list[object] = []
    done: list[str] = []
    try:
        runner.submit(
            "double",
            lambda value: value * 2,
            21,
            on_success=lambda result: successes.append(result.result),
            on_done=lambda result: done.append(result.name),
        )

        assert runner.wait_for_idle(timeout_s=1.0) is True
        assert successes == [42]
        assert done == ["double"]
    finally:
        runner.close(wait=True, cancel_pending=True)


def test_tkinter_task_runner_delivers_errors() -> None:
    runner = TkGuiTaskRunner()
    errors: list[str] = []

    def _fail() -> None:
        raise ValueError("bad task")

    try:
        runner.submit(
            "failing_task",
            _fail,
            on_error=lambda result: errors.append(result.error),
        )

        assert runner.wait_for_idle(timeout_s=1.0) is True
        assert errors == ["ValueError: bad task"]
    finally:
        runner.close(wait=True, cancel_pending=True)


def test_tkinter_task_runner_can_schedule_tk_polling() -> None:
    scheduled: list[tuple[int, object]] = []
    runner = TkGuiTaskRunner(after=lambda delay_ms, callback: scheduled.append((delay_ms, callback)))
    try:
        assert runner.start_polling() is True
        assert runner.start_polling() is False
        assert scheduled
        assert scheduled[0][0] == 50
        assert callable(scheduled[0][1])
    finally:
        runner.close(wait=True, cancel_pending=True)


def test_tkinter_gui_real_tk_smoke_renders_fake_backend() -> None:
    tk = pytest.importorskip("tkinter")
    try:
        root = tk.Tk()
    except Exception as exc:
        pytest.skip(f"tkinter display unavailable: {exc}")

    from LiuXin_alpha.surfaces.tkinter_gui.controller import TkGuiApplication

    db = _FakeDatabase()
    backend = TkGuiBackend(db)
    app = TkGuiApplication(
        root,
        config=TkGuiConfig(database=Path("library.sqlite")),
        backend=backend,
    )
    try:
        root.withdraw()
        assert app.task_runner.wait_for_idle(timeout_s=2.0) is True
        root.update_idletasks()

        assert app.table_sidebar.visible_table_names[0] == "items"
        assert app.current_page is not None
        assert app.current_page.table == "items"
        assert tuple(app.row_grid.tree["columns"]) == ("item_id", "title", "creator")
        assert "table: items" in app.table_sidebar.schema_text.get("1.0", "end")
        assert "rows 1-2 of 2" in app.status_var.get()

        app.row_grid.tree.selection_set("0")
        app.on_row_selected()
        assert "title: Arabian Frights" in app.detail_panel.text.get("1.0", "end")
        assert "item_id 1 selected" in app.metadata_panel.text.get("1.0", "end")
    finally:
        app.close()
        root.destroy()


def test_tkinter_gui_parser_builds_config() -> None:
    args = build_arg_parser().parse_args(
        [
            "--database",
            "library.sqlite",
            "--db-type",
            "SQLite",
            "--title",
            "Library",
            "--page-size",
            "25",
            "--enable-maintenance",
        ]
    )

    config = config_from_args(args)

    assert config.database == Path("library.sqlite")
    assert config.db_type == "SQLite"
    assert config.title == "Library"
    assert config.page_size == 25
    assert config.enable_maintenance is True
