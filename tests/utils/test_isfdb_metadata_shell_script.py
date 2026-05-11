from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_script_module() -> Any:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "isfdb_metadata_shell.py"
    spec = importlib.util.spec_from_file_location("isfdb_metadata_shell", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _FakeRow:
    row_id = 7
    row_dict = {"item_id": 7}


class _FakeDatabase:
    def __init__(self) -> None:
        self.closed = False

    def get_all_rows(self, table: str):
        assert table == "items"
        return iter([_FakeRow()])

    def get_record_count(self, table: str) -> int:
        assert table == "items"
        return 1

    def close(self) -> None:
        self.closed = True


def test_isfdb_metadata_shell_lazy_startup_does_not_open_database(tmp_path: Path, monkeypatch) -> None:
    module = _load_script_module()
    db_path = tmp_path / "isfdb.test_db"
    db_path.write_bytes(b"")

    def fail_open_database(*args, **kwargs):
        raise AssertionError("database should not be opened during lazy startup")

    monkeypatch.setattr(module, "open_database", fail_open_database)

    assert module.main(["--database", str(db_path), "--no-console", "--no-sample"]) == 0


def test_isfdb_metadata_shell_eager_open_opens_and_closes_database(tmp_path: Path, monkeypatch) -> None:
    module = _load_script_module()
    db_path = tmp_path / "isfdb.test_db"
    db_path.write_bytes(b"")
    fake_db = _FakeDatabase()

    monkeypatch.setattr(module, "open_database", lambda *args, **kwargs: fake_db)

    assert module.main(["--database", str(db_path), "--no-console", "--no-sample", "--eager-open"]) == 0
    assert fake_db.closed is True
