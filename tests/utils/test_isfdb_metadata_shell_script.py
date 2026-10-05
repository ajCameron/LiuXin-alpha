"""
Provide test isfdb metadata shell script utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test isfdb metadata shell script through a consuming regression::

        python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_script_module() -> Any:
    """
    Perform the load script module utility operation under explicit compatibility rules.

    Example:
        Exercise  load script module through a consuming regression::

            python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "isfdb_metadata_shell.py"
    spec = importlib.util.spec_from_file_location("isfdb_metadata_shell", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _FakeRow:
    """
    Carry normalized FakeRow data across the Calibre compatibility boundary.

    Example:
        Exercise  FakeRow through a consuming regression::

            python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py
    """
    row_id = 7
    row_dict = {"item_id": 7}


class _FakeDatabase:
    """
    Provide the FakeDatabase utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  FakeDatabase through a consuming regression::

            python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the FakeDatabase state.

        Example:
            Exercise  FakeDatabase.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


        :return: None; validated state is stored on the receiving object.
        """
        self.closed = False

    def get_all_rows(self, table: str):
        """
        Return all rows under the documented compatibility and safety rules.

        Example:
            Exercise  FakeDatabase.get all rows through a consuming regression::

                python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert table == "items"
        return iter([_FakeRow()])

    def get_record_count(self, table: str) -> int:
        """
        Return record count under the documented compatibility and safety rules.

        Example:
            Exercise  FakeDatabase.get record count through a consuming regression::

                python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert table == "items"
        return 1

    def close(self) -> None:
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise  FakeDatabase.close through a consuming regression::

                python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.closed = True


def test_isfdb_metadata_shell_lazy_startup_does_not_open_database(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test isfdb metadata shell lazy startup does not open database utility operation under explicit compatibility rules.

    Example:
        Exercise test isfdb metadata shell lazy startup does not open database through a consuming regression::

            python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    module = _load_script_module()
    db_path = tmp_path / "isfdb.test_db"
    db_path.write_bytes(b"")

    def fail_open_database(*args, **kwargs):
        """
        Perform the fail open database utility operation under explicit compatibility rules.

        Example:
            Exercise test isfdb metadata shell lazy startup does not open database.fail open database through a consuming regression::

                python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise AssertionError("database should not be opened during lazy startup")

    monkeypatch.setattr(module, "open_database", fail_open_database)

    assert module.main(["--database", str(db_path), "--no-console", "--no-sample"]) == 0


def test_isfdb_metadata_shell_eager_open_opens_and_closes_database(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test isfdb metadata shell eager open opens and closes database utility operation under explicit compatibility rules.

    Example:
        Exercise test isfdb metadata shell eager open opens and closes database through a consuming regression::

            python -m pytest -q tests/utils/test_isfdb_metadata_shell_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    module = _load_script_module()
    db_path = tmp_path / "isfdb.test_db"
    db_path.write_bytes(b"")
    fake_db = _FakeDatabase()

    monkeypatch.setattr(module, "open_database", lambda *args, **kwargs: fake_db)

    assert module.main(["--database", str(db_path), "--no-console", "--no-sample", "--eager-open"]) == 0
    assert fake_db.closed is True
