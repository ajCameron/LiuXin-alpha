"""
Exercise the standalone OPF round-trip smoke script and its failure reporting.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata opf round trip smoke script through its owning regression module::

        python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
from tests.metadata.containers.test_item_metadata_hydrator import _build_fake_database


_SCRIPT_MODULE: Any | None = None


def _load_script_module() -> Any:
    """
    Perform the load script module test-helper operation with deterministic inputs.

    Example:
        Exercise load script module through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :return: The deterministic value, row, identity or collection described above.
    """
    global _SCRIPT_MODULE
    if _SCRIPT_MODULE is not None:
        return _SCRIPT_MODULE
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "metadata_opf_round_trip_smoke.py"
    spec = importlib.util.spec_from_file_location("metadata_opf_round_trip_smoke", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _SCRIPT_MODULE = module
    return module


class _FakeRow:
    """
    Provide the FakeRow test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeRow through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py
    """
    def __init__(self, item_id: int) -> None:
        """
        Initialize the FakeRow test double.

        Example:
            Exercise FakeRow.init through its owning regression module::

                python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


        :param item_id: Value supplied for item id in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.row_id = item_id
        self.row_dict = {"item_id": item_id}


class _FakeItemDatabase:
    """
    Provide the FakeItemDatabase test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeItemDatabase through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py
    """
    def get_all_rows(self, table: str):
        """
        Return copied rows from the requested in-memory table.

        Example:
            Exercise FakeItemDatabase.get all rows through its owning regression module::

                python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        assert table == "items"
        return iter([_FakeRow(4), _FakeRow(5), _FakeRow(6)])


def test_metadata_opf_round_trip_smoke_selects_item_ids() -> None:
    """
    Verify metadata opf round trip smoke selects item ids.

    Example:
        Exercise test metadata opf round trip smoke selects item ids through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :return: None; the function records state or raises through its assertions.
    """
    module = _load_script_module()

    assert module.select_item_ids(_FakeItemDatabase(), (), limit=2) == (4, 5)
    assert module.select_item_ids(_FakeItemDatabase(), (9, 10), limit=2) == (9, 10)


def test_metadata_opf_round_trip_smoke_compares_snapshots() -> None:
    """
    Verify metadata opf round trip smoke compares snapshots.

    Example:
        Exercise test metadata opf round trip smoke compares snapshots through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :return: None; the function records state or raises through its assertions.
    """
    module = _load_script_module()

    before = {
        "title": "Permutation City",
        "authors": ("Greg Egan",),
        "tags": ("Space Opera",),
        "series": (),
        "identifiers": {"isbn": ("9780000000001",)},
    }
    after = {
        "title": "Permutation City",
        "authors": ("Greg Egan",),
        "tags": ("Space Opera",),
        "series": (),
        "identifiers": {"isbn": ("9780000000001",)},
    }

    assert module.compare_snapshots(before, after) == []
    assert module.compare_snapshots(before, {**after, "title": "Changed"}) == [
        "title changed from 'Permutation City' to 'Changed'",
    ]
    assert module.compare_snapshots(before, {**after, "tags": ()}, strict=True) == [
        "tags changed from ('Space Opera',) to ()",
    ]


def test_metadata_opf_round_trip_smoke_runs_against_fake_database(tmp_path: Path) -> None:
    """
    Verify metadata opf round trip smoke runs against fake database.

    Example:
        Exercise test metadata opf round trip smoke runs against fake database through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    module = _load_script_module()
    db = _build_fake_database()

    result = module.round_trip_item(db, 1, opf_dir=tmp_path)

    assert result.ok is True
    assert result.errors == ()
    assert result.before["title"] == "Permutation City"
    assert result.after["title"] == "Permutation City"
    assert result.opf_bytes > 0
    assert result.opf_path is not None
    assert Path(result.opf_path).is_file()


def test_metadata_opf_round_trip_smoke_can_write_back_after_opf(tmp_path: Path) -> None:
    """
    Verify metadata opf round trip smoke can write back after opf.

    Example:
        Exercise test metadata opf round trip smoke can write back after opf through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    module = _load_script_module()
    db = _build_fake_database()

    result = module.round_trip_item(
        db,
        1,
        opf_dir=tmp_path,
        write_back=True,
        write_back_fields=("tags",),
        write_back_add_tags=("smoke-writeback-tag",),
    )
    rehydrated = LiuXinWEMIMetadata.from_database(db, item_id=1)

    assert result.ok is True
    assert result.write_report is not None
    assert result.write_report["changed"] is True
    assert "smoke-writeback-tag" in rehydrated.tags


def test_metadata_opf_round_trip_smoke_requires_safe_write_back_target(tmp_path: Path) -> None:
    """
    Verify metadata opf round trip smoke requires safe write back target.

    Example:
        Exercise test metadata opf round trip smoke requires safe write back target through its owning regression module::

            python -m pytest -q tests/metadata/test_metadata_opf_round_trip_smoke_script.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    module = _load_script_module()
    source = tmp_path / "source.test_db"
    source.write_bytes(b"db")

    with pytest.raises(ValueError, match="Refusing metadata write-back"):
        module.prepare_write_back_database(
            source,
            write_back=True,
            scratch_db=None,
            allow_write_original=False,
        )

    scratch = tmp_path / "scratch.test_db"
    opened = module.prepare_write_back_database(
        source,
        write_back=True,
        scratch_db=scratch,
        allow_write_original=False,
    )

    assert opened == scratch
    assert scratch.read_bytes() == b"db"
