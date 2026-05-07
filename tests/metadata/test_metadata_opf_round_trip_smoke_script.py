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
    def __init__(self, item_id: int) -> None:
        self.row_id = item_id
        self.row_dict = {"item_id": item_id}


class _FakeItemDatabase:
    def get_all_rows(self, table: str):
        assert table == "items"
        return iter([_FakeRow(4), _FakeRow(5), _FakeRow(6)])


def test_metadata_opf_round_trip_smoke_selects_item_ids() -> None:
    module = _load_script_module()

    assert module.select_item_ids(_FakeItemDatabase(), (), limit=2) == (4, 5)
    assert module.select_item_ids(_FakeItemDatabase(), (9, 10), limit=2) == (9, 10)


def test_metadata_opf_round_trip_smoke_compares_snapshots() -> None:
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
