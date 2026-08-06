from __future__ import annotations

import json

from pathlib import Path
from types import SimpleNamespace

from LiuXin_alpha.surfaces.cli import storage_audit


class _FakeLibrary:
    init_kwargs: dict[str, object] = {}
    register_args: tuple[Path, dict[str, object]] | None = None

    def __init__(self, **kwargs: object) -> None:
        type(self).init_kwargs = kwargs
        print("legacy setup noise")

    def __enter__(self) -> _FakeLibrary:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def register_unmanaged_disk(self, disk_root: Path, **kwargs: object) -> SimpleNamespace:
        type(self).register_args = (disk_root, kwargs)
        print("legacy scan noise")
        return SimpleNamespace(errors=[], to_dict=lambda: {"scanned_files": 1, "errors": []})


def test_storage_audit_uses_local_sqlite_and_creates_database_by_default(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    disk_root = tmp_path / "drive"
    disk_root.mkdir()
    database_path = tmp_path / "audit.sqlite3"
    monkeypatch.setattr(storage_audit, "_open_library", _FakeLibrary)

    result = storage_audit.main(
        [
            "--database",
            str(database_path),
            "--disk-root",
            str(disk_root),
            "--store-name",
            "portable-audit",
        ]
    )

    assert result == 0
    assert _FakeLibrary.init_kwargs == {
        "database_path": database_path,
        "db_type": "SQLite",
        "create": True,
        "backup": False,
        "enable_storage_manager": False,
        "storage_startup_on_add": False,
    }
    assert _FakeLibrary.register_args == (
        disk_root,
        {
            "store_name": "portable-audit",
            "compute_hash": True,
            "follow_symlinks": False,
            "attach_store_links": True,
            "refresh_storage_manager": False,
        },
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["database_path"] == str(database_path)
    assert payload["registration_report"]["scanned_files"] == 1
    assert "legacy setup noise" in captured.err
    assert "legacy scan noise" in captured.err


def test_storage_audit_rejects_a_missing_disk_root(tmp_path: Path, capsys) -> None:
    result = storage_audit.main(
        [
            "--database",
            str(tmp_path / "audit.sqlite3"),
            "--disk-root",
            str(tmp_path / "missing-drive"),
        ]
    )

    assert result == 2
    assert "disk root is not a directory" in capsys.readouterr().err
