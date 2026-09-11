"""
Check SQLite-only storage-audit dispatch and separation of JSON from legacy noise.

The fake Library records construction/scan requests but creates no database or
real file registrations. These tests prove adapter routing/defaults and early path
rejection, not the database driver's creation or scanner's filesystem semantics.
"""

from __future__ import annotations

import json

from pathlib import Path
from types import SimpleNamespace

from LiuXin_alpha.surfaces.cli import storage_audit


class _FakeLibrary:
    """
    Record the latest audit constructor and scan calls through shared class state.

    Emit representative legacy stdout noise and return a fixed successful report.
    Class observations persist across instances and are not reset automatically.

    Example:
        >>> library = _FakeLibrary(db_type="SQLite")
        legacy setup noise
        >>> library.init_kwargs["db_type"]
        'SQLite'


    :ivar init_kwargs: Most recent constructor keyword dictionary, retained by reference.
    :ivar register_args: Most recent disk-root/scan-keyword pair, or None before any scan.
    """
    init_kwargs: dict[str, object] = {}
    register_args: tuple[Path, dict[str, object]] | None = None

    def __init__(self, **kwargs: object) -> None:
        """
        Store constructor keywords on the concrete class and emit setup chatter.

        Example:
            >>> _ = _FakeLibrary(create=True)
            legacy setup noise


        :param kwargs: Library options to record without opening a database.
        :return: None; the class-level latest-call observation is replaced.
        """
        type(self).init_kwargs = kwargs
        print("legacy setup noise")

    def __enter__(self) -> _FakeLibrary:
        """
        Return this fake for use in the adapter's with statement.

        Example:
            >>> library = _FakeLibrary()
            legacy setup noise
            >>> library.__enter__() is library
            True


        :return: This same fake instance, without acquiring resources.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """
        Leave the fake context without suppressing exceptions or clearing observations.

        Example:
            >>> library = _FakeLibrary()
            legacy setup noise
            >>> library.__exit__(None, None, None) is None
            True


        :param args: Exception-type/value/traceback arguments, ignored by the fake.
        :return: None, allowing any active exception to propagate.
        """
        return None

    def register_unmanaged_disk(self, disk_root: Path, **kwargs: object) -> SimpleNamespace:
        """
        Record a scan request and return a fixed one-file report without filesystem work.

        Example:
            >>> library = _FakeLibrary()
            legacy setup noise
            >>> report = library.register_unmanaged_disk(Path("drive"))
            legacy scan noise
            >>> report.to_dict()["scanned_files"], report.errors
            (1, [])


        :param disk_root: Drive path recorded without existence or content inspection.
        :param kwargs: Scan-policy options retained in the class-level observation.
        :return: Namespace with empty errors and a callable producing the report dict.
        """
        type(self).register_args = (disk_root, kwargs)
        print("legacy scan noise")
        return SimpleNamespace(errors=[], to_dict=lambda: {"scanned_files": 1, "errors": []})


def test_storage_audit_uses_local_sqlite_and_creates_database_by_default(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    """
    Assert exact SQLite constructor/scan defaults and JSON versus chatter routing.

    The fake records create=True but does not create a real SQLite database.

    Example:
        >>> test_storage_audit_uses_local_sqlite_and_creates_database_by_default(monkeypatch, tmp_path, capsys)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture replacing lazy Library construction with the fake.
    :param tmp_path: Isolated directory containing the existing empty drive fixture.
    :param capsys: Pytest stdout/stderr capture used to separate JSON from legacy messages.
    :return: None after routing, payload, and output assertions pass.
    """
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
    """
    Assert that a nonexistent drive produces the early path error and status two.

    Example:
        >>> test_storage_audit_rejects_a_missing_disk_root(tmp_path, capsys)  # doctest: +SKIP


    :param tmp_path: Isolated parent for nonexistent database and drive paths.
    :param capsys: Pytest capture used to inspect the stderr diagnostic.
    :return: None after checking refusal status and its path-error message.
    """
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
