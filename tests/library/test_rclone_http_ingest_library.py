"""
Check Library delegation into rclone file registration using a substituted runner.

The provisioned catalogue receives real writes, but inventory bytes are canned and
manager refresh is disabled. The test does not establish remote service behavior.

Example:
    >>> test_library_register_rclone_http_store(db, monkeypatch)  # doctest: +SKIP
"""

from __future__ import annotations

from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def test_library_register_rclone_http_store(db, monkeypatch) -> None:
    """
    Delegate Library remote registration to the catalogue workflow with an injected one-entry
    inventory.

    Borrow the fixture database without transferring close ownership and disable manager refresh.
    Assert the insertion receipt and empty errors rather than a live rclone fetch.

    Example:
        >>> test_library_register_rclone_http_store(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned catalogue fixture receiving actual legacy Store/file/link writes.
    :param monkeypatch: Pytest fixture restoring injected process/backend/helper seams after the test.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    def _fake_run_rclone_json(args, **kwargs):
        """
        Return a fresh one-file inventory only for the recursive files-only lsjson prefix.

        Ignore all other runner options and return an empty list for other commands; no subprocess
        is launched.

        Example:
            >>> rows = _fake_run_rclone_json(["lsjson", "-R", "--files-only"])  # doctest: +SKIP


        :param args: Command sequence inspected for the listing prefix.
        :param kwargs: Accepted runner options, ignored by this fixture.
        :return: Fresh canned inventory or an empty fallback list.
        """
        if list(args[:3]) == ["lsjson", "-R", "--files-only"]:
            return [{"Path": "books/one.epub", "Name": "one.epub", "Size": 11, "ModTime": "2025-01-02T03:04:05Z"}]
        return []

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_run_rclone_json)

    lib = Library(database=db, close_database_on_close=False)
    report = lib.register_rclone_http_store(
        "remote:",
        store_name="library_web_mirror",
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []
