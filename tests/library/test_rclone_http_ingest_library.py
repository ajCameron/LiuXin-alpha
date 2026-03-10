from __future__ import annotations

from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)


def test_library_register_rclone_http_store(db, monkeypatch) -> None:
    def _fake_run_rclone_json(args, **kwargs):
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

