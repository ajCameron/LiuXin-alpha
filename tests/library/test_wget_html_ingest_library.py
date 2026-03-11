from __future__ import annotations

from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult


def test_library_register_wget_html_store(db, monkeypatch) -> None:
    def _fake_run_wget(args, **kwargs):
        listing = "https://example.com/books/one.epub\n"
        return WgetResult(args=list(args), returncode=0, stdout=listing, stderr="")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    lib = Library(database=db, close_database_on_close=False)
    report = lib.register_wget_html_store(
        "https://example.com/",
        store_name="library_wget_web_mirror",
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []

