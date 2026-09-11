"""
Check Library's wget registration facade with injected diagnostics and a real catalogue.

No subprocess or remote byte transfer is exercised. Database ownership remains
with the fixture; the test does not assert Library cleanup behavior.
"""

from __future__ import annotations

from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def test_library_register_wget_html_store(db, monkeypatch) -> None:
    """
    Check Library delegates wget registration and returns one inserted file without report errors.

    The Library borrows the test database and receives fake subprocess output.

    Example:
        >>> test_library_register_wget_html_store(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned test catalogue receiving real Store/file/link writes.
    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    def _fake_run_wget(args, **kwargs):
        """
        Return a single ebook listing without invoking wget or emitting streamed callbacks.

        Example:
            >>> result = _fake_run_wget(command, line_callback=callback)  # doctest: +SKIP


        :param args: Invocation tokens copied to the fake result and any enclosing capture list.
        :param kwargs: Runner options; only explicitly described callback fields are used.
        :return: Successful fake result carrying the selected diagnostic text.
        """
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
