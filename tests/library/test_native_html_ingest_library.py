"""
Check Library's native-HTML registration facade using a temporary catalogue.

HTML page fetching is injected; this does not test live discovery, byte copying,
Library closure, or robots availability. The database remains fixture-owned.
"""

from __future__ import annotations

from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    native_html_storage_backend as backend_module,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _html_result(url: str, body: str) -> object:
    """
    Build an untruncated successful UTF-8 HTML fetch record without performing HTTP.

    Example:
        >>> _html_result('https://example.test/', '<p>page</p>').status
        200


    :param url: Address used as both requested and final URL.
    :param body: HTML text encoded as UTF-8 bytes.
    :return: Injected fetch record with status 200 and an HTML content type.
    """
    return backend_module._FetchResult(
        requested_url=url,
        final_url=url,
        status=200,
        content_type="text/html; charset=utf-8",
        body=body.encode("utf-8"),
        charset="utf-8",
    )


def test_library_register_native_html_store(db, monkeypatch) -> None:
    """
    Check Library delegates native registration and returns a report with one insert and no errors.

    The Library borrows the fixture database. Page results are injected; separate robots requests
    are not replaced.

    Example:
        >>> test_library_register_native_html_store(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned test catalogue receiving real Store/file/link writes.
    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            "<html><body><a href=\"files/one.epub\">One</a></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
        """
        Return the response assigned to this exact URL in the enclosing fixture mapping.

        Example:
            >>> fetched = _fake_fetch(store, store.url)  # doctest: +SKIP


        :param self: Injected backend instance, unused by the mapping lookup.
        :param url: Exact response-map key; absent entries raise KeyError.
        :return: Existing fetch record retained in the enclosing response mapping.
        """
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    lib = Library(database=db, close_database_on_close=False)
    report = lib.register_native_html_store(
        "https://example.com/library/",
        store_name="library_native_web_mirror",
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []
