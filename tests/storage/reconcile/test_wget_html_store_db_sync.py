from __future__ import annotations

import json

from LiuXin_alpha.storage.reconcile import (
    register_wget_html_readonly_store_files,
    register_wget_html_readonly_with_database_path,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult


def _ok_wget_result(*, args: list[str], stdout: str = "", stderr: str = "") -> WgetResult:
    return WgetResult(args=list(args), returncode=0, stdout=stdout, stderr=stderr)


def test_register_wget_html_store_files_inserts_rows_and_tracks_policy(db, monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        listing = "\n".join(
            [
                "https://example.com/books/one.epub",
                "https://example.com/covers/one.jpg",
            ]
        )
        return _ok_wget_result(args=list(args), stdout=listing)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    report = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_web_mirror",
        max_http_requests_per_hour=60.0,
        refresh_storage_manager=False,
    )

    assert report.errors == []
    assert report.scanned_files == 2
    assert report.ebook_candidates == 1
    assert report.skipped_non_ebook_files == 1
    assert report.inserted_files == 1

    file_rows = db.search("files", "file_store_id", report.store_row_id)
    assert len(file_rows) == 1
    row = file_rows[0]
    assert row["file_storage_key"] == "books/one.epub"
    assert row["file_extension"] == "epub"

    store_row = db.get_row_from_id("stores", report.store_row_id)
    assert store_row is not None
    policy_raw = store_row["store_policy_json"]
    assert policy_raw
    policy = json.loads(str(policy_raw))
    assert policy["backend"] == "wget_html_readonly"
    assert policy["wget"]["max_http_requests_per_hour"] == 60.0
    assert int(store_row["store_supports_checksums"] or 0) == 0

    assert captured_args
    assert "--wait=60.000" in captured_args[0]

    if "file_store_links" in set(db.get_tables()):
        link_rows = db.search("file_store_links", "file_store_link_store_id", report.store_row_id)
        assert len(link_rows) == 1


def test_register_wget_html_store_files_is_idempotent(db, monkeypatch) -> None:
    def _fake_run_wget(args, **kwargs):
        listing = "https://example.com/books/one.epub\n"
        return _ok_wget_result(args=list(args), stdout=listing)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    first = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_web_mirror",
        refresh_storage_manager=False,
    )
    assert first.inserted_files == 1

    second = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_web_mirror",
        refresh_storage_manager=False,
    )
    assert second.inserted_files == 0
    assert second.updated_files == 0
    assert second.unchanged_files == 1


def test_wget_rate_limit_is_restored_when_storage_manager_bootstraps(db, monkeypatch) -> None:
    def _fake_run_wget(args, **kwargs):
        listing = "https://example.com/books/one.epub\n"
        return _ok_wget_result(args=list(args), stdout=listing)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_web_mirror_bootstrap",
        max_http_requests_per_hour=30.0,
        refresh_storage_manager=True,
    )

    assert db.storage is not None
    store = db.storage.get_store("wget_web_mirror_bootstrap")
    assert getattr(store.options, "max_http_requests_per_hour", None) == 30.0


def test_register_wget_html_with_database_path_helper(provision_test_database, driver_spec, monkeypatch) -> None:
    provisioned = provision_test_database("test_db_13")

    def _fake_run_wget(args, **kwargs):
        listing = "https://example.com/books/one.epub\n"
        return _ok_wget_result(args=list(args), stdout=listing)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    report = register_wget_html_readonly_with_database_path(
        database_path=provisioned.db_path,
        remote_url="https://example.com/",
        db_type=driver_spec.db_type,
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []


def test_register_wget_html_store_files_incremental_writes_during_crawl(db, monkeypatch) -> None:
    counts_during_run: list[int] = []

    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        assert callable(callback)
        callback("https://example.com/books/one.epub")
        counts_during_run.append(int(db.get_record_count("files")))
        callback("https://example.com/books/two.epub")
        counts_during_run.append(int(db.get_record_count("files")))
        return _ok_wget_result(args=list(args), stdout="")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    report = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_incremental",
        refresh_storage_manager=False,
        incremental_db_writes=True,
    )

    assert report.inserted_files == 2
    assert report.errors == []
    assert counts_during_run == [1, 2]


def test_register_wget_html_store_files_non_incremental_defers_writes(db, monkeypatch) -> None:
    counts_during_run: list[int] = []

    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        assert callable(callback)
        callback("https://example.com/books/one.epub")
        counts_during_run.append(int(db.get_record_count("files")))
        callback("https://example.com/books/two.epub")
        counts_during_run.append(int(db.get_record_count("files")))
        return _ok_wget_result(args=list(args), stdout="")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    report = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/",
        store_name="wget_non_incremental",
        refresh_storage_manager=False,
        incremental_db_writes=False,
    )

    assert report.inserted_files == 2
    assert report.errors == []
    assert counts_during_run == [0, 0]


def test_register_wget_html_store_files_tracks_crawler_observation_counts(db, monkeypatch) -> None:
    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        assert callable(callback)
        callback("https://example.com/books/index")
        callback("https://example.com/books/one.epub")
        callback("https://example.com/books/guide.html")
        callback("https://other.example.com/books/author.html")
        callback("https://example.com/books/one.epub")
        return _ok_wget_result(args=list(args), stdout="")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    report = register_wget_html_readonly_store_files(
        db,
        remote_url="https://example.com/books/",
        store_name="wget_observation_counts",
        refresh_storage_manager=False,
    )

    assert report.errors == []
    assert report.scanned_files == 2
    assert report.ebook_candidates == 2
    assert report.inserted_files == 2
    assert report.crawler_urls_observed == 4
    assert report.crawler_html_seen == 2
    assert report.crawler_book_like_found == 3
    assert report.crawler_html_rejected == 1
    assert report.crawler_rejection_counts == {
        "not_file_like": 1,
        "out_of_scope": 1,
    }
