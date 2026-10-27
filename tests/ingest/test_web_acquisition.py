"""Walk a real loopback HTTP site and publish its exact bytes into a real catalogue.

The payloads are small arbitrary byte fixtures; these tests establish acquisition
and identity behavior, not validity of their ebook filename extensions.
"""

from __future__ import annotations

import json
import os
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from LiuXin_alpha.ingest.web.application import WebIngestApplication
from LiuXin_alpha.ingest.web.models import WebIngestOptions
from LiuXin_alpha.ingest.web.repository import WebIngestRepository
from LiuXin_alpha.library.library import Library
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


@pytest.fixture
def site():
    """Record every request so registration cannot accidentally read ebook bodies."""
    state = {
        "requests": [],
        "books": {"a.epub": b"same book", "b.pdf": b"same book"},
        "index_status": 200,
        "robots": b"User-agent: *\nAllow: /\n",
        "overrides": {},
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.reply(head=False)

        def do_HEAD(self):
            self.reply(head=True)

        def reply(self, *, head):
            state["requests"].append(("HEAD" if head else "GET", self.path))
            status, mime = 200, "text/plain"
            if self.path == "/robots.txt":
                body = state["robots"]
            elif self.path == "/books/":
                status, mime = state["index_status"], "text/html"
                body = b'<a href="a.epub">A</a><a href="b.pdf">B</a>'
            elif self.path.startswith("/books/") and self.path[7:] in state["books"]:
                body = state["books"][self.path[7:]]
                mime = "application/octet-stream"
            else:
                status, body = 404, b"missing"
            headers = {
                "Content-Type": mime,
                "Content-Length": str(len(body)),
                "ETag": '"version-one"',
            }
            status, headers, body = state["overrides"].get(
                self.path, (status, headers, body)
            )
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if not head:
                self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state["root"] = f"http://127.0.0.1:{server.server_port}/books/"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def options(site, tmp_path, **overrides):
    return WebIngestOptions(
        **{
            "root_url": site["root"],
            "requests_per_hour": 0,
            "extensions": ("epub", "pdf"),
            "state_directory": str(tmp_path / "staging"),
            **overrides,
        }
    )


def library(db, tmp_path):
    destination = FilesystemStore(tmp_path / "retained")
    manager = StorageManager(
        db=db, stores=(destination,), default_store_ref=destination.store_ref
    )
    return Library(database=db, storage=manager)


def test_register_only_never_fetches_ebook_payloads_or_creates_assets(
    db, site, tmp_path
):
    before = db.macros.get_rows("digital_assets")
    report = WebIngestApplication(Library(database=db), options(site, tmp_path)).run()
    assert report.status == "complete", report.errors
    assert report.registered_files == 2 and report.downloaded_files == 0
    assert site["requests"] == [("GET", "/robots.txt"), ("GET", "/books/")]
    assert db.macros.get_rows("digital_assets") == before
    assert not (tmp_path / "staging").exists()
    candidates = WebIngestRepository(db).files(site["root"])
    assert len(candidates) == 2
    assert all(row["remote_file_referrer"] == site["root"] for row in candidates)
    assert all(row["remote_file_latest_capture_json"] is None for row in candidates)


def test_registered_candidates_can_be_downloaded_later_without_recrawling(
    db, site, tmp_path
):
    service = library(db, tmp_path)
    registered = WebIngestApplication(service, options(site, tmp_path)).run()
    assert registered.status == "complete", registered.errors
    observed_before = [
        row["remote_file_last_seen"]
        for row in WebIngestRepository(db).files(site["root"])
    ]
    site["index_status"] = 503
    site["requests"].clear()
    report = WebIngestApplication(
        service, options(site, tmp_path, mode="download", registered_only=True)
    ).run()
    assert report.status == "complete", report.errors
    assert report.downloaded_files == 2
    assert report.assets_created == 1 and report.replicas_created == 1
    assert ("GET", "/books/") not in site["requests"]
    for receipt in report.files:
        assert service.storage.read_file(receipt["digital_asset_id"]) == b"same book"
        assert receipt["source_url"].startswith(site["root"])
        assert receipt["etag"] == '"version-one"'
    candidates = WebIngestRepository(db).files(site["root"])
    assert [row["remote_file_last_seen"] for row in candidates] == observed_before
    assert all(
        json.loads(row["remote_file_latest_capture_json"])["verified"]
        for row in candidates
    )
    assert not list((tmp_path / "staging").rglob("*.part"))
    assert not list((tmp_path / "staging").rglob("*.complete"))


def test_direct_book_url_can_be_registered_then_acquired(db, site, tmp_path):
    service = library(db, tmp_path)
    root = site["root"] + "a.epub?edition=1"
    site["overrides"]["/books/a.epub?edition=1"] = (
        200,
        {"Content-Length": "9"},
        b"same book",
    )
    registered = WebIngestApplication(
        service, options(site, tmp_path, root_url=root)
    ).run()
    assert registered.status == "complete" and registered.registered_files == 1
    assert not site["requests"]
    acquired = WebIngestApplication(
        service,
        options(site, tmp_path, root_url=root, mode="download", registered_only=True),
    ).run()
    assert acquired.status == "complete", acquired.errors
    assert acquired.files[0]["source_url"] == root
    assert (
        service.storage.read_file(acquired.files[0]["digital_asset_id"]) == b"same book"
    )
    assert site["requests"] == [
        ("GET", "/robots.txt"),
        ("GET", "/books/a.epub?edition=1"),
    ]


def test_default_selection_walks_html_pages_without_retaining_them(db, site, tmp_path):
    site["overrides"]["/books/"] = (
        200,
        {"Content-Type": "text/html"},
        b'<a href="index.html">More</a>',
    )
    site["overrides"]["/books/index.html"] = (
        200,
        {"Content-Type": "text/html"},
        b'<a href="a.epub">Book</a>',
    )
    service = library(db, tmp_path)
    report = WebIngestApplication(
        service, options(site, tmp_path, mode="download", extensions=())
    ).run()
    assert report.status == "complete", report.errors
    assert report.registered_files == report.downloaded_files == 1
    assert report.files[0]["source_url"].endswith("a.epub")
    assert ("GET", "/books/index.html") in site["requests"]


def test_resume_skips_completed_payloads_and_new_runs_preserve_changed_versions(
    db, site, tmp_path
):
    service = library(db, tmp_path)
    controls = options(site, tmp_path, mode="download", include=("*/a.epub",))
    first = WebIngestApplication(service, controls).run()
    assert first.status == "complete", first.errors
    site["requests"].clear()
    resumed = WebIngestApplication(service, controls, resume=first.run_id).run()
    assert resumed.status == "complete", resumed.errors
    assert resumed.already_completed == 1
    assert not site["requests"]
    site["books"]["a.epub"] = b"changed edition"
    changed = WebIngestApplication(service, controls).run()
    assert changed.status == "complete", changed.errors
    assert changed.files[0]["digital_asset_id"] != first.files[0]["digital_asset_id"]
    assert service.storage.read_file(first.files[0]["digital_asset_id"]) == b"same book"
    assert (
        service.storage.read_file(changed.files[0]["digital_asset_id"])
        == b"changed edition"
    )


def test_low_space_stops_before_any_ebook_transfer(db, site, tmp_path):
    service = library(db, tmp_path)
    report = WebIngestApplication(
        service, options(site, tmp_path, mode="download", min_free_bytes=10**18)
    ).run()
    assert report.status == "limited", report.errors
    assert not any(
        path.endswith((".epub", ".pdf")) for method, path in site["requests"]
    )
    assert report.downloaded_files == 0
    assert not list((tmp_path / "staging").rglob("*.part"))


def test_candidate_ceiling_still_acquires_the_selected_subset(db, site, tmp_path):
    service = library(db, tmp_path)
    report = WebIngestApplication(
        service, options(site, tmp_path, mode="download", max_files=1)
    ).run()
    assert report.status == "limited", report.errors
    assert report.registered_files == report.downloaded_files == 1
    assert [
        path
        for method, path in site["requests"]
        if method == "GET" and path.endswith((".epub", ".pdf"))
    ] == ["/books/a.epub"]


@pytest.mark.parametrize("mode", ["register", "download"])
def test_real_wget_registration_and_acquisition(db, site, tmp_path, mode):
    if not shutil.which("wget"):
        if os.environ.get("LIUXIN_REQUIRE_WGET_TESTS") == "1":
            pytest.fail("required wget integration is unavailable")
        pytest.skip("wget is unavailable")
    service = Library(database=db) if mode == "register" else library(db, tmp_path)
    report = WebIngestApplication(
        service, options(site, tmp_path, crawler="wget", mode=mode)
    ).run()
    assert report.status == "complete", report.errors
    assert report.registered_files == 2
    payloads = [
        path
        for method, path in site["requests"]
        if method == "GET" and path.endswith((".epub", ".pdf"))
    ]
    if mode == "register":
        assert not payloads
        assert not (tmp_path / "staging").exists()
    else:
        assert report.downloaded_files == 2 and len(payloads) == 2


@pytest.mark.parametrize(
    "case", ["unknown-length-too-large", "truncated", "redirect", "robots"]
)
def test_bad_or_disallowed_transfers_never_become_retained_assets(
    db, site, tmp_path, case
):
    controls = {"mode": "download", "include": ("*/a.epub",)}
    if case == "unknown-length-too-large":
        site["overrides"]["/books/a.epub"] = (
            200,
            {"Content-Type": "application/octet-stream"},
            b"too large",
        )
        controls["max_file_bytes"] = 3
    elif case == "truncated":
        site["overrides"]["/books/a.epub"] = (200, {"Content-Length": "200"}, b"short")
    elif case == "redirect":
        site["overrides"]["/books/a.epub"] = (
            302,
            {"Location": "/outside/secret.epub"},
            b"",
        )
    else:
        site["robots"] = b"User-agent: *\nDisallow: /books/a.epub\n"
    service = library(db, tmp_path)
    before = db.macros.get_rows("digital_assets")
    report = WebIngestApplication(service, options(site, tmp_path, **controls)).run()
    assert report.status in {"partial", "limited"}, report.to_dict()
    assert report.registered_files == 1
    assert report.downloaded_files == 0
    assert db.macros.get_rows("digital_assets") == before
    assert not list((tmp_path / "staging").rglob("*.part"))
    assert not list((tmp_path / "staging").rglob("*.complete"))
    assert ("GET", "/outside/secret.epub") not in site["requests"]
    if case == "robots":
        assert ("GET", "/books/a.epub") not in site["requests"]


def test_publication_before_receipt_can_resume_in_a_new_manager(
    db, site, tmp_path, monkeypatch
):
    service = library(db, tmp_path)
    controls = options(site, tmp_path, mode="download", include=("*/a.epub",))
    original = WebIngestRepository.save_item

    def fail_receipt(self, *args, **kwargs):
        if kwargs["state"] == "downloaded":
            raise RuntimeError("injected failure after publication")
        return original(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(WebIngestRepository, "save_item", fail_receipt)
        first = WebIngestApplication(service, controls).run()
    assert first.status == "partial", first.errors
    assert len(list((tmp_path / "staging").rglob("*.complete"))) == 1
    site["requests"].clear()
    destination = service.storage.get_store(service.storage.get_default_store_ref())
    fresh_manager = StorageManager(
        db=db, stores=(destination,), default_store_ref=destination.store_ref
    )
    resumed = WebIngestApplication(
        Library(database=db, storage=fresh_manager), controls, resume=first.run_id
    ).run()
    assert resumed.status == "complete", resumed.errors
    assert not site["requests"]
    assert len(db.macros.get_rows("digital_assets")) == 1
    assert len(db.macros.get_rows("asset_replicas")) == 1
    assert not list((tmp_path / "staging").rglob("*.complete"))


def test_interrupted_transfer_restarts_without_mixing_remote_versions(
    db, site, tmp_path
):
    service = library(db, tmp_path)
    controls = options(site, tmp_path, mode="download", include=("*/a.epub",))
    site["overrides"]["/books/a.epub"] = (200, {"Content-Length": "100"}, b"old prefix")
    first = WebIngestApplication(service, controls).run()
    assert first.status == "partial", first.errors
    assert first.downloaded_bytes == len(b"old prefix")
    assert not list((tmp_path / "staging").rglob("*.part"))
    site["overrides"].clear()
    site["books"]["a.epub"] = b"new complete edition"
    resumed = WebIngestApplication(service, controls, resume=first.run_id).run()
    assert resumed.status == "complete", resumed.errors
    assert (
        service.storage.read_file(resumed.files[0]["digital_asset_id"])
        == b"new complete edition"
    )
    assert resumed.downloaded_bytes == len(b"old prefixnew complete edition")


def test_dry_run_does_not_add_catalogue_tables_or_observations(db, site, tmp_path):
    tables = set(db.get_tables())
    report = WebIngestApplication(
        Library(database=db), options(site, tmp_path, dry_run=True)
    ).run()
    assert report.status == "complete" and report.run_id is None
    assert report.registered_files == 2
    assert set(db.get_tables()) == tables
    assert not (tmp_path / "staging").exists()


def test_unknown_length_total_budget_is_durable_and_stops_resume_before_get(
    db, site, tmp_path
):
    site["overrides"]["/books/a.epub"] = (200, {}, b"longer than budget")
    service = library(db, tmp_path)
    controls = options(
        site, tmp_path, mode="download", include=("*/a.epub",), max_total_bytes=3
    )
    first = WebIngestApplication(service, controls).run()
    assert first.status == "limited", first.errors
    assert first.downloaded_bytes == 4  # one byte of EOF lookahead, never retained
    assert first.downloaded_files == 0
    site["requests"].clear()
    resumed = WebIngestApplication(service, controls, resume=first.run_id).run()
    assert resumed.status == "limited", resumed.errors
    assert resumed.downloaded_bytes == 4
    assert not site["requests"]
    assert not list((tmp_path / "staging").rglob("*.part"))


def test_cleanup_failure_does_not_lose_capture_or_force_a_redownload(
    db, site, tmp_path, monkeypatch
):
    from LiuXin_alpha.ingest.web.staging import WebStaging

    service = library(db, tmp_path)
    controls = options(site, tmp_path, mode="download", include=("*/a.epub",))
    original = WebStaging.discard

    def fail_complete_cleanup(self, remote_id, *, partial=False):
        if not partial and self.path(remote_id).exists():
            raise OSError("injected complete-file cleanup failure")
        return original(self, remote_id, partial=partial)

    with monkeypatch.context() as patch:
        patch.setattr(WebStaging, "discard", fail_complete_cleanup)
        first = WebIngestApplication(service, controls).run()
    assert first.status == "partial", first.errors
    saved = WebIngestRepository(db).items(first.run_id)
    assert saved[0]["web_ingest_item_state"] == "downloaded"
    site["requests"].clear()
    resumed = WebIngestApplication(service, controls, resume=first.run_id).run()
    assert resumed.status == "complete", resumed.errors
    assert resumed.already_completed == 1
    assert not site["requests"]
    assert not list((tmp_path / "staging").rglob("*.complete"))


def test_partial_crawl_retains_observations_and_retries_discovery(db, site, tmp_path):
    site["index_status"] = 503
    service = Library(database=db)
    controls = options(site, tmp_path)
    first = WebIngestApplication(service, controls).run()
    assert first.status == "partial", first.errors
    assert not WebIngestRepository(db).run(first.run_id)[
        "web_ingest_run_discovery_complete"
    ]
    site["index_status"] = 200
    resumed = WebIngestApplication(service, controls, resume=first.run_id).run()
    assert resumed.status == "complete" and resumed.registered_files == 2


def test_cli_initialization_registration_download_and_status_round_trip(
    site, tmp_path, capsys, monkeypatch
):
    from LiuXin_alpha.surfaces.cli.app import main as cli_main

    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[2] / "src"))
    root = tmp_path / "system"
    assert cli_main(["init", str(root), "--compact"]) == 0
    capsys.readouterr()
    common = ["--system-root", str(root), "--compact", "--wait-timeout", "30"]
    assert (
        cli_main(["ingest", "web", site["root"], "--requests-per-hour", "0", *common])
        == 0
    )
    registered = json.loads(capsys.readouterr().out)["execution"]["result"]
    assert registered["registered_files"] == 2
    assert (
        cli_main(
            [
                "ingest",
                "web",
                site["root"],
                "--download",
                "--registered-only",
                "--requests-per-hour",
                "0",
                *common,
            ]
        )
        == 0
    )
    downloaded = json.loads(capsys.readouterr().out)["execution"]["result"]
    assert downloaded["downloaded_files"] == 2
    site["requests"].clear()
    assert cli_main(["ingest", "web", site["root"], "--list", *common]) == 0
    listing = json.loads(capsys.readouterr().out)
    files = listing["files"]
    assert len(files) == 2 and all(row["last_capture"] for row in files)
    assert {row["run_id"] for row in listing["runs"]} == {
        registered["run_id"],
        downloaded["run_id"],
    }
    assert cli_main(["ingest", "web", "--status", downloaded["run_id"], *common]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "complete"
    assert not site["requests"]
