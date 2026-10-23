"""Exercise remote observations and restart fencing against a real catalogue."""

import json

import pytest

from LiuXin_alpha.ingest.web.models import WebIngestOptions, WebRunBusyError
from LiuXin_alpha.ingest.web.repository import WebIngestRepository


def test_remote_observations_persist_without_creating_assets(db):
    before = db.macros.get_rows("digital_assets")
    repository = WebIngestRepository(db)
    first = repository.observe(
        "https://books.test/",
        "https://books.test/a.epub",
        "https://books.test/index.html",
    )
    repeated = repository.observe("https://books.test/", "https://books.test/a.epub")
    assert first["remote_file_id"] == repeated["remote_file_id"]
    assert repeated["remote_file_first_seen"] == first["remote_file_first_seen"]
    assert repeated["remote_file_latest_capture_json"] is None
    assert len(WebIngestRepository(db).files("https://books.test/")) == 1
    assert db.macros.get_rows("digital_assets") == before


def test_saved_run_claims_fence_stale_workers_and_preserve_captures(db):
    repository = WebIngestRepository(db)
    options = WebIngestOptions("https://books.test/", mode="download")
    run = repository.new_run(options)
    owner = repository.claim(run, lease_ms=60_000)
    with pytest.raises(WebRunBusyError):
        repository.claim(run, lease_ms=60_000)
    candidate = repository.observe(options.root_url, options.root_url + "a.epub")
    identity = candidate["remote_file_id"]
    receipt = {"digital_asset_id": 42, "sha256": "a" * 64, "size_bytes": 7}
    repository.save_item(run, owner, identity, state="downloaded", receipt=receipt)
    repository.finish(run, owner, {"status": "complete"})
    new_owner = repository.claim(run, lease_ms=60_000)
    with pytest.raises(WebRunBusyError):
        repository.save_item(run, owner, identity, state="failed", receipt={})
    assert (
        json.loads(repository.item(run, identity)["web_ingest_item_receipt_json"])
        == receipt
    )
    assert (
        json.loads(
            repository.files(options.root_url)[0]["remote_file_latest_capture_json"]
        )
        == receipt
    )
    repository.finish(run, new_owner, {"status": "complete"})


def test_reading_candidates_does_not_create_tables(db):
    before = set(db.get_tables())
    assert WebIngestRepository(db, create=False).files("https://books.test/") == []
    assert WebIngestRepository(db, create=False).runs("https://books.test/") == []
    assert set(db.get_tables()) == before


def test_run_identity_is_discoverable_before_any_result_is_written(db):
    repository = WebIngestRepository(db)
    options = WebIngestOptions("https://books.test/")
    run = repository.new_run(options)
    repository.claim(run, lease_ms=60_000)
    saved = WebIngestRepository(db, create=False).runs(options.root_url)
    assert len(saved) == 1
    assert saved[0]["web_ingest_run_id"] == run
    assert saved[0]["web_ingest_run_status"] == "running"
    assert saved[0]["web_ingest_run_report_json"] is None
    assert repository.runs("https://different.test/") == []


def test_expired_run_owner_cannot_renew_or_change_receipts(db, monkeypatch):
    import LiuXin_alpha.ingest.web.repository as module

    monkeypatch.setattr(module, "now_ms", lambda: 1000)
    repository = WebIngestRepository(db)
    run = repository.new_run(WebIngestOptions("https://books.test/"))
    owner = repository.claim(run, lease_ms=10)
    monkeypatch.setattr(module, "now_ms", lambda: 1011)
    with pytest.raises(WebRunBusyError):
        repository.heartbeat(run, owner, lease_ms=1000)
    new_owner = repository.claim(run, lease_ms=1000)
    assert new_owner != owner
    with pytest.raises(WebRunBusyError):
        repository.charge_bytes(run, owner, 10, 100)
    with pytest.raises(WebRunBusyError):
        repository.finish(run, owner, {"status": "failed"})
    assert repository.run(run)["web_ingest_run_bytes_received"] == 0


@pytest.mark.parametrize("case", ["unmarked", "public", "symlink", "mismatched-marker"])
def test_staging_refuses_borrowed_or_unsafe_directories(tmp_path, case):
    import os
    from uuid import uuid4

    from LiuXin_alpha.ingest.web.staging import WebStaging

    if os.name != "posix":
        pytest.skip("POSIX directory permissions and symlink fixture")
    run = str(uuid4())
    root = tmp_path / run
    borrowed = tmp_path / "borrowed"
    borrowed.mkdir()
    (borrowed / "keep").write_text("untouched")
    if case == "symlink":
        root.symlink_to(borrowed, target_is_directory=True)
    else:
        root.mkdir(mode=0o700)
        if case == "public":
            root.chmod(0o755)
        elif case == "mismatched-marker":
            (root / "owner.json").write_text('{"run_id": "someone else"}')
    with pytest.raises(ValueError):
        WebStaging(tmp_path, run)
    assert (borrowed / "keep").read_text() == "untouched"
