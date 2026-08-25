from __future__ import annotations

import json
from uuid import UUID

import pytest

from LiuXin_alpha.storage.api import StorageInvalidAddress
from LiuXin_alpha.storage.reconcile import (
    ensure_rclone_http_readonly_store,
    register_rclone_http_readonly_store_files,
    register_rclone_http_readonly_with_database_path,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as backend_module,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _extract_tpslimit(extra_args: tuple[str, ...]) -> float | None:
    for arg in extra_args:
        if arg.startswith("--tpslimit="):
            try:
                return float(arg.split("=", 1)[1])
            except Exception:
                return None
    return None


def _fake_rclone_json_from_listing(
    listing: list[dict[str, object]],
    *,
    captured_extra_args: list[tuple[str, ...]] | None = None,
):
    def _fake_run_rclone_json(args, **kwargs):
        if captured_extra_args is not None:
            captured_extra_args.append(tuple(kwargs.get("extra_args", ())))

        if list(args[:3]) == ["lsjson", "-R", "--files-only"]:
            return [dict(entry) for entry in listing]

        if list(args[:2]) == ["lsjson", "--stat"] and len(args) >= 3:
            target = str(args[2])
            rel = target.split(":", 1)[1] if ":" in target else target
            rel = rel.lstrip("/")
            for entry in listing:
                entry_path = str(entry.get("Path") or entry.get("Name") or "").lstrip("/")
                if entry_path == rel:
                    return dict(entry)
            return {}

        return []

    return _fake_run_rclone_json


def test_register_rclone_http_store_files_inserts_rows_and_tracks_policy(db, monkeypatch) -> None:
    ensure_surface_asset_tables(db)
    captured_extra_args: list[tuple[str, ...]] = []
    listing = [
        {"Path": "books/one.epub", "Name": "one.epub", "Size": 11, "ModTime": "2025-01-02T03:04:05Z"},
        {"Path": "covers/one.jpg", "Name": "one.jpg", "Size": 7, "ModTime": "2025-01-02T03:04:05Z"},
    ]

    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        _fake_rclone_json_from_listing(listing, captured_extra_args=captured_extra_args),
    )

    report = register_rclone_http_readonly_store_files(
        db,
        remote_url="remote:",
        store_name="web_mirror",
        max_http_requests_per_hour=10.0,
        enforce_global_rate_limit=False,
        max_inventory_entries=23,
        max_json_token_chars=4096,
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
    assert row["file_size_bytes"] == 11
    assert row["file_extension"] == "epub"

    store_row = db.get_row_from_id("stores", report.store_row_id)
    assert store_row is not None
    policy_raw = store_row["store_policy_json"]
    assert policy_raw
    policy = json.loads(str(policy_raw))
    assert policy["backend"] == "rclone_http_readonly"
    assert policy["rclone"]["max_http_requests_per_hour"] == 10.0
    assert policy["rclone"]["max_inventory_entries"] == 23
    assert policy["rclone"]["max_json_token_chars"] == 4096
    assert int(store_row["store_supports_checksums"] or 0) == 1

    # Ensure the configured rate limit is translated into rclone TPS flags.
    tpslimit = _extract_tpslimit(captured_extra_args[0])
    assert tpslimit is not None
    assert abs(tpslimit - (10.0 / 3600.0)) < 1e-8

    if "file_store_links" in set(db.get_tables()):
        link_rows = db.search("file_store_links", "file_store_link_store_id", report.store_row_id)
        assert len(link_rows) == 1


def test_register_rclone_http_store_files_is_idempotent_and_updates(db, monkeypatch) -> None:
    ensure_surface_asset_tables(db)
    listing = [
        {"Path": "books/one.epub", "Name": "one.epub", "Size": 11, "ModTime": "2025-01-02T03:04:05Z"},
    ]

    monkeypatch.setattr(backend_module, "run_rclone_json", _fake_rclone_json_from_listing(listing))

    first = register_rclone_http_readonly_store_files(
        db,
        remote_url="remote:",
        store_name="web_mirror",
        enforce_global_rate_limit=False,
        refresh_storage_manager=False,
    )
    assert first.inserted_files == 1

    second = register_rclone_http_readonly_store_files(
        db,
        remote_url="remote:",
        store_name="web_mirror",
        enforce_global_rate_limit=False,
        refresh_storage_manager=False,
    )
    assert second.inserted_files == 0
    assert second.updated_files == 0
    assert second.unchanged_files == 1

    listing[0] = {
        "Path": "books/one.epub",
        "Name": "one.epub",
        "Size": 33,
        "ModTime": "2025-01-05T00:00:00Z",
    }
    third = register_rclone_http_readonly_store_files(
        db,
        remote_url="remote:",
        store_name="web_mirror",
        enforce_global_rate_limit=False,
        refresh_storage_manager=False,
    )
    assert third.updated_files >= 1

    file_rows = db.search("files", "file_store_id", third.store_row_id)
    assert len(file_rows) == 1
    assert file_rows[0]["file_size_bytes"] == 33


def test_rclone_rate_limit_is_restored_when_storage_manager_bootstraps(db, monkeypatch) -> None:
    ensure_surface_asset_tables(db)
    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        _fake_rclone_json_from_listing(
            [{"Path": "books/one.epub", "Name": "one.epub", "Size": 5, "ModTime": "2025-01-02T03:04:05Z"}]
        ),
    )

    register_rclone_http_readonly_store_files(
        db,
        remote_url="remote:",
        store_name="web_mirror_bootstrap",
        max_http_requests_per_hour=5.0,
        enforce_global_rate_limit=False,
        refresh_storage_manager=True,
    )

    assert db.storage is not None
    rows = db.search("stores", "store_name", "web_mirror_bootstrap")
    assert len(rows) == 1
    store = db.storage.get_store(UUID(str(rows[0]["store_uuid"])))
    assert getattr(store.options, "max_http_requests_per_hour", None) == 5.0


def test_register_rclone_http_with_database_path_helper(provision_test_database, driver_spec, monkeypatch) -> None:
    from LiuXin_alpha.databases.database import Database

    provisioned = provision_test_database("test_db_13")
    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as seeded:
        ensure_surface_asset_tables(seeded)

    monkeypatch.setattr(
        backend_module,
        "run_rclone_json",
        _fake_rclone_json_from_listing(
            [{"Path": "books/one.epub", "Name": "one.epub", "Size": 5, "ModTime": "2025-01-02T03:04:05Z"}]
        ),
    )

    report = register_rclone_http_readonly_with_database_path(
        database_path=provisioned.db_path,
        remote_url="remote:",
        db_type=driver_spec.db_type,
        enforce_global_rate_limit=False,
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []


@pytest.mark.parametrize(
    "invalid_root",
    [
        "remote:\ud800",
        "remote:path\nsecond-command",
        ':s3,secret_access_key="do-not-store":bucket',
    ],
)
def test_rclone_invalid_or_secret_roots_create_no_database_rows(
    db,
    invalid_root: str,
) -> None:
    ensure_surface_asset_tables(db)
    before = len(db.get_all_rows("stores", iterator_return=False) or ())

    with pytest.raises(StorageInvalidAddress) as raised:
        ensure_rclone_http_readonly_store(db, invalid_root)

    assert "do-not-store" not in str(raised.value)
    after = len(db.get_all_rows("stores", iterator_return=False) or ())
    assert after == before
