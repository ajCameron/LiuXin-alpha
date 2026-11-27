"""Exercise disk registration against native catalogues without legacy files rows."""

import hashlib

import pytest

from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.ingest.native_disk import register_native_disk


@pytest.mark.parametrize("backend", ["SQLite", "SQLite_apsw"])
def test_disk_scan_reuses_assets_and_replica_claims(tmp_path, backend):
    disk = tmp_path / "disk"
    disk.mkdir()
    (disk / "first.epub").write_bytes(b"same content")
    (disk / "second.epub").write_bytes(b"same content")
    (disk / "ignore.bin").write_bytes(b"ignore")
    with create_core(
        database_path=tmp_path / "catalog.sqlite",
        db_type=backend,
        create=True,
        start_jobs=False,
        enable_maintenance=False,
    ) as core:
        assert not list(core.database.get_all_rows("files"))
        first = register_native_disk(core.library, disk)
        assert not first.errors
        assert first.inserted_assets == 1 and first.inserted_replicas == 2
        second = register_native_disk(core.library, disk)
        assert second.inserted_assets == second.inserted_replicas == 0
        assert second.reused_replicas == 2
        assets = list(core.library.storage.iter_digital_asset_records())
        assert any(
            digest.algorithm == "sha256"
            and digest.value == hashlib.sha256(b"same content").hexdigest()
            for asset in assets
            for digest in asset.digests
        )
        (disk / "first.epub").write_bytes(b"changed")
        changed = register_native_disk(core.library, disk)
        assert changed.errors and "already claimed" in changed.errors[0]


def test_conversion_publication_cancellation_and_competing_output(tmp_path):
    from pathlib import Path

    from LiuXin_alpha.file_formats.conversion.publication import publish_conversion

    output = tmp_path / "book.epub"

    def convert(staging):
        Path(staging).write_bytes(b"converted")
        return {"format": "epub"}

    checks = []

    def cancelled():
        checks.append(True)
        if len(checks) == 2:
            raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        publish_conversion(str(output), convert, checkpoint=cancelled)
    assert not output.exists()
    assert not list(tmp_path.glob(".liuxin-conversion-*"))
    assert publish_conversion(str(output), convert)["publication"] == "committed"
    with pytest.raises(FileExistsError):
        publish_conversion(str(output), convert)
    assert output.read_bytes() == b"converted"


def test_directory_conversion_rejected_without_masking_error_or_leaking_staging(
    tmp_path,
):
    from pathlib import Path

    from LiuXin_alpha.file_formats.conversion.publication import publish_conversion

    def convert(staging):
        path = Path(staging)
        path.unlink()
        path.mkdir()
        (path / "partial.html").write_bytes(b"incomplete")
        return {}

    with pytest.raises(ValueError, match="regular staged file"):
        publish_conversion(str(tmp_path / "output.html"), convert)
    assert not (tmp_path / "output.html").exists()
    assert not list(tmp_path.glob(".liuxin-conversion-*"))


def test_conversion_competing_publication_does_not_overwrite(tmp_path):
    from pathlib import Path

    from LiuXin_alpha.file_formats.conversion.publication import publish_conversion

    output = tmp_path / "book.epub"

    def convert(staging):
        Path(staging).write_bytes(b"converted")
        # The destination was unused at entry, then another publisher wins.
        output.write_bytes(b"other publisher")
        return {}

    with pytest.raises(FileExistsError):
        publish_conversion(str(output), convert)
    assert output.read_bytes() == b"other publisher"
    assert not list(tmp_path.glob(".liuxin-conversion-*"))


def test_conversion_failure_after_publication_retains_output(tmp_path, monkeypatch):
    import os
    from pathlib import Path

    from LiuXin_alpha.file_formats.conversion.publication import publish_conversion

    if os.name != "posix":
        pytest.skip("Directory fsync is a POSIX publication boundary")
    output = tmp_path / "book.epub"
    real_fsync = os.fsync

    def fail_directory_sync(descriptor):
        if output.exists():
            raise OSError("directory sync failed after publication")
        real_fsync(descriptor)

    def convert(staging):
        Path(staging).write_bytes(b"completed output")
        return {}

    monkeypatch.setattr(os, "fsync", fail_directory_sync)
    with pytest.raises(OSError, match="after publication"):
        publish_conversion(str(output), convert)
    assert output.read_bytes() == b"completed output"
    assert not list(tmp_path.glob(".liuxin-conversion-*"))
    with pytest.raises(FileExistsError):
        publish_conversion(str(output), convert)
