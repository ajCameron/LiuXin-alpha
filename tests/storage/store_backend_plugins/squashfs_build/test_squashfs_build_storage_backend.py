from __future__ import annotations

import hashlib
import pathlib
import subprocess

import pytest

from LiuXin_alpha.storage.api import StoreSpec
from LiuXin_alpha.storage.errors import SquashfsBuildImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import SquashfsBuildStorageBackend
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import SquashfsReadOnlyStorageBackend
from LiuXin_alpha.storage.store_manager import StorageManager


def test_squashfs_build_designate_and_iter_locations(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "source.epub"
    source.write_bytes(b"EPUB-DATA")
    archive = tmp_path / "backup.squashfs"
    stage = tmp_path / "stage"

    store = SquashfsBuildStorageBackend(url=str(archive), staging_root=str(stage))
    loc = store.designate_file(source, archive_path="books/source.epub")

    assert loc.file_url == str(archive.resolve()) + "/books/source.epub"
    assert loc.as_bytes() == b"EPUB-DATA"
    assert store.exists("books/source.epub") is True
    assert store.file_size("books/source.epub") == len(b"EPUB-DATA")
    assert [l.file_url for l in store.iter_locations()] == [str(archive.resolve()) + "/books/source.epub"]


def test_squashfs_build_implicit_write_uses_hash_layout_and_is_deduplicating(tmp_path: pathlib.Path) -> None:
    archive = tmp_path / "backup.squashfs"
    stage = tmp_path / "stage"
    payload = b"prompt-cache-payload"
    digest = hashlib.sha256(payload).hexdigest()

    store = SquashfsBuildStorageBackend(url=str(archive), staging_root=str(stage))
    first = store.write_bytes(payload)
    second = store.write_bytes(payload)

    assert first.file_url == second.file_url
    assert first.parts == ("objects", digest[:5], digest)
    assert first.as_bytes() == payload
    assert len(list(store.iter_locations())) == 1


def test_squashfs_build_implicit_write_fails_loudly_on_non_file_collision(tmp_path: pathlib.Path) -> None:
    archive = tmp_path / "backup.squashfs"
    stage = tmp_path / "stage"
    payload = b"collision-payload"
    digest = hashlib.sha256(payload).hexdigest()
    collision = stage / "objects" / digest[:5] / digest
    collision.mkdir(parents=True, exist_ok=True)

    store = SquashfsBuildStorageBackend(url=str(archive), staging_root=str(stage))
    with pytest.raises(SquashfsBuildImplicitOverwriteError, match="non-file path"):
        store.write_bytes(payload)


def test_squashfs_build_seal_invokes_mksquashfs_and_returns_readonly_store(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "backup.squashfs"
    stage = tmp_path / "stage"
    store = SquashfsBuildStorageBackend(url=str(archive), staging_root=str(stage), compression="gzip", deterministic=True)
    store.write_bytes(b"ONE", location="docs/one.txt")

    calls: list[list[str]] = []

    def fake_run(cmd, stdout=None, stderr=None, check=False):
        calls.append(list(cmd))
        pathlib.Path(cmd[2]).write_bytes(b"FAKE-SQUASHFS")
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    monkeypatch.setattr("LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_storage_backend.shutil.which", lambda exe: exe)
    monkeypatch.setattr("LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_storage_backend.subprocess.run", fake_run)

    built = store.seal(force=True, quiet=True)

    assert isinstance(built, SquashfsReadOnlyStorageBackend)
    assert built.archive_path == archive.resolve()
    assert archive.read_bytes() == b"FAKE-SQUASHFS"
    assert calls
    assert calls[0][0] == "mksquashfs"
    assert calls[0][1] == str(stage.resolve())
    assert calls[0][2] == str(archive.resolve())
    assert "-comp" in calls[0]
    assert "gzip" in calls[0]
    assert "-quiet" in calls[0]
    assert "-all-time" in calls[0]


def test_storage_manager_can_instantiate_squashfs_build_plugin_from_store_spec(tmp_path: pathlib.Path) -> None:
    archive = tmp_path / "backup.squashfs"
    manager = StorageManager(startup_on_add=False)
    plugin = manager.create_store_plugin(
        StoreSpec(
            store_id=None,
            store_uuid=None,
            store_name="squashfs-builder",
            store_kind="squashfs_backup",
            store_url=str(archive),
            store_access_protocol="file",
            store_root_uri=str(archive),
            store_is_read_only=False,
        )
    )

    assert isinstance(plugin, SquashfsBuildStorageBackend)
    assert plugin.archive_path == archive.resolve()
