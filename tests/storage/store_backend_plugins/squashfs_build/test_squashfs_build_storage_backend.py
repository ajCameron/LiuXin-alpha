from __future__ import annotations

import hashlib
import os
import pathlib
import shutil

from uuid import uuid4

import pytest

from LiuXin_alpha.storage.api import (
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageWriteUsage,
    StoreAlreadyExists,
    StoreConfiguration,
    StorePreconditionFailed,
    StoreIntegrityError,
    StoreUnavailable,
    StoreUnsupportedOperation,
    StorageTimeout,
    WriteMode,
)
from LiuXin_alpha.storage.errors import SquashfsBuildImplicitOverwriteError
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
    SquashfsBuildStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_PAYLOAD,
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def test_squashfs_build_staging_preserves_unicode_names_and_bytes(
    tmp_path: pathlib.Path,
) -> None:
    stage = tmp_path / "stage"
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(stage),
    )

    info = store.store_bytes(UNICODE_PAYLOAD, location=UNICODE_KEY)

    assert (
        store.characteristics.publication_model
        is StoragePublicationModel.STAGING_THEN_SEAL
    )
    assert (
        store.characteristics.temporary_space
        is StorageTemporarySpaceRequirement.STORE_COPY
    )
    assert store.characteristics.recommended_write_usage is StorageWriteUsage.ARCHIVAL_SNAPSHOT
    assert store.characteristics.limitation("explicit_seal_required") is not None
    assert info.location.key == UNICODE_KEY
    assert store.stat_file(info).hints.suggested_filename == UNICODE_FILENAME
    assert [location.key for location in store.iter_locations()] == [UNICODE_KEY]
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert stage.joinpath(*UNICODE_KEY.split("/")).read_bytes() == UNICODE_PAYLOAD


def test_squashfs_build_staging_reads_tortured_unicode_paths_exactly(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
    )

    assert {result.location.key for result in results} == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_squashfs_build_staging_reads_surrogateescaped_names(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )

    info = store.store_bytes(
        POSIX_BAD_BYTES_PAYLOAD,
        location=POSIX_BAD_BYTES_FILENAME,
    )

    assert info.location.key == POSIX_BAD_BYTES_FILENAME
    assert store.read_file(info) == POSIX_BAD_BYTES_PAYLOAD


def test_squashfs_build_designate_and_iter_locations(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "source.epub"
    source.write_bytes(b"EPUB-DATA")
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )

    info = store.designate_file(source, archive_path="books/source.epub")

    assert info.location.key == "books/source.epub"
    assert store.read_file(info) == b"EPUB-DATA"
    assert store.stat_file(info).size == len(b"EPUB-DATA")
    assert [location.key for location in store.iter_locations()] == [
        "books/source.epub"
    ]


def test_squashfs_build_implicit_write_uses_hash_layout_and_deduplicates(
    tmp_path: pathlib.Path,
) -> None:
    payload = b"prompt-cache-payload"
    digest = hashlib.sha256(payload).hexdigest()
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )

    first = store.store_bytes(payload)
    second = store.store_bytes(payload)

    assert first.location == second.location
    assert first.location.key == f"objects/{digest[:5]}/{digest}"
    assert store.read_file(first) == payload
    assert len(list(store.iter_locations())) == 1


def test_squashfs_build_implicit_write_fails_loudly_on_non_file_collision(
    tmp_path: pathlib.Path,
) -> None:
    payload = b"collision-payload"
    digest = hashlib.sha256(payload).hexdigest()
    stage = tmp_path / "stage"
    (stage / "objects" / digest[:5] / digest).mkdir(parents=True)
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(stage),
    )

    with pytest.raises(SquashfsBuildImplicitOverwriteError):
        store.store_bytes(payload)


def test_squashfs_build_explicit_collision_requires_explicit_replace(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )
    first = store.store_bytes(b"one", location="docs/one.txt")

    with pytest.raises(StoreAlreadyExists):
        store.store_bytes(b"two", location=first.location)
    replaced = store.store_bytes(
        b"two",
        location=first.location,
        write_mode=WriteMode.REPLACE,
    )
    assert store.read_file(replaced) == b"two"


def test_squashfs_build_abort_never_publishes_partial_staged_file(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )
    location = store.locate("docs/partial.txt")

    with store.begin_write(location, expected_size=8) as session:
        session.write(b"partial")

    assert store.file_exists(location) is False


def test_squashfs_build_refuses_to_seal_with_active_write_session(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
    )
    store.store_bytes(b"committed", location="ready.bin")
    session = store.begin_write(store.locate("pending.bin"))
    try:
        with pytest.raises(StorePreconditionFailed):
            store.seal()
    finally:
        session.abort()


def test_squashfs_build_seal_publishes_atomically_and_returns_readonly_store(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    archive = tmp_path / "backup.squashfs"
    stage = tmp_path / "stage"
    store = SquashfsBuildStorageBackend(
        str(archive),
        staging_root=str(stage),
        compression="gzip",
        deterministic=True,
    )
    store.store_bytes(b"ONE", location="docs/one.txt")

    built = store.seal(force=True, quiet=True)

    assert isinstance(built, SquashfsReadOnlyStorageBackend)
    assert built.archive_path == archive.resolve()
    assert built.read_file("docs/one.txt") == b"ONE"
    assert archive.stat().st_size > 0
    assert not list(archive.parent.glob(f".{archive.name}.*.part"))
    with pytest.raises(StorePreconditionFailed, match="already been sealed"):
        store.store_bytes(b"TWO", location="docs/two.txt")
    with pytest.raises(StorePreconditionFailed, match="already been sealed"):
        store.seal(force=True)


def test_failed_force_seal_preserves_existing_archive(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "backup.squashfs"
    archive.write_bytes(b"OLD-ARCHIVE")
    store = SquashfsBuildStorageBackend(
        str(archive),
        staging_root=str(tmp_path / "stage"),
    )
    store.store_bytes(b"ONE", location="one.txt")

    def fake_failure(_output: pathlib.Path, *, quiet: bool) -> None:
        del quiet
        raise StoreUnavailable("mksquashfs failed")

    monkeypatch.setattr(store, "_run_mksquashfs", fake_failure)

    with pytest.raises(StoreUnavailable, match="mksquashfs failed"):
        store.seal(force=True)
    assert archive.read_bytes() == b"OLD-ARCHIVE"
    assert not list(archive.parent.glob(f".{archive.name}.*.part"))


@pytest.mark.skipif(os.name != "posix", reason="symbolic links are a POSIX contract")
def test_squashfs_seal_rejects_symbolic_links_before_build(
    tmp_path: pathlib.Path,
) -> None:
    archive = tmp_path / "backup.squashfs"
    archive.write_bytes(b"OLD-ARCHIVE")
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / "target.bin").write_bytes(b"target")
    (stage / "link.bin").symlink_to("target.bin")
    store = SquashfsBuildStorageBackend(str(archive), staging_root=str(stage))

    with pytest.raises(StoreUnsupportedOperation, match="symbolic link"):
        store.seal(force=True)

    assert archive.read_bytes() == b"OLD-ARCHIVE"


def test_squashfs_seal_rejects_total_budget_without_publication(
    tmp_path: pathlib.Path,
) -> None:
    archive = tmp_path / "backup.squashfs"
    store = SquashfsBuildStorageBackend(
        str(archive),
        staging_root=str(tmp_path / "stage"),
        max_total_uncompressed_bytes=6,
    )
    store.store_bytes(b"1234", location="one.bin")
    store.store_bytes(b"5678", location="two.bin")

    with pytest.raises(StoreUnsupportedOperation, match="total size"):
        store.seal()

    assert not archive.exists()


def test_squashfs_seal_rejects_invalid_candidate_before_force_replace(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "backup.squashfs"
    archive.write_bytes(b"OLD-ARCHIVE")
    store = SquashfsBuildStorageBackend(
        str(archive),
        staging_root=str(tmp_path / "stage"),
    )
    store.store_bytes(b"ONE", location="one.txt")

    def fake_build(output: pathlib.Path, *, quiet: bool) -> None:
        del quiet
        output.write_bytes(b"NOT-A-SQUASHFS")

    monkeypatch.setattr(store, "_run_mksquashfs", fake_build)

    with pytest.raises(StoreIntegrityError, match="candidate validation"):
        store.seal(force=True)

    assert archive.read_bytes() == b"OLD-ARCHIVE"


def test_squashfs_build_enforces_external_command_timeout(
    tmp_path: pathlib.Path,
) -> None:
    fake = tmp_path / "slow-mksquashfs"
    fake.write_text("#!/bin/sh\nsleep 2\n", encoding="utf-8")
    fake.chmod(0o700)
    archive = tmp_path / "backup.squashfs"
    store = SquashfsBuildStorageBackend(
        str(archive),
        staging_root=str(tmp_path / "stage"),
        mksquashfs_exe=str(fake),
        command_timeout_s=0.05,
    )
    store.store_bytes(b"ONE", location="one.txt")

    with pytest.raises(StorageTimeout, match="exceeded"):
        store.seal()

    assert not archive.exists()


def test_squashfs_build_bounds_streaming_stage_and_persists_policy(
    tmp_path: pathlib.Path,
) -> None:
    store = SquashfsBuildStorageBackend(
        str(tmp_path / "backup.squashfs"),
        staging_root=str(tmp_path / "stage"),
        max_member_bytes=4,
        max_total_uncompressed_bytes=20,
        max_compression_ratio=50,
    )

    with store.begin_write(store.locate("large.bin")) as session:
        with pytest.raises(StoreUnsupportedOperation, match="limited to 4"):
            session.write(b"12345")

    assert not store.file_exists("large.bin")
    options = dict(store.configuration.backend_options)
    assert options["max_member_bytes"] == 4
    assert options["max_total_uncompressed_bytes"] == 20
    assert options["max_compression_ratio"] == 50.0
    assert options["staging_root"] == str((tmp_path / "stage").resolve())
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_storage_manager_can_instantiate_squashfs_builder_from_configuration(
    tmp_path: pathlib.Path,
) -> None:
    archive = tmp_path / "backup.squashfs"
    configuration = StoreConfiguration(
        store_uuid=uuid4(),
        store_name="squashfs-builder",
        store_kind="squashfs_build",
        store_root_uri=str(archive),
        store_access_protocol="squashfs-build",
    )

    def factory(config: StoreConfiguration):
        return SquashfsBuildStorageBackend(
            config.store_root_uri,
            name=config.store_name,
            uuid=config.store_uuid,
            staging_root=str(tmp_path / "stage"),
        )

    manager = InMemoryStorageManager(store_factory=factory)
    manager.create_store(configuration, startup=False)

    plugin = manager.get_store(configuration.store_uuid)
    assert isinstance(plugin, SquashfsBuildStorageBackend)
    assert plugin.archive_path == archive.resolve()
