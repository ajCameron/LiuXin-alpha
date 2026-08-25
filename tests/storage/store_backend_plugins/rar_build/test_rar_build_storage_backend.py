"""Lifecycle and publication contracts for the build-once RAR Store."""

from __future__ import annotations

import hashlib
import io
import os
import pathlib
import shutil
import subprocess

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.errors import RarBuildImplicitOverwriteError
from LiuXin_alpha.storage.store_backend_plugins.rar_build import (
    RarBuildStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rar_readonly import (
    RarReadOnlyStorageBackend,
)
from tests.fixtures.storage_unicode import TORTURED_UNICODE_PATH_CASES
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


_RAR_FIXTURE = (
    pathlib.Path(__file__).resolve().parents[4]
    / "src/LiuXin_alpha/utils/decompression/rarfile/test/files/seektest.rar"
)


def _stored_fixture_payload() -> bytes:
    return RarReadOnlyStorageBackend(str(_RAR_FIXTURE)).read_file("stest2.txt")


def test_rar_build_staging_preserves_tortured_unicode_paths(
    tmp_path: pathlib.Path,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
    )

    assert {result.location.key for result in results} == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }
    assert store.staging_root == tmp_path / ".backup.rar.staging"
    assert dict(store.configuration.backend_options)["staging_root"] == str(
        store.staging_root
    )
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.STAGING_THEN_SEAL
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.STORE_COPY
    )
    assert store.characteristics.limitation("external_rar_creator_required")
    assert store.characteristics.limitation("create_only_archive_publication")


@pytest.mark.skipif(os.name != "posix", reason="symlink safety is a POSIX contract")
def test_rar_build_rejects_symlink_injected_into_staging(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "outside.bin"
    source.write_bytes(b"outside")
    store = RarBuildStorageBackend(str(tmp_path / "backup.rar"))
    (store.staging_root / "link.bin").symlink_to(source)
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    with pytest.raises(api.StoreUnsupportedOperation, match="symbolic link"):
        store.seal()


def test_rar_build_implicit_staging_is_content_addressed_and_deduplicated(
    tmp_path: pathlib.Path,
) -> None:
    payload = b"content-addressed RAR staging"
    digest = hashlib.sha256(payload).hexdigest()
    store = RarBuildStorageBackend(str(tmp_path / "backup.rar"))

    first = store.store_bytes(payload)
    second = store.store_bytes(payload)

    assert first.location == second.location
    assert first.location.key == f"objects/{digest[:5]}/{digest}"
    assert store.read_file(first) == payload


def test_rar_build_implicit_collision_fails_loudly(
    tmp_path: pathlib.Path,
) -> None:
    payload = b"collision"
    digest = hashlib.sha256(payload).hexdigest()
    store = RarBuildStorageBackend(str(tmp_path / "backup.rar"))
    store.staging_root.joinpath("objects", digest[:5], digest).mkdir(parents=True)

    with pytest.raises(RarBuildImplicitOverwriteError):
        store.store_bytes(payload)


def test_rar_build_requires_external_creator_only_when_sealing(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))
    store.store_bytes(b"book", location="book.epub")
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: None,
    )

    assert store.probe().writable
    assert any("requires" in warning for warning in store.probe().warnings)
    with pytest.raises(api.StoreUnsupportedOperation, match="licensed rar"):
        store.seal()
    assert not output.exists()
    assert store.read_file("book.epub") == b"book"


def test_rar_build_refuses_to_seal_with_active_write_session(
    tmp_path: pathlib.Path,
) -> None:
    store = RarBuildStorageBackend(str(tmp_path / "backup.rar"))
    store.store_bytes(b"ready", location="ready.bin")
    session = store.begin_write(store.locate("pending.bin"))
    try:
        with pytest.raises(api.StorePreconditionFailed, match="mutations are active"):
            store.seal()
    finally:
        session.abort()


def test_rar_build_bounds_streaming_members_total_and_persists_policy(
    tmp_path: pathlib.Path,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(
        str(output),
        staging_root=str(tmp_path / "stage"),
        max_member_bytes=4,
        max_total_uncompressed_bytes=6,
        max_compression_ratio=50,
        max_path_bytes=512,
    )

    with store.begin_write(store.locate("large.bin")) as session:
        with pytest.raises(api.StoreUnsupportedOperation, match="limited to 4"):
            session.write(b"12345")
    store.store_bytes(b"1234", location="one.bin")
    store.store_bytes(b"56", location="two.bin")
    store.staging_root.joinpath("three.bin").write_bytes(b"7")

    with pytest.raises(api.StoreUnsupportedOperation, match="total size"):
        store.seal()

    assert not output.exists()
    assert not store.file_exists("large.bin")
    options = dict(store.configuration.backend_options)
    assert options["max_member_bytes"] == 4
    assert options["max_total_uncompressed_bytes"] == 6
    assert options["max_compression_ratio"] == 50.0
    assert options["max_path_bytes"] == 512
    assert store.characteristics.limitation("validated_bounded_seal") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_rar_build_seals_rar4_non_solid_validates_and_permanently_locks(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "backup.rar"
    stage = tmp_path / "stage"
    payload = _stored_fixture_payload()
    store = RarBuildStorageBackend(
        str(output),
        staging_root=str(stage),
        rar_exe="rar-custom",
        compression_level=4,
        command_timeout_s=12.0,
    )
    store.store_bytes(payload, location="stest1.txt")
    store.store_bytes(payload, location="stest2.txt")
    calls: list[tuple[list[str], str | None]] = []

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda value: "/fake/rar" if value == "rar-custom" else None,
    )

    class FakeProcess:
        def __init__(self, command, **kwargs):
            calls.append((list(command), kwargs.get("cwd")))
            self.command = list(command)
            self.stdout = io.BytesIO()
            self.stderr = io.BytesIO()
            if command[1] == "a":
                shutil.copyfile(_RAR_FIXTURE, pathlib.Path(command[-2]))
            elif command[1] == "p":
                self.stdout = io.BytesIO(payload)
                self.stderr = io.BytesIO()

        def wait(self, timeout=None):
            assert timeout == 12.0
            return 0

        def kill(self):
            raise AssertionError("successful RAR commands must not be killed")

        def poll(self):
            return 0

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.subprocess.Popen",
        FakeProcess,
    )
    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.subprocess.Popen",
        FakeProcess,
    )
    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.shutil.which",
        lambda value: (
            "/fake/rar"
            if value in {"rar-custom", "/fake/rar", "rar"}
            else None
        ),
    )

    readonly = store.seal()

    assert isinstance(readonly, RarReadOnlyStorageBackend)
    assert store.built_store is readonly
    assert output.read_bytes() == _RAR_FIXTURE.read_bytes()
    create_command, create_cwd = calls[0]
    assert create_command[0:2] == ["/fake/rar", "a"]
    assert "-ma4" in create_command
    assert "-m4" in create_command
    assert "-s-" in create_command
    assert "-p-" in create_command
    assert create_command[-1] == "."
    assert create_cwd == str(stage.resolve())
    assert calls[1][0][0:2] == ["/fake/rar", "t"]
    assert readonly.read_file("stest2.txt") == payload
    assert not store.probe().writable
    with pytest.raises(api.StorePreconditionFailed, match="sealed"):
        store.store_bytes(b"late", location="late.bin")
    with pytest.raises(api.StorePreconditionFailed, match="already sealed"):
        store.seal()


def test_rar_build_candidate_mismatch_never_publishes(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))
    store.store_bytes(b"wrong", location="different.bin")

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class FakeProcess:
        def __init__(self, command, **_kwargs):
            self.stdout = io.BytesIO()
            if command[1] == "a":
                shutil.copyfile(_RAR_FIXTURE, pathlib.Path(command[-2]))

        def wait(self, timeout=None):
            assert timeout is not None
            return 0

        def kill(self):
            raise AssertionError("successful RAR commands must not be killed")

        def poll(self):
            return 0

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.subprocess.Popen",
        FakeProcess,
    )

    with pytest.raises(api.StoreIntegrityError, match="differ from staging"):
        store.seal()
    assert not output.exists()
    assert store.read_file("different.bin") == b"wrong"
    assert not list(tmp_path.glob(".backup.rar.build-*.part.rar"))


def test_rar_build_command_failure_preserves_staging_and_output_absence(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))
    store.store_bytes(b"book", location="book.epub")
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class FailedProcess:
        def __init__(self, _command, **kwargs):
            del kwargs
            self.stdout = io.BytesIO(b"creator failed safely")

        def wait(self, timeout=None):
            return 3

        def kill(self):
            raise AssertionError("completed RAR command must not be killed")

        def poll(self):
            return 3

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.subprocess.Popen",
        FailedProcess,
    )

    with pytest.raises(api.StoreUnavailable, match="creator failed safely"):
        store.seal()
    assert not output.exists()
    assert store.read_file("book.epub") == b"book"


def test_rar_build_timeout_kills_creator_and_preserves_staging(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output), command_timeout_s=0.25)
    store.store_bytes(b"book", location="book.epub")
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class TimedOutProcess:
        def __init__(self, command, **_kwargs):
            self.command = command
            self.killed = False
            self.stdout = io.BytesIO()

        def wait(self, timeout=None):
            if not self.killed:
                raise subprocess.TimeoutExpired(self.command, timeout)
            return -9

        def kill(self):
            self.killed = True

        def poll(self):
            return -9 if self.killed else None

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.subprocess.Popen",
        TimedOutProcess,
    )

    with pytest.raises(api.StorageTimeout, match="exceeded 0.25 seconds"):
        store.seal()
    assert not output.exists()
    assert store.read_file("book.epub") == b"book"


def test_rar_build_publish_race_preserves_external_output(
    tmp_path: pathlib.Path,
) -> None:
    output = tmp_path / "backup.rar"
    candidate = tmp_path / "candidate.rar"
    candidate.write_bytes(b"verified candidate")
    store = RarBuildStorageBackend(str(output))
    output.write_bytes(b"external publisher")

    with pytest.raises(api.StoreAlreadyExists, match="appeared during sealing"):
        store._publish_archive(candidate)

    assert output.read_bytes() == b"external publisher"
    assert candidate.read_bytes() == b"verified candidate"
    assert not store.probe().writable


def test_rar_build_existing_output_is_never_adopted_or_replaced(
    tmp_path: pathlib.Path,
) -> None:
    output = tmp_path / "backup.rar"
    output.write_bytes(b"pre-existing artifact")
    store = RarBuildStorageBackend(str(output))

    assert not store.probe().writable
    assert store.built_store is None
    with pytest.raises(api.StorePreconditionFailed, match="sealed"):
        store.store_bytes(b"book", location="book.epub")
    with pytest.raises(api.StorePreconditionFailed, match="already sealed"):
        store.seal()
    assert output.read_bytes() == b"pre-existing artifact"


def test_rar_build_rejects_output_inside_staging(tmp_path: pathlib.Path) -> None:
    stage = tmp_path / "stage"

    with pytest.raises(ValueError, match="outside"):
        RarBuildStorageBackend(
            str(stage / "backup.rar"),
            staging_root=str(stage),
        )


def test_rar_build_validates_compression_level_and_timeout(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(ValueError, match="compression_level"):
        RarBuildStorageBackend(str(tmp_path / "bad.rar"), compression_level=6)
    with pytest.raises(ValueError, match="command_timeout_s"):
        RarBuildStorageBackend(str(tmp_path / "bad.rar"), command_timeout_s=0)
