"""
Exercise RAR builder staging, implicit destinations, sealing policy, and publication failures.

Staging and output checks use real temporary filesystem artifacts. Creator/test
processes are memory doubles that copy a bundled RAR candidate, emit diagnostics,
or simulate timeout state; command assertions describe requested flags rather than
a live creator run. The returned archive reader exercises the real parser path.
"""

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
    """
    Read the known stored member from the bundled RAR fixture through the real read-only Store.

    Example:
        >>> len(_stored_fixture_payload())
        2048


    :return: Complete stest2.txt bytes used as expected staged/extractor content.
    """
    return RarReadOnlyStorageBackend(str(_RAR_FIXTURE)).read_file("stest2.txt")


def test_rar_build_staging_preserves_tortured_unicode_paths(
    tmp_path: pathlib.Path,
) -> None:
    """
    Seed real filesystem staging through the shared Unicode harness and require exact member keys.
    Check the default hidden staging directory, persisted path, and advertised staging/sealing
    requirements without invoking a creator.

    Example:
        >>> test_rar_build_staging_preserves_tortured_unicode_paths(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Inject a symlink to a neighbouring source and require sealing to reject it during staging
    inspection before fake creator discovery can lead to execution. Skip outside POSIX.

    Example:
        >>> test_rar_build_rejects_symlink_injected_into_staging(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Stage the same payload twice without an explicit destination and require one shared Location
    using the five-character SHA-256 bucket and complete digest value.

    Example:
        >>> test_rar_build_implicit_staging_is_content_addressed_and_deduplicated(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Precreate a directory at the implicit digest destination and require the dedicated
    implicit-overwrite exception when staging bytes there.

    Example:
        >>> test_rar_build_implicit_collision_fails_loudly(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Disable creator discovery after staging a real file. Staging remains writable with a warning,
    while seal fails actionably and retains source bytes without publishing output.

    Example:
        >>> test_rar_build_requires_external_creator_only_when_sealing(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Keep a staged-write lease active and require immediate seal rejection; abort the session in
    finally to release its lease.

    Example:
        >>> test_rar_build_refuses_to_seal_with_active_write_session(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject an unannounced oversized write, then exceed the total budget through an externally
    injected staging file. Require seal refusal, absent output/failed member, and durable
    member/total/ratio/path policies.

    Example:
        >>> test_rar_build_bounds_streaming_members_total_and_persists_policy(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Use a process double that copies a known RAR fixture to exercise creation, testing, inventory
    validation, and local publication. Check requested RAR4/non-solid/compression flags, staging
    cwd, returned reader, and permanent mutation/seal refusal. This does not run a real RAR creator.

    Example:
        >>> test_rar_build_seals_rar4_non_solid_validates_and_permanently_locks(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
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
        """
        Simulate successful create/test/extract commands with fixture files and memory output.

        Creation copies the bundled archive to the requested candidate path; extraction supplies
        known bytes. No real subprocess is launched, and command/cwd observations are retained by
        the enclosing test.

        Example:
            >>> process = FakeProcess(command, cwd=str(stage))  # doctest: +SKIP
        """
        def __init__(self, command, **kwargs):
            """
            Record command/cwd, initialize pipes, and perform the fixture action selected by
            command[1].

            Example:
                >>> process = FakeProcess(command, cwd=str(stage))  # doctest: +SKIP


            :param command: Creator/test/extractor argument sequence; a copies the fixture and p exposes the known payload.
            :param kwargs: Launch options, with cwd retained in the enclosing call log and other options ignored.
            :return: None after recording the call and preparing its fake outputs or candidate file.
            """
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
            """
            Require the configured twelve-second timeout and report success immediately.

            Example:
                >>> process.wait(timeout=12.0)  # doctest: +SKIP
                0


            :param timeout: Expected 12.0-second argument from the builder or reader.
            :return: Zero exit code after the timeout assertion passes.
            """
            assert timeout == 12.0
            return 0

        def kill(self):
            """
            Fail the test if an already completed successful command is killed.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: Never returns: raises AssertionError for unexpected termination.
            """
            raise AssertionError("successful RAR commands must not be killed")

        def poll(self):
            """
            Report successful completion without a real process-state lookup.

            Example:
                >>> process.poll()  # doctest: +SKIP
                0


            :return: Zero exit code.
            """
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
    """
    Have a successful memory creator copy an unrelated fixture. Candidate metadata must differ from
    staging, leaving output absent, staged bytes intact, and the candidate path removed.

    Example:
        >>> test_rar_build_candidate_mismatch_never_publishes(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))
    store.store_bytes(b"wrong", location="different.bin")

    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class FakeProcess:
        """
        Simulate successful commands while creating a fixture that mismatches the staging manifest.

        Example:
            >>> process = FakeProcess(command)  # doctest: +SKIP
        """
        def __init__(self, command, **_kwargs):
            """
            Initialize empty output and copy the bundled candidate only for an a command.

            Example:
                >>> process = FakeProcess(command)  # doctest: +SKIP


            :param command: RAR argument sequence whose create form supplies the candidate at the penultimate position.
            :param _kwargs: Ignored process launch options.
            :return: None after preparing output and any requested fixture copy.
            """
            self.stdout = io.BytesIO()
            if command[1] == "a":
                shutil.copyfile(_RAR_FIXTURE, pathlib.Path(command[-2]))

        def wait(self, timeout=None):
            """
            Require a supplied timeout and report successful command completion without blocking.

            Example:
                >>> process.wait(timeout=12.0)  # doctest: +SKIP
                0


            :param timeout: Any non-None timeout; the double checks presence rather than elapsed time.
            :return: Zero exit code.
            """
            assert timeout is not None
            return 0

        def kill(self):
            """
            Fail the test if an already completed successful command is killed.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: Never returns: raises AssertionError for unexpected termination.
            """
            raise AssertionError("successful RAR commands must not be killed")

        def poll(self):
            """
            Report successful completion without a real process-state lookup.

            Example:
                >>> process.poll()  # doctest: +SKIP
                0


            :return: Zero exit code.
            """
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
    """
    Return creator exit code three with captured diagnostic text and require StoreUnavailable while
    retaining staged bytes and absent output.

    Example:
        >>> test_rar_build_command_failure_preserves_staging_and_output_absence(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output))
    store.store_bytes(b"book", location="book.epub")
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class FailedProcess:
        """
        Expose a completed command failure with a fixed memory diagnostic stream.

        Example:
            >>> process = FailedProcess(command)  # doctest: +SKIP
        """
        def __init__(self, _command, **kwargs):
            """
            Initialize the creator-failure diagnostic stream while ignoring launch inputs.

            Example:
                >>> process = FailedProcess(command)  # doctest: +SKIP


            :param _command: Ignored creator argument sequence.
            :param kwargs: Ignored process launch options.
            :return: None after creating stdout containing the expected diagnostic.
            """
            del kwargs
            self.stdout = io.BytesIO(b"creator failed safely")

        def wait(self, timeout=None):
            """
            Return the fixed failing exit code without checking or waiting for the timeout.

            Example:
                >>> process.wait(timeout=12.0)  # doctest: +SKIP
                3


            :param timeout: Ignored wait timeout argument.
            :return: Exit code three.
            """
            return 3

        def kill(self):
            """
            Reject attempts to kill this already completed failed command.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: Never returns: raises AssertionError.
            """
            raise AssertionError("completed RAR command must not be killed")

        def poll(self):
            """
            Report the same completed failure observed by wait.

            Example:
                >>> process.poll()  # doctest: +SKIP
                3


            :return: Exit code three.
            """
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
    """
    Use a process double whose wait raises TimeoutExpired until killed. Require the configured
    timeout diagnostic, retained staging, and absent output without waiting for a real command
    timeout.

    Example:
        >>> test_rar_build_timeout_kills_creator_and_preserves_staging(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :param monkeypatch: Fixture restoring creator/parser process and executable-discovery substitutions.
    :return: None after the stated regression assertions pass.
    """
    output = tmp_path / "backup.rar"
    store = RarBuildStorageBackend(str(output), command_timeout_s=0.25)
    store.store_bytes(b"book", location="book.epub")
    monkeypatch.setattr(
        "LiuXin_alpha.storage.store_backend_plugins.rar_build."
        "rar_build_storage_backend.shutil.which",
        lambda _value: "/fake/rar",
    )

    class TimedOutProcess:
        """
        Simulate an unfinished command that times out until its kill flag is set.

        Example:
            >>> process = TimedOutProcess(command)  # doctest: +SKIP
        """
        def __init__(self, command, **_kwargs):
            """
            Retain the command by reference, clear kill state, and initialize empty output.

            Example:
                >>> process = TimedOutProcess(command)  # doctest: +SKIP


            :param command: Argument sequence included in the synthetic timeout exception.
            :param _kwargs: Ignored process launch options.
            :return: None after setting up the unfinished process double.
            """
            self.command = command
            self.killed = False
            self.stdout = io.BytesIO()

        def wait(self, timeout=None):
            """
            Raise a synthetic timeout before kill and report signal termination afterward.

            Example:
                >>> process.kill()  # doctest: +SKIP
                >>> process.wait()  # doctest: +SKIP
                -9


            :param timeout: Value attached to TimeoutExpired while the process remains un-killed.
            :return: -9 after kill; otherwise raises subprocess.TimeoutExpired immediately.
            """
            if not self.killed:
                raise subprocess.TimeoutExpired(self.command, timeout)
            return -9

        def kill(self):
            """
            Mark the memory process killed so later wait and poll report termination.

            Example:
                >>> process.kill()  # doctest: +SKIP


            :return: None after setting killed to True.
            """
            self.killed = True

        def poll(self):
            """
            Translate the memory kill flag into unfinished or terminated process state.

            Example:
                >>> process.poll() is None  # doctest: +SKIP
                True


            :return: None before kill, otherwise -9.
            """
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
    """
    Create output after builder construction and call the publication helper with a candidate.
    Require a create-only collision, unchanged external/candidate bytes, and non-writable builder
    status.

    Example:
        >>> test_rar_build_publish_race_preserves_external_output(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Construct around an existing arbitrary artifact and require non-writable status, no built
    facade, rejected mutations/sealing, and unchanged output bytes.

    Example:
        >>> test_rar_build_existing_output_is_never_adopted_or_replaced(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Require constructor rejection when the output path lies inside the explicitly selected staging
    directory.

    Example:
        >>> test_rar_build_rejects_output_inside_staging(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
    stage = tmp_path / "stage"

    with pytest.raises(ValueError, match="outside"):
        RarBuildStorageBackend(
            str(stage / "backup.rar"),
            staging_root=str(stage),
        )


def test_rar_build_validates_compression_level_and_timeout(
    tmp_path: pathlib.Path,
) -> None:
    """
    Require constructor ValueErrors for a compression level above five and a zero command timeout.

    Example:
        >>> test_rar_build_validates_compression_level_and_timeout(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory containing real staging trees, candidate files, and output artifacts.
    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(ValueError, match="compression_level"):
        RarBuildStorageBackend(str(tmp_path / "bad.rar"), compression_level=6)
    with pytest.raises(ValueError, match="command_timeout_s"):
        RarBuildStorageBackend(str(tmp_path / "bad.rar"), command_timeout_s=0)
