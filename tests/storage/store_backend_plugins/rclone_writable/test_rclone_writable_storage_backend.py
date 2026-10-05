"""
Exercise writable rclone Store publication, Unicode, ingest, and staged failure boundaries.

A memory remote implements selected command effects and derives metadata from its
bytes. Write sessions use real local staging, and filesystem-ingest sources use
real temporary files; manager records remain in memory. Publication/corruption
fakes expose precisely timed failures without claiming live backend atomicity or
independent validation of remote service behavior.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.ingest.stores import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    rclone_http_storage_backend as invocation_module,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_writable import (
    RcloneWritableStorageBackend,
)
from LiuXin_alpha.storage.stores import FilesystemStore
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


class _FakeRcloneRemote:
    """
    Model selected rclone commands using an in-memory key-to-bytes map and invocation records.

    Stat computes SHA-256 from stored bytes. copyto reads another remote key or real local staging;
    moveto transfers dictionary entries. Immutable writes reject any existing target, while unknown
    commands return success. These mechanics do not model backend atomicity, permissions, or a real
    rclone process.

    Example:
        >>> remote = _FakeRcloneRemote()
        >>> remote.objects, remote.commands
        ({}, [])
    """
    def __init__(self) -> None:
        """
        Initialize the empty object map and independent command/process argument recorders.

        Example:
            >>> remote = _FakeRcloneRemote()
            >>> remote.spawned
            []


        :return: None after creating mutable dictionaries/lists for the fake remote.
        """
        self.objects: dict[str, bytes] = {}
        self.commands: list[list[str]] = []
        self.spawned: list[list[str]] = []

    @staticmethod
    def key(target: str) -> str:
        """
        Assert the fixed remote: prefix and return the remaining key text unchanged.

        Example:
            >>> _FakeRcloneRemote.key("remote:books/book.epub")
            'books/book.epub'


        :param target: Target identifier required by the fake to start with remote:.
        :return: Suffix after remote: without path or existence validation; foreign prefixes fail an assertion.
        """
        assert target.startswith("remote:")
        return target[len("remote:") :]

    def json(self, arguments, **kwargs):
        """
        Return generated stat metadata or a sorted decoded listing from current memory bytes.

        Stat exposes actual payload length/hash and a truncated-hash synthetic ID; missing keys
        raise a textual not-found error. Listing includes all stored keys, including remote staging.
        Other command shapes return an empty object.

        Example:
            >>> remote = _FakeRcloneRemote()
            >>> remote.objects["book"] = b"text"
            >>> remote.json(["lsjson", "--stat", "remote:book"])["Size"]
            4


        :param arguments: Command vector selecting stat by --stat or listing by its first lsjson token.
        :param kwargs: Invocation options accepted and ignored by the patched JSON runner.
        :return: Stat dictionary, sorted record list, or empty dictionary according to the command shape.
        """
        del kwargs
        args = list(arguments)
        if "--stat" in args:
            key = self.key(args[-1])
            if key not in self.objects:
                raise RuntimeError("object not found")
            payload = self.objects[key]
            return {
                "Name": Path(key).name,
                "Path": key,
                "Size": len(payload),
                "Hashes": {"SHA-256": hashlib.sha256(payload).hexdigest()},
                "ID": hashlib.sha256(payload).hexdigest()[:16],
                "IsDir": False,
            }
        if args and args[0] == "lsjson":
            return [
                {
                    "Path": key,
                    "Name": Path(key).name,
                    "Size": len(payload),
                    "Hashes": {
                        "SHA-256": hashlib.sha256(payload).hexdigest()
                    },
                }
                for key, payload in sorted(self.objects.items())
            ]
        return {}

    def command(self, arguments, **kwargs):
        """
        Record and simulate copyto, moveto, and deletefile, returning success for other commands.

        copyto may read real local staging bytes. The immutable marker rejects an existing target
        before mutation. moveto uses dictionary pop/assignment; missing source/deletion keys raise
        textual not-found errors. No process or independent publication protocol is exercised by
        these dictionary updates.

        Example:
            >>> remote = _FakeRcloneRemote()
            >>> remote.objects["source"] = b"book"
            >>> result = remote.command(["copyto", "remote:source", "remote:target", "--immutable"])
            >>> remote.objects["target"]
            b'book'


        :param arguments: Command vector recorded before its supported operation is interpreted.
        :param kwargs: Invocation options accepted and ignored by the patched command runner.
        :return: SimpleNamespace(returncode=0) on simulated success; supported operation failures raise RuntimeError or underlying lookup/file errors.
        """
        del kwargs
        args = list(arguments)
        self.commands.append(args)
        command = args[0]
        if command == "copyto":
            key = self.key(args[2])
            if "--immutable" in args and key in self.objects:
                raise RuntimeError("already exists")
            self.objects[key] = (
                self.objects[self.key(args[1])]
                if args[1].startswith("remote:")
                else Path(args[1]).read_bytes()
            )
            return SimpleNamespace(returncode=0)
        if command == "moveto":
            source = self.key(args[1])
            destination = self.key(args[2])
            if "--immutable" in args and destination in self.objects:
                raise RuntimeError("already exists")
            if source not in self.objects:
                raise RuntimeError("object not found")
            self.objects[destination] = self.objects.pop(source)
            return SimpleNamespace(returncode=0)
        if command == "deletefile":
            key = self.key(args[1])
            if key not in self.objects:
                raise RuntimeError("object not found")
            del self.objects[key]
            return SimpleNamespace(returncode=0)
        return SimpleNamespace(returncode=0)

    def spawn(self, arguments):
        """
        Record arguments, find the fixed remote target, and wrap optionally sliced memory bytes as a
        successful process.

        Offset/count options use Python slices. The fake does not implement a streamed lsjson
        command; its target assertion can select the driver's JSON fallback. Returned lifecycle
        lambdas are immediate and perform no real process control.

        Example:
            >>> remote = _FakeRcloneRemote()
            >>> remote.objects["book"] = b"text"
            >>> process = remote.spawn(["cat", "remote:book", "--offset", "1"])
            >>> process.stdout.read()
            b'ext'
            >>> process.stdout.close()
            >>> process.stderr.close()


        :param arguments: Recorded vector whose second item must name a stored remote: object for successful byte streaming.
        :return: Process-shaped SimpleNamespace with BytesIO streams and successful completion methods.
        """
        args = list(arguments)
        self.spawned.append(args)
        payload = self.objects[self.key(args[1])]
        if "--offset" in args:
            payload = payload[int(args[args.index("--offset") + 1]) :]
        if "--count" in args:
            payload = payload[: int(args[args.index("--count") + 1])]
        return SimpleNamespace(
            stdout=io.BytesIO(payload),
            stderr=io.BytesIO(),
            wait=lambda timeout=None: 0,
            poll=lambda: 0,
            terminate=lambda: None,
            kill=lambda: None,
        )


@pytest.fixture
def writable_store(monkeypatch, tmp_path: Path):
    """
    Install memory-remote invocation seams and create a writable Store with real local staging under
    tmp_path.

    The fixture returns the Store and fake together for publication assertions. Pytest restores
    patches and owns the temporary directory; the fake remote never performs network I/O.

    Example:
        >>> store, remote = request.getfixturevalue("writable_store")  # doctest: +SKIP


    :param monkeypatch: Pytest fixture replacing JSON, command, and writable-process invocation seams.
    :param tmp_path: Temporary parent of the explicit local staging directory used by write sessions.
    :return: Pair of configured RcloneWritableStorageBackend and its _FakeRcloneRemote.
    """
    remote = _FakeRcloneRemote()
    monkeypatch.setattr(invocation_module, "run_rclone_json", remote.json)
    monkeypatch.setattr(invocation_module, "run_rclone", remote.command)
    monkeypatch.setattr(
        RcloneWritableStorageBackend,
        "spawn_rclone_process",
        lambda self, arguments: remote.spawn(arguments),
    )
    store = RcloneWritableStorageBackend(
        "remote:",
        local_staging_directory=str(tmp_path / "staging"),
    )
    return store, remote


def test_writable_rclone_roundtrip_replace_and_delete(writable_store) -> None:
    """
    Create, range-read, reject collision, replace, and delete a file through local staging and the
    fake remote.

    Assert writable capabilities, conservative publication characteristics, durable staging
    configuration, URI round trips, and no leftover staging after successful creation. Remote
    atomicity is simulated by dictionary operations.

    Example:
        >>> test_writable_rclone_roundtrip_replace_and_delete(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store
    first = store.store_bytes(b"first", location="books/book.epub")
    durable_options = dict(store.configuration.backend_options)

    assert store.capabilities.create
    assert store.capabilities.replace
    assert store.capabilities.delete
    assert store.capabilities.atomic_publish is False
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.PER_OBJECT
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    )
    assert store.characteristics.limitation(
        "rclone_backend_dependent_limits"
    ) is not None
    assert durable_options["local_staging_directory"]
    assert "env" not in durable_options
    assert store.read_bytes(first.location, offset=1, length=3) == b"irs"
    assert store.location_uri(first.location) == "remote:books/book.epub"
    assert store.location_from_uri("remote:books/book.epub") == first.location
    assert remote.objects["books/book.epub"] == b"first"
    assert not any(key.startswith(".liuxin-staging/") for key in remote.objects)

    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"collision", location=first.location)

    replaced = store.store_bytes(
        b"second",
        location=first.location,
        write_mode="replace",
    )
    assert store.read_bytes(replaced.location) == b"second"

    store.delete(replaced.location)
    assert not store.exists(replaced.location)
    store.delete(replaced.location, missing_ok=True)


def test_writable_rclone_preserves_unicode_names_and_bytes(
    writable_store,
) -> None:
    """
    Preserve the exact Unicode key, filename hint, URI, inventory entry, and bytes across staged
    publication and readback.

    Example:
        >>> test_writable_rclone_preserves_unicode_names_and_bytes(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store

    info = store.store_bytes(UNICODE_PAYLOAD, location=UNICODE_KEY)
    current = store.stat_file(info)

    assert info.location.key == UNICODE_KEY
    assert current.hints.suggested_filename == UNICODE_FILENAME
    assert store.location_uri(info.location) == f"remote:{UNICODE_KEY}"
    assert [location.key for location in store.iter_locations()] == [
        UNICODE_KEY
    ]
    assert store.read_file(current) == UNICODE_PAYLOAD
    assert remote.objects == {UNICODE_KEY: UNICODE_PAYLOAD}


def test_writable_rclone_reads_tortured_unicode_paths_exactly(
    writable_store,
) -> None:
    """
    Seed the shared hostile-Unicode corpus through real local staging and verify exact remote keys
    and URI/read round trips.

    Example:
        >>> test_writable_rclone_reads_tortured_unicode_paths_exactly(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
        check_uri_round_trip=True,
    )

    assert set(remote.objects) == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }
    assert {result.location.key for result in results} == set(remote.objects)


def test_store_ingest_publishes_to_writable_rclone(
    writable_store,
    tmp_path: Path,
) -> None:
    """
    Ingest real local filesystem bytes into the fake writable remote and read them through a
    memory-manager asset record.

    Example:
        >>> test_store_ingest_publishes_to_writable_rclone(writable_store, tmp_path)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :param tmp_path: Temporary parent of the real filesystem ingest source.
    :return: None after the stated regression assertions pass.
    """
    destination, remote = writable_store
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"remote ingest", location="incoming/book.epub")
    manager = TransientStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert remote.objects[item.result.location.key] == b"remote ingest"
    assert manager.read_file(item.result.asset_record) == b"remote ingest"


def test_rclone_to_rclone_ingest_uses_verified_native_transfer(
    writable_store,
) -> None:
    """
    Select native remote-to-remote ingest with required metadata identity and no cat invocation
    recorded by the fake.

    Assert successful report and a copyto source identifier. The in-memory runner supplies hash
    evidence; this does not establish a live service transfer guarantee.

    Example:
        >>> test_rclone_to_rclone_ingest_uses_verified_native_transfer(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    destination, remote = writable_store
    remote.objects["incoming/native.epub"] = b"native remote transfer"
    source = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )
    manager = TransientStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    assert any(
        command[0] == "copyto" and command[1] == "remote:incoming/native.epub"
        for command in remote.commands
    )
    assert not any(arguments[0] == "cat" for arguments in remote.spawned)


def test_writable_rclone_abandoned_and_invalid_writes_publish_nothing(
    writable_store,
) -> None:
    """
    Leave the fake remote empty after an uncommitted context exit and, separately, a wrong expected
    digest.

    Example:
        >>> test_writable_rclone_abandoned_and_invalid_writes_publish_nothing(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store
    location = store.locate("books/abandoned.epub")
    with store.begin_write(location) as session:
        session.write(b"partial")
    assert remote.objects == {}

    wrong = api.Digest("sha256", "0" * 64)
    with pytest.raises(api.StoreIntegrityError):
        store.store_bytes(
            b"payload",
            location=location,
            expected_digest=wrong,
        )
    assert remote.objects == {}


def test_writable_rclone_allocates_digest_keys_and_rejects_native_metadata(
    writable_store,
) -> None:
    """
    Use the exact digest-derived allocation path and reject nonempty raw-driver native metadata.

    Example:
        >>> test_writable_rclone_allocates_digest_keys_and_rejects_native_metadata(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, _remote = writable_store
    digest = api.Digest("sha256", hashlib.sha256(b"book").hexdigest())
    allocated = store.allocate_location(expected_digest=digest)
    assert allocated.key == f"objects/sha256/{digest.value[:2]}/{digest.value}"

    with pytest.raises(api.StorageUnsupportedOperation, match="metadata"):
        store.driver.begin_write(
            store.driver.parse_object_address("metadata.bin"),
            metadata=(("title", "Book"),),
        )


def test_writable_rclone_rejects_configless_http_roots() -> None:
    """
    Reject a plain HTTPS root after normalization identifies the read-only config-less HTTP backend.

    Example:
        >>> test_writable_rclone_rejects_configless_http_roots()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(api.StorageInvalidAddress, match="read-only"):
        RcloneWritableStorageBackend("https://example.invalid/books/")


def test_writable_rclone_hides_and_reserves_its_remote_staging_namespace(
    writable_store,
) -> None:
    """
    Hide an existing staging key from inventory and reject public reads/writes to that reserved
    prefix.

    Example:
        >>> test_writable_rclone_hides_and_reserves_its_remote_staging_namespace(writable_store)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store
    remote.objects["visible.bin"] = b"visible"
    remote.objects[".liuxin-staging/interrupted.part"] = b"private"

    assert [location.key for location in store.iter_locations()] == ["visible.bin"]
    reserved = store.locate(".liuxin-staging/interrupted.part")
    with pytest.raises(api.StorageInvalidAddress, match="reserved"):
        store.read_bytes(reserved)
    with pytest.raises(api.StorageInvalidAddress, match="reserved"):
        store.store_bytes(b"user data", location=reserved)


def test_writable_rclone_failed_publish_cleans_remote_staging(
    writable_store,
    monkeypatch,
) -> None:
    """
    Remove uploaded staging and leave no destination when the injected moveto fails before mutation.

    The failure occurs before the fake applies publication; this does not test rollback after a
    partially effective remote move or final-stat failure.

    Example:
        >>> test_writable_rclone_failed_publish_cleans_remote_staging(writable_store, monkeypatch)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :param monkeypatch: Pytest fixture restoring the injected publication/copy failure override.
    :return: None after the stated regression assertions pass.
    """
    store, remote = writable_store
    original_command = remote.command

    def _failing_publish(arguments, **kwargs):
        """
        Raise a connection-reset error before moveto takes effect and delegate all other commands
        unchanged.

        Example:
            >>> result = _failing_publish(arguments)  # doctest: +SKIP


        :param arguments: Command vector whose first item selects the injected publication failure.
        :param kwargs: Invocation options forwarded to the original fake for non-moveto commands.
        :return: Original fake result for other commands; moveto raises RuntimeError before remote mutation.
        """
        if list(arguments)[0] == "moveto":
            raise RuntimeError("connection reset during publish")
        return original_command(arguments, **kwargs)

    monkeypatch.setattr(invocation_module, "run_rclone", _failing_publish)

    with pytest.raises(api.StoreUnavailable, match="connection reset"):
        store.store_bytes(b"book", location="books/book.epub")

    assert "books/book.epub" not in remote.objects
    assert not any(
        key.startswith(".liuxin-staging/") for key in remote.objects
    )


def test_corrupt_rclone_native_transfer_publishes_no_manager_records(
    writable_store,
    monkeypatch,
) -> None:
    """
    Corrupt staged native-copy bytes and require integrity failure, no asset/replica records, and
    only the original source remaining remotely.

    Example:
        >>> test_corrupt_rclone_native_transfer_publishes_no_manager_records(writable_store, monkeypatch)  # doctest: +SKIP


    :param writable_store: Fixture pair of a writable Store with local staging and its observable memory remote.
    :param monkeypatch: Pytest fixture restoring the injected publication/copy failure override.
    :return: None after the stated regression assertions pass.
    """
    destination, remote = writable_store
    remote.objects["incoming/native.epub"] = b"authoritative source"
    original_command = remote.command

    def _corrupt_copy(arguments, **kwargs):
        """
        Run the original command, then replace bytes of a copied incoming remote source with a
        shorter corrupt payload.

        Example:
            >>> result = _corrupt_copy(arguments)  # doctest: +SKIP


        :param arguments: Command vector inspected after original execution for the targeted copyto source prefix.
        :param kwargs: Invocation options forwarded unchanged to the original fake command runner.
        :return: Original successful result, even when the copied staging object has subsequently been corrupted.
        """
        result = original_command(arguments, **kwargs)
        args = list(arguments)
        if args[0] == "copyto" and args[1].startswith("remote:incoming/"):
            remote.objects[remote.key(args[2])] = b"corrupt transfer"
        return result

    monkeypatch.setattr(invocation_module, "run_rclone", _corrupt_copy)
    source = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )
    manager = TransientStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok and report.ingested_files == 0
    assert report.failures[0].error_type == "StorageIntegrityError"
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert set(remote.objects) == {"incoming/native.epub"}
