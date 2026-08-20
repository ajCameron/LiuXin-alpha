from __future__ import annotations

import hashlib
import io

from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
    rclone_http_storage_backend as invocation_module,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_writable import (
    RcloneWritableStorageBackend,
)
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)


class _FakeRcloneRemote:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.commands: list[list[str]] = []
        self.spawned: list[list[str]] = []

    @staticmethod
    def key(target: str) -> str:
        assert target.startswith("remote:")
        return target[len("remote:") :]

    def json(self, arguments, **kwargs):
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
    store, remote = writable_store
    first = store.store_bytes(b"first", location="books/book.epub")
    durable_options = dict(store.configuration.backend_options)

    assert store.capabilities.create
    assert store.capabilities.replace
    assert store.capabilities.delete
    assert store.capabilities.atomic_publish is False
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
    store, remote = writable_store

    for case in TORTURED_UNICODE_PATH_CASES:
        stored = store.store_bytes(case.payload, location=case.key)
        assert stored.location.key == case.key
        assert store.read_file(stored) == case.payload
        assert store.location_uri(stored.location) == f"remote:{case.key}"
        assert store.location_from_uri(f"remote:{case.key}") == stored.location

    assert set(remote.objects) == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }
    assert {location.key for location in store.iter_locations()} == set(remote.objects)


def test_store_ingest_publishes_to_writable_rclone(
    writable_store,
    tmp_path: Path,
) -> None:
    destination, remote = writable_store
    source = FilesystemStore(tmp_path / "source")
    source.store_bytes(b"remote ingest", location="incoming/book.epub")
    manager = InMemoryStorageManager(
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
    destination, remote = writable_store
    remote.objects["incoming/native.epub"] = b"native remote transfer"
    source = RcloneHttpReadOnlyStorageBackend(
        "remote:",
        options=RcloneBackendOptions(
            max_http_requests_per_hour=0,
            enforce_global_rate_limit=False,
        ),
    )
    manager = InMemoryStorageManager(
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
    with pytest.raises(api.StorageInvalidAddress, match="read-only"):
        RcloneWritableStorageBackend("https://example.invalid/books/")


def test_writable_rclone_hides_and_reserves_its_remote_staging_namespace(
    writable_store,
) -> None:
    store, remote = writable_store
    remote.objects["visible.bin"] = b"visible"
    remote.objects[".liuxin-staging/interrupted.part"] = b"private"

    assert [location.key for location in store.iter_locations()] == ["visible.bin"]
    reserved = store.locate(".liuxin-staging/interrupted.part")
    with pytest.raises(api.StorageInvalidAddress, match="reserved"):
        store.read_bytes(reserved)
    with pytest.raises(api.StorageInvalidAddress, match="reserved"):
        store.store_bytes(b"user data", location=reserved)
