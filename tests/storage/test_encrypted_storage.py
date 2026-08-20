"""Contract coverage for the authenticated-encryption Store wrapper."""

from __future__ import annotations

import hashlib
import dataclasses
import os

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import (
    EncryptedStore,
    FilesystemStore,
    StaticEncryptionKeyProvider,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_PAYLOAD,
    StoragePathCase,
    TORTURED_UNICODE_PATH_CASES,
)


class _HintRecordingFilesystemStore(FilesystemStore):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.seen_placement_hints = []

    @property
    def capabilities(self):
        return dataclasses.replace(super().capabilities, placement_hints=True)

    def begin_write(self, location, **kwargs):
        self.seen_placement_hints.append(kwargs.get("placement_hints"))
        return super().begin_write(location, **kwargs)


class _ReadCountingFilesystemStore(FilesystemStore):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.open_read_calls = 0

    def open_read(self, location, **kwargs):
        self.open_read_calls += 1
        return super().open_read(location, **kwargs)


def _provider(*, active: str = "key-v1") -> StaticEncryptionKeyProvider:
    return StaticEncryptionKeyProvider(
        {"key-v1": b"1" * 32, "key-v2": b"2" * 32},
        active_key_id=active,
    )


def _stores(tmp_path, *, prefix: str = ""):
    inner = FilesystemStore(tmp_path / "ciphertext")
    inner.startup()
    encrypted = EncryptedStore(
        inner,
        key_provider=_provider(),
        chunk_size=4096,
        inner_prefix=prefix,
    )
    return inner, encrypted


def test_encrypted_store_round_trips_plaintext_and_cross_chunk_ranges(tmp_path) -> None:
    inner, store = _stores(tmp_path)
    plaintext = b"not visible:" + b"a" * 5000 + b":tail"

    info = store.store_bytes(plaintext, location="books/example.epub")

    assert info.size == len(plaintext)
    assert store.read_file(info) == plaintext
    assert store.read_bytes(info.location, offset=4088, length=32) == plaintext[4088:4120]
    assert store.read_bytes(info.location, offset=len(plaintext) + 20) == b""
    assert store.stat(info.location).size == len(plaintext)
    assert not store.capabilities.external_uri_rendering
    assert store.location_uri(info.location) is None
    ciphertext = inner.read_file("books/example.epub")
    assert ciphertext.startswith(b"LXENC01\0")
    assert plaintext not in ciphertext


def test_encrypted_reads_use_one_contiguous_ciphertext_stream(tmp_path) -> None:
    inner = _ReadCountingFilesystemStore(tmp_path / "counted-ciphertext")
    store = EncryptedStore(inner, key_provider=_provider(), chunk_size=4096)
    payload = b"x" * (4096 * 4 + 17)
    info = store.store_bytes(payload, location="large.bin")
    inner.open_read_calls = 0

    assert store.read_bytes(info.location, if_version=info.version) == payload
    # Two small pinned header reads plus one contiguous ciphertext body read.
    assert inner.open_read_calls == 3


def test_known_size_encrypted_writes_stream_into_inner_staging(tmp_path) -> None:
    inner = FilesystemStore(tmp_path / "direct-inner")
    staging = tmp_path / "encrypted-stage"
    store = EncryptedStore(
        inner,
        key_provider=_provider(),
        chunk_size=4096,
        local_staging_directory=staging,
    )
    payload = b"streamed" * 2000
    location = store.locate("direct.bin")

    with store.begin_write(
        location,
        expected_size=len(payload),
        expected_digest=api.Digest(
            "sha256", hashlib.sha256(payload).hexdigest()
        ),
    ) as session:
        session.write(payload)
        assert list(staging.iterdir()) == []
        assert list(inner.iter_locations()) == []
        info = session.commit()

    assert info.size == len(payload)
    assert store.read_bytes(location) == payload


def test_store_ingest_reads_from_and_publishes_to_encrypted_stores(tmp_path) -> None:
    source_inner = FilesystemStore(tmp_path / "source-ciphertext")
    source = EncryptedStore(
        source_inner,
        key_provider=_provider(),
        chunk_size=4096,
    )
    source.store_bytes(b"secret ingest", location="incoming/book.epub")
    destination_inner = FilesystemStore(tmp_path / "destination-ciphertext")
    destination = EncryptedStore(
        destination_inner,
        key_provider=_provider(),
        chunk_size=4096,
    )
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert item.result.asset_record.metadata.original_name == "book.epub"
    assert manager.read_file(item.result.asset_record) == b"secret ingest"
    ciphertext = destination_inner.read_file(item.result.location.key)
    assert b"secret ingest" not in ciphertext


def test_encrypted_store_supports_empty_files_prefixes_and_inventory(tmp_path) -> None:
    inner, store = _stores(tmp_path, prefix="private")
    stored = store.store_bytes(b"", location="empty.bin")
    store.store_bytes(b"content", location="nested/content.bin")

    assert store.read_file(stored) == b""
    assert inner.file_exists("private/empty.bin")
    assert {location.key for location in store.iter_locations()} == {
        "empty.bin",
        "nested/content.bin",
    }
    assert [
        location.key
        for location in store.iter_locations(prefix=store.locate("nested"))
    ] == ["nested/content.bin"]


def test_encrypted_store_authenticates_ciphertext_and_object_address(tmp_path) -> None:
    inner, store = _stores(tmp_path)
    location = store.store_bytes(b"authenticated payload", location="object.bin").location
    ciphertext = bytearray(inner.read_file("object.bin"))
    pristine = bytes(ciphertext)
    ciphertext[-1] ^= 1
    inner.store_bytes(
        bytes(ciphertext),
        location="object.bin",
        write_mode=api.WriteMode.REPLACE,
    )

    with pytest.raises(api.StoreIntegrityError, match="authentication failed"):
        store.read_file(location)

    inner.store_bytes(pristine, location="renamed.bin")
    with pytest.raises(api.StoreIntegrityError, match="authentication failed"):
        store.read_file("renamed.bin")


def test_encrypted_store_validates_plaintext_expectations_before_publication(tmp_path) -> None:
    inner, store = _stores(tmp_path)
    expected = api.Digest("sha256", hashlib.sha256(b"right").hexdigest())

    with pytest.raises(api.StoreIntegrityError, match="expected 10"):
        with store.begin_write(store.locate("wrong-size.bin"), expected_size=10) as session:
            session.write(b"short")
            session.commit()
    with pytest.raises(api.StoreIntegrityError, match="digest mismatch"):
        store.store_bytes(
            b"wrong",
            location="wrong-digest.bin",
            expected_digest=expected,
        )
    with store.begin_write(store.locate("abandoned.bin")) as session:
        session.write(b"partial")

    assert list(inner.iter_locations()) == []


def test_encrypted_store_preserves_create_replace_and_delete_semantics(tmp_path) -> None:
    _inner, store = _stores(tmp_path)
    original = store.store_bytes(b"one", location="object.bin")

    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"collision", location="object.bin")
    replaced = store.store_bytes(
        b"two",
        location="object.bin",
        mode=api.WriteMode.REPLACE,
    )
    assert store.read_file(replaced) == b"two"
    store.delete(replaced.location)
    assert not store.file_exists(original.location)


def test_encrypted_store_reads_historical_keys_after_rotation(tmp_path) -> None:
    inner, first = _stores(tmp_path)
    old = first.store_bytes(b"old", location="old.bin")
    rotated = EncryptedStore(
        inner,
        key_provider=_provider(active="key-v2"),
        chunk_size=4096,
        configuration=first.configuration,
    )

    new = rotated.store_bytes(b"new", location="new.bin")

    assert rotated.read_file(old.location) == b"old"
    assert rotated.read_file(new.location) == b"new"
    assert dict(rotated.configuration.backend_options)["key_id"] == "key-v1"
    assert b"key-v2" in inner.read_file("new.bin")


def test_encrypted_store_fails_closed_when_historical_key_is_unavailable(tmp_path) -> None:
    inner, first = _stores(tmp_path)
    old = first.store_bytes(b"old", location="old.bin")
    missing = EncryptedStore(
        inner,
        key_provider=StaticEncryptionKeyProvider(
            {"key-v2": b"2" * 32},
            active_key_id="key-v2",
        ),
        chunk_size=4096,
        configuration=first.configuration,
    )

    with pytest.raises(api.StoreIntegrityError, match="unavailable key"):
        missing.read_file(old.location)


def test_encrypted_store_configuration_contains_key_identity_but_no_secret(tmp_path) -> None:
    _inner, store = _stores(tmp_path)
    options = dict(store.configuration.backend_options)

    assert options["key_id"] == "key-v1"
    assert options["inner_store_uuid"]
    assert b"1" * 32 not in repr(store.configuration).encode()


def test_encrypted_store_only_forwards_rich_metadata_when_explicitly_enabled(
    tmp_path,
) -> None:
    inner = _HintRecordingFilesystemStore(tmp_path / "inner")
    private = EncryptedStore(
        inner,
        key_provider=_provider(),
        chunk_size=4096,
        inner_prefix="private",
    )
    rich = EncryptedStore(
        inner,
        key_provider=_provider(),
        chunk_size=4096,
        inner_prefix="rich",
        forward_placement_hints=True,
    )

    private.store_bytes(
        b"private",
        location="book.epub",
        metadata={"title": "Do not expose"},
    )
    rich.store_bytes(
        b"rich",
        location="book.epub",
        metadata={"title": "Visible placement title"},
    )

    assert inner.seen_placement_hints[0] is None
    assert inner.seen_placement_hints[1] is not None
    assert private.capabilities.placement_hints is False
    assert rich.capabilities.placement_hints is True


def test_encryption_keys_must_be_exactly_256_bits() -> None:
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        StaticEncryptionKeyProvider({"bad": b"short"}, active_key_id="bad")


def test_encrypted_store_rejects_unsafe_prefixes_and_chunk_sizes(tmp_path) -> None:
    inner = FilesystemStore(tmp_path / "inner")

    with pytest.raises(ValueError, match="inner_prefix"):
        EncryptedStore(inner, key_provider=_provider(), inner_prefix="../outside")
    with pytest.raises(ValueError, match="between 4096 bytes and 64 MiB"):
        EncryptedStore(inner, key_provider=_provider(), chunk_size=1024)


@pytest.mark.parametrize(
    "case",
    TORTURED_UNICODE_PATH_CASES,
    ids=lambda case: case.case_id,
)
def test_encrypted_store_reads_tortured_unicode_paths_exactly(
    tmp_path,
    case: StoragePathCase,
) -> None:
    _inner, store = _stores(tmp_path)

    stored = store.store_bytes(case.payload, location=case.key)
    [discovered] = list(store.iter_locations())

    assert stored.location.key == case.key
    assert discovered.key == case.key
    assert store.read_file(discovered) == case.payload


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_encrypted_filesystem_store_reads_surrogateescaped_object_names(
    tmp_path,
) -> None:
    _inner, store = _stores(tmp_path)

    stored = store.store_bytes(
        POSIX_BAD_BYTES_PAYLOAD,
        location=POSIX_BAD_BYTES_FILENAME,
    )
    [discovered] = list(store.iter_locations())

    assert stored.location.key == POSIX_BAD_BYTES_FILENAME
    assert discovered.key == POSIX_BAD_BYTES_FILENAME
    assert store.read_file(discovered) == POSIX_BAD_BYTES_PAYLOAD
