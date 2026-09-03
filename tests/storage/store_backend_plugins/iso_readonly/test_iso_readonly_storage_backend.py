"""Read-only ISO driver, Store, registry, and Unicode contract coverage."""

from __future__ import annotations

import hashlib
import io
import os
import pathlib

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.store_backend_plugins.iso_readonly import (
    IsoReadOnlyStorageBackend,
)
from tests.fixtures.iso_image import (
    build_iso9660_iso,
    build_joliet_iso,
    build_rock_ridge_iso,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _basic_image(tmp_path: pathlib.Path) -> pathlib.Path:
    return build_joliet_iso(
        tmp_path / "library.iso",
        {
            "book one.txt": b"hello",
            "nested/book_two.epub": b"EPUB-DATA",
            "nested/empty.bin": b"",
        },
    )


def _udf_bridge_image(tmp_path: pathlib.Path) -> pathlib.Path:
    pycdlib = pytest.importorskip("pycdlib")
    path = tmp_path / "udf-bridge.iso"
    payload = io.BytesIO(b"UDF payload")
    image = pycdlib.PyCdlib()
    image.new(interchange_level=3, udf="2.60")
    image.add_directory(iso_path="/BOOKS", udf_path="/📚")
    image.add_fp(
        payload,
        len(payload.getvalue()),
        iso_path="/BOOKS/BOOK.EPUB;1",
        udf_path="/📚/naïve.epub",
    )
    image.write(str(path))
    image.close()
    return path


def _rock_ridge_udf_bridge_image(tmp_path: pathlib.Path) -> pathlib.Path:
    pycdlib = pytest.importorskip("pycdlib")
    path = tmp_path / "rock-ridge-udf-bridge.iso"
    payload = io.BytesIO(b"Rock Ridge payload")
    image = pycdlib.PyCdlib()
    image.new(interchange_level=3, rock_ridge="1.09", udf="2.60")
    image.add_directory(
        iso_path="/BOOKS",
        rr_name="rr-books",
        udf_path="/📚",
    )
    image.add_fp(
        payload,
        len(payload.getvalue()),
        iso_path="/BOOKS/BOOK.EPUB;1",
        rr_name="rr-book.epub",
        udf_path="/📚/udf-book.epub",
    )
    image.write(str(path))
    image.close()
    return path


def test_iso_readonly_status_configuration_and_registry(tmp_path) -> None:
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image), name="Disc archive")

    status = store.startup()
    descriptor = DEFAULT_BACKEND_REGISTRY.descriptor("iso9660")

    assert status.available is True
    assert status.writable is False
    assert status.object_count == 3
    assert dict(status.details)["namespace"] == "joliet"
    assert store.configuration.store_kind == "iso_readonly"
    assert store.configuration.store_root_uri == image.resolve().as_uri()
    assert store.configuration.store_access_protocol == "iso"
    assert descriptor.kind == "iso_readonly"
    assert descriptor.read_only_default is True
    assert descriptor.supports_immutable_objects is True
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    )
    assert store.characteristics.limitation("bounded_iso_logical_expansion")
    assert store.characteristics.limitation("nested_expansion_budget_external")
    options = dict(store.configuration.backend_options)
    assert options["max_total_uncompressed_bytes"] == 64 * 1024 * 1024 * 1024
    assert options["max_logical_expansion_ratio"] == 200.0


def test_iso_readonly_locate_stat_range_digest_and_inventory(tmp_path) -> None:
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))

    location = store.locate("nested/book_two.epub")
    info = store.stat_file(location)

    assert info.size == len(b"EPUB-DATA")
    assert info.modified_at is not None and info.modified_at.tzinfo is not None
    assert info.version is not None and info.version.startswith("iso:")
    assert info.hints.suggested_filename == "book_two.epub"
    assert dict(info.hints.metadata)["iso_namespace"] == "joliet"
    assert store.read_file(location) == b"EPUB-DATA"
    assert store.read_file(location, offset=2, length=4) == b"UB-D"
    assert store.read_file(location, offset=99) == b""
    assert store.read_file(location, offset=3, length=0) == b""
    assert store.read_file("nested/empty.bin") == b""
    assert store.compute_digest(location, "sha256").value == hashlib.sha256(
        b"EPUB-DATA"
    ).hexdigest()
    assert {item.key for item in store.iter_locations()} == {
        "book one.txt",
        "nested/book_two.epub",
        "nested/empty.bin",
    }
    assert {item.key for item in store.iter_locations(prefix=store.locate("nested"))} == {
        "nested/book_two.epub",
        "nested/empty.bin",
    }


def test_iso_readonly_falls_back_to_primary_iso9660_namespace(tmp_path) -> None:
    image = build_iso9660_iso(
        tmp_path / "primary.iso",
        {"BOOKS/NOVEL.EPUB": b"primary-volume"},
    )
    store = IsoReadOnlyStorageBackend(str(image))

    status = store.startup()

    assert dict(status.details)["namespace"] == "iso9660"
    assert store.read_file("BOOKS/NOVEL.EPUB") == b"primary-volume"


def test_iso_readonly_prefers_and_reads_the_udf_namespace(tmp_path) -> None:
    image = _udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))

    status = store.startup()
    info = store.stat_file("📚/naïve.epub")

    assert dict(status.details)["namespace"] == "udf"
    assert {location.key for location in store.iter_locations()} == {
        "📚/naïve.epub"
    }
    assert info.modified_at is not None and info.modified_at.tzinfo is not None
    assert dict(info.hints.metadata)["iso_namespace"] == "udf"
    assert store.read_file(info, offset=1, length=5) == b"DF pa"
    assert store.characteristics.limitation("udf_member_reads_spooled")


def test_iso_readonly_keeps_rock_ridge_priority_over_udf(tmp_path) -> None:
    image = _rock_ridge_udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))

    assert dict(store.startup().details)["namespace"] == "rock-ridge"
    assert store.read_file("rr-books/rr-book.epub") == b"Rock Ridge payload"


def test_iso_readonly_can_disable_udf_and_persists_the_policy(tmp_path) -> None:
    image = _udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(
        str(image),
        enable_udf=False,
        max_udf_member_bytes=123456,
    )

    assert dict(store.startup().details)["namespace"] == "iso9660"
    assert store.read_file("BOOKS/BOOK.EPUB") == b"UDF payload"
    assert dict(store.configuration.backend_options)["enable_udf"] is False
    assert dict(store.configuration.backend_options)["max_udf_member_bytes"] == 123456


def test_iso_udf_bridge_falls_back_when_optional_parser_is_absent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = _udf_bridge_image(tmp_path)

    def unavailable(name: str):
        if name == "pycdlib":
            raise ImportError("not installed")
        raise AssertionError(name)

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.iso.importlib.import_module",
        unavailable,
    )
    store = IsoReadOnlyStorageBackend(str(image))

    assert dict(store.startup().details)["namespace"] == "iso9660"
    assert store.read_file("BOOKS/BOOK.EPUB") == b"UDF payload"


def test_iso_names_retain_literal_terminal_version_like_text(tmp_path) -> None:
    joliet = build_joliet_iso(
        tmp_path / "literal-version-joliet.iso",
        {"folder;1/book;1": b"joliet"},
    )
    rock_ridge = build_rock_ridge_iso(
        tmp_path / "literal-version-rock-ridge.iso",
        {b"folder;1/book;1": b"rock-ridge"},
    )

    assert IsoReadOnlyStorageBackend(str(joliet)).read_file(
        "folder;1/book;1"
    ) == b"joliet"
    assert IsoReadOnlyStorageBackend(str(rock_ridge)).read_file(
        "folder;1/book;1"
    ) == b"rock-ridge"


def test_iso_readonly_applies_generic_unicode_torture_contract(tmp_path) -> None:
    expected = {case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES}
    image = build_joliet_iso(tmp_path / "unicode.iso", expected)
    store = IsoReadOnlyStorageBackend(str(image))

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)

    assert {result.location.key for result in results} == set(expected)


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX byte-name contract")
def test_iso_rock_ridge_reads_undecodable_filename_bytes(tmp_path) -> None:
    raw_key = b"legacy/" + POSIX_BAD_BYTES_FILENAME_BYTES
    image = build_rock_ridge_iso(
        tmp_path / "rock-ridge.iso",
        {raw_key: POSIX_BAD_BYTES_PAYLOAD},
    )
    store = IsoReadOnlyStorageBackend(str(image))

    [location] = list(store.iter_locations())

    assert dict(store.startup().details)["namespace"] == "rock-ridge"
    assert location.key == "legacy/" + POSIX_BAD_BYTES_FILENAME
    assert os.fsencode(location.key) == raw_key
    assert store.stat_file(location).hints.suggested_filename == POSIX_BAD_BYTES_FILENAME
    assert store.read_file(location) == POSIX_BAD_BYTES_PAYLOAD


def test_iso_readonly_supports_concurrent_reads(tmp_path) -> None:
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))
    requests = [
        ("book one.txt", 0, None, b"hello"),
        ("nested/book_two.epub", 0, None, b"EPUB-DATA"),
        ("nested/book_two.epub", 2, 4, b"UB-D"),
    ] * 8

    def read_one(request):
        key, offset, length, _expected = request
        return store.read_file(key, offset=offset, length=length)

    with ThreadPoolExecutor(max_workers=8) as executor:
        observed = list(executor.map(read_one, requests))

    assert observed == [expected for _key, _offset, _length, expected in requests]


def test_iso_readonly_enforces_image_version_on_open(tmp_path) -> None:
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))
    info = store.stat_file("book one.txt")
    original = image.read_bytes()
    image.write_bytes(original + bytes(2048))

    with pytest.raises(api.StoragePreconditionFailed):
        store.driver.open_read(
            store.driver.parse_object_address("book one.txt"),
            if_version=info.version,
        )


def test_iso_readonly_rejects_mutation_and_noncanonical_paths(tmp_path) -> None:
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))

    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"new", location="new.bin")
    with pytest.raises(api.StoreReadOnly):
        store.delete_file("book one.txt")
    for invalid in ("", "/absolute", "../escape", "a/../b", "a//b", "a\\b"):
        with pytest.raises((api.StorageInvalidAddress, api.StoreInvalidLocation, ValueError)):
            store.locate(invalid)


def test_iso_readonly_reports_truncated_and_non_iso_images(tmp_path) -> None:
    truncated = tmp_path / "truncated.iso"
    complete = _basic_image(tmp_path).read_bytes()
    truncated.write_bytes(complete[: 16 * 2048 + 100])
    malformed = tmp_path / "not-an-iso.iso"
    malformed.write_bytes(bytes(24 * 2048))

    with pytest.raises(api.StorageIntegrityError):
        IsoReadOnlyStorageBackend(str(truncated)).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="no ISO 9660"):
        IsoReadOnlyStorageBackend(str(malformed)).startup()


def test_iso_readonly_reports_udf_only_boundary_explicitly(tmp_path) -> None:
    pytest.importorskip("pycdlib")
    path = tmp_path / "udf-only.iso"
    payload = bytearray(64 * 2048)
    payload[16 * 2048 : 16 * 2048 + 7] = b"\x00BEA01\x01"
    payload[17 * 2048 : 17 * 2048 + 7] = b"\x00NSR02\x01"
    path.write_bytes(payload)
    store = IsoReadOnlyStorageBackend(str(path))

    with pytest.raises(api.StorageUnsupportedOperation, match="UDF-only"):
        store.startup()
    assert store.characteristics.limitation("udf_only_images_unsupported")


def test_iso_readonly_rejects_corrupt_both_endian_fields(tmp_path) -> None:
    image = _basic_image(tmp_path)
    payload = bytearray(image.read_bytes())
    descriptor = 16 * 2048
    payload[descriptor + 130 : descriptor + 132] = (1024).to_bytes(2, "big")
    image.write_bytes(payload)

    with pytest.raises(api.StorageIntegrityError, match="byte orders disagree"):
        IsoReadOnlyStorageBackend(str(image)).startup()


def test_iso_readonly_enforces_inventory_and_directory_limits(tmp_path) -> None:
    image = _basic_image(tmp_path)

    with pytest.raises(api.StorageUnsupportedOperation, match="inventory limit"):
        IsoReadOnlyStorageBackend(str(image), max_inventory_entries=1).startup()
    with pytest.raises(api.StorageUnavailable, match="directory.*byte limit"):
        IsoReadOnlyStorageBackend(str(image), max_directory_bytes=1024).startup()


def test_iso_readonly_bounds_member_total_and_path_bytes(tmp_path) -> None:
    image = _basic_image(tmp_path)

    with pytest.raises(api.StorageUnsupportedOperation, match="member size"):
        IsoReadOnlyStorageBackend(
            str(image),
            max_udf_member_bytes=8,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="total logical size"):
        IsoReadOnlyStorageBackend(
            str(image),
            max_total_uncompressed_bytes=10,
        ).startup()
    with pytest.raises(api.StorageIntegrityError, match="non-canonical member name"):
        IsoReadOnlyStorageBackend(
            str(image),
            max_path_bytes=5,
        ).startup()


def test_iso_udf_spool_rejects_output_beyond_indexed_size(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = _udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))
    info = store.stat_file("📚/naïve.epub")

    class FakeImage:
        def open(self, _path):
            return None

        def get_file_from_iso_fp(self, destination, *, udf_path):
            del udf_path
            destination.write(b"x" * (info.size + 1))

        def close(self):
            return None

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.iso._require_pycdlib",
        lambda _path: SimpleNamespace(PyCdlib=FakeImage),
    )

    with pytest.raises(api.StorageIntegrityError, match="exceeded its indexed size"):
        store.read_file(info)


def test_iso_readonly_missing_image_has_actionable_typed_error(tmp_path) -> None:
    missing = tmp_path / "missing-library.iso"

    with pytest.raises(api.StorageNotFound) as observed:
        IsoReadOnlyStorageBackend(str(missing))

    message = str(observed.value)
    assert "ISO configure failed" in message
    assert "missing-library.iso" in message


def test_registry_builds_iso_from_file_uri(tmp_path) -> None:
    image = _basic_image(tmp_path)
    configuration = api.StoreConfiguration(
        store_uuid=uuid4(),
        store_name="ISO",
        store_kind="iso",
        store_root_uri=image.resolve().as_uri(),
        read_only=True,
    )

    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert isinstance(store, IsoReadOnlyStorageBackend)
    assert store.read_file("book one.txt") == b"hello"
