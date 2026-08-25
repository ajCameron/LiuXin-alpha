"""Contracts for ZIP, TAR, and RAR storage plugins."""

from __future__ import annotations

import base64
import io
import os
import pathlib
import stat
import tarfile
import zipfile

from uuid import uuid4

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import zip as zip_driver_module
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.rar_readonly import (
    RarReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.tar_readonly import (
    TarReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.tar_writable import (
    TarWritableStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.zip_readonly import (
    ZipReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.zip_writable import (
    ZipWritableStorageBackend,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_PAYLOAD,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


_RAR_FIXTURE = (
    pathlib.Path(__file__).resolve().parents[3]
    / "src/LiuXin_alpha/utils/decompression/rarfile/test/files/seektest.rar"
)
_RAR_UNICODE_FIXTURE = _RAR_FIXTURE.with_name("unicode.rar")
_RAR5_SUBDIRS = (
    "UmFyIRoHAQDz4YLrCwEFBwAGAQGAgIAAWyrxsjACAwuGAASGAKSDAsekBMmAAAESc3ViL2RpcjIvZmlsZTIudHh0CgMTCNwVX4XkBhNmaWxlMgokNHkgOAIDC4gABIgApIMCfSS3cYAAARpzdWIvd2l0aCBzcGFjZS9sb25nIGZuLnR4dAoDEyncFV9mv8sdbG9uZyBmbgoOjzxzOAIDC4UABIUApIMCwYnsL4AAARpzdWIvw7zItcSpw7bhuIvDqC9maWxlLnR4dAoDE0TdFV+dEHMIZmlsZQqvrxG4MAIDC4YABIYApIMCBPcp4oAAARJzdWIvZGlyMS9maWxlMS50eHQKAxP92xVfHJEnNGZpbGUxCtVl6Z4kAgMLAAUA7YMBAAAAAIAAAQhzdWIvZGlyMgoDEwjcFV/ICfsT1nxQqSoCAwsABQDtgwEAAAAAgAABDnN1Yi93aXRoIHNwYWNlCgMTKdwVX1vbgh6UOOweJQIDCwAFAO2DAQAAAACAAAEJc3ViL2VtcHR5CgMT5dsVX/bv4ArIG6fPLQIDCwAFAO2DAQAAAACAAAERc3ViL8O8yLXEqcO24biLw6gKAxNE3RVfmSwqCYEdQEkkAgMLAAUA7YMBAAAAAIAAAQhzdWIvZGlyMQoDE/3bFV8Nrd40msCgER8CAwsABQDtgwEAAAAAgAABA3N1YgoDEzLdFV+8OWMOHXdWUQMFBAA="
)
_RAR5_SYMLINKS = (
    "UmFyIRoHAQAzkrXlCgEFBgAFAQGAgABuR35XMgIDGAAECP/DAgAAAACAQAEJZGF0YV9saW5rCgMTBuQdXzlDegcMBQEACGRhdGEudHh0/Yr6ESYCAwuFAASFALSDAoLFweaAQAEIZGF0YS50eHQKAxPt4x1f8MPfIWRhdGEKXgUHFDgCAxwABAz/wwIAAAAAgEABC3JhbmRvbV9saW5rCgMTSOgdX2WpcDUQBQEADC4uL3JhbmRvbTEyMx13VlEDBQQA"
)


def _write_zip(path: pathlib.Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for key, payload in members.items():
            archive.writestr(key, payload)


def _write_tar(
    path: pathlib.Path,
    members: dict[str, bytes],
    *,
    mode: str = "w",
) -> None:
    with tarfile.open(
        path,
        mode,
        format=tarfile.PAX_FORMAT,
        encoding="utf-8",
        errors="surrogateescape",
    ) as archive:
        for key, payload in members.items():
            info = tarfile.TarInfo(key)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def test_zip_readonly_preserves_tortured_unicode_names_ranges_and_bytes(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "unicode.zip"
    _write_zip(path, {case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES})
    store = ZipReadOnlyStorageBackend(str(path))

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)

    assert len(results) == len(TORTURED_UNICODE_PATH_CASES)
    assert store.startup().available
    assert store.capabilities.enumeration is api.EnumerationCompleteness.COMPLETE
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    assert store.characteristics.max_object_bytes == 4 * 1024 * 1024 * 1024
    assert store.characteristics.limitation("bounded_zip_expansion") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")
    assert dict(store.configuration.backend_options)["max_compression_ratio"] == 200.0
    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"forbidden", location="new.bin")


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_tar_readonly_preserves_tortured_and_legacy_byte_names(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "unicode.tar.gz"
    members = {case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES}
    members[POSIX_BAD_BYTES_FILENAME] = POSIX_BAD_BYTES_PAYLOAD
    _write_tar(path, members, mode="w:gz")
    store = TarReadOnlyStorageBackend(str(path))

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)

    assert len(results) == len(TORTURED_UNICODE_PATH_CASES)
    assert store.read_file(POSIX_BAD_BYTES_FILENAME) == POSIX_BAD_BYTES_PAYLOAD
    assert store.startup().available
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_zip_writable_rebuilds_atomically_and_reopens_readonly(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "library.zip"
    store = ZipWritableStorageBackend(str(path), deterministic=True)

    first = store.store_bytes(b"first", location="books/book.epub")
    retained = store.store_bytes(b"retain", location="books/retain.mobi")
    before_failed_write = path.read_bytes()
    with pytest.raises(api.StoreIntegrityError):
        store.store_stream(
            io.BytesIO(b"short"),
            location="broken.bin",
            expected_size=99,
        )
    assert path.read_bytes() == before_failed_write
    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"collision", location=first.location)

    replaced = store.store_bytes(
        b"second",
        location=first.location,
        write_mode=api.WriteMode.REPLACE,
    )
    store.delete_file(retained)
    reopened = ZipReadOnlyStorageBackend(str(path))

    assert reopened.read_file(replaced.location.key) == b"second"
    assert not reopened.file_exists(retained.location.key)
    assert store.capabilities.atomic_publish
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.WHOLE_STORE_REBUILD
    )
    assert store.characteristics.limitation("whole_store_rebuild") is not None


@pytest.mark.parametrize(
    ("compression", "compresslevel"),
    (
        ("deflated", -2),
        ("deflated", 10),
        ("bzip2", 0),
        ("bzip2", 10),
        ("stored", 1),
        ("lzma", 1),
    ),
)
def test_zip_writable_rejects_algorithm_inappropriate_compression_levels(
    tmp_path: pathlib.Path,
    compression: str,
    compresslevel: int,
) -> None:
    with pytest.raises(ValueError, match="compresslevel"):
        ZipWritableStorageBackend(
            str(tmp_path / "invalid.zip"),
            compression=compression,
            compresslevel=compresslevel,
        )


@pytest.mark.parametrize(
    ("compression", "compresslevel"),
    (("deflated", -1), ("deflated", 9), ("bzip2", 1), ("bzip2", 9)),
)
def test_zip_writable_accepts_supported_compression_levels(
    tmp_path: pathlib.Path,
    compression: str,
    compresslevel: int,
) -> None:
    path = tmp_path / f"{compression}-{compresslevel}.zip"
    store = ZipWritableStorageBackend(
        str(path),
        compression=compression,
        compresslevel=compresslevel,
    )

    store.store_bytes(b"compress me" * 100, location="book.epub")

    assert ZipReadOnlyStorageBackend(str(path)).read_file("book.epub") == (
        b"compress me" * 100
    )


@pytest.mark.parametrize("compression", ["none", "gz", "bz2", "xz"])
def test_tar_writable_roundtrip_replace_delete_and_reopen(
    tmp_path: pathlib.Path,
    compression: str,
) -> None:
    suffix = "tar" if compression == "none" else f"tar.{compression}"
    path = tmp_path / f"library.{suffix}"
    store = TarWritableStorageBackend(
        str(path),
        compression=compression,
        deterministic=True,
    )

    first = store.store_bytes(b"first", location="books/book.epub")
    retained = store.store_bytes(b"retain", location="books/retain.mobi")
    replaced = store.store_bytes(
        b"second",
        location=first.location,
        write_mode=api.WriteMode.REPLACE,
    )
    store.delete_file(retained, if_version=store.stat_file(retained).version)
    reopened = TarReadOnlyStorageBackend(str(path))

    assert reopened.read_file(replaced.location.key) == b"second"
    assert not reopened.file_exists(retained.location.key)
    assert store.capabilities.atomic_publish
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.WHOLE_STORE_REBUILD
    )


@pytest.mark.parametrize("format_name", ["zip", "tar.gz"])
def test_deterministic_archive_writers_reproduce_container_bytes(
    tmp_path: pathlib.Path,
    format_name: str,
) -> None:
    paths = [tmp_path / f"first.{format_name}", tmp_path / f"second.{format_name}"]
    for path in paths:
        if format_name == "zip":
            store = ZipWritableStorageBackend(str(path), deterministic=True)
        else:
            store = TarWritableStorageBackend(
                str(path),
                compression="gz",
                deterministic=True,
            )
        store.store_bytes(b"first", location="z/last.epub")
        store.store_bytes(b"second", location="a/first.epub")

    assert paths[0].read_bytes() == paths[1].read_bytes()


def test_archive_writers_fail_closed_before_lossy_rebuild(
    tmp_path: pathlib.Path,
) -> None:
    zip_path = tmp_path / "commented.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.comment = b"preserve me"
        archive.writestr("book.epub", b"book")
    zip_store = ZipWritableStorageBackend(str(zip_path))

    with pytest.raises(api.StoreUnsupportedOperation, match="allow_lossy_rebuild"):
        zip_store.store_bytes(b"new", location="new.epub")
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.comment == b"preserve me"
    lossy_zip = ZipWritableStorageBackend(
        str(zip_path),
        allow_lossy_rebuild=True,
    )
    lossy_zip.store_bytes(b"new", location="new.epub")
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.comment == b""
        assert set(archive.namelist()) == {"book.epub", "new.epub"}

    tar_path = tmp_path / "linked.tar"
    with tarfile.open(tar_path, "w") as archive:
        payload = tarfile.TarInfo("book.epub")
        payload.size = 4
        archive.addfile(payload, io.BytesIO(b"book"))
        link = tarfile.TarInfo("alias.epub")
        link.type = tarfile.SYMTYPE
        link.linkname = "book.epub"
        archive.addfile(link)
    tar_store = TarWritableStorageBackend(str(tar_path))

    with pytest.raises(api.StoreUnsupportedOperation, match="link members are rejected"):
        tar_store.delete_file("book.epub")
    with tarfile.open(tar_path) as archive:
        assert archive.getmember("alias.epub").issym()
    lossy_tar = TarWritableStorageBackend(
        str(tar_path),
        allow_lossy_rebuild=True,
    )
    with pytest.raises(api.StoreUnsupportedOperation, match="link members are rejected"):
        lossy_tar.delete_file("book.epub")
    with tarfile.open(tar_path) as archive:
        assert archive.getnames() == ["book.epub", "alias.epub"]


def test_archive_writers_report_unpreserved_regular_member_metadata(
    tmp_path: pathlib.Path,
) -> None:
    zip_path = tmp_path / "member-metadata.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        info = zipfile.ZipInfo("book.epub")
        info.comment = b"member comment"
        info.external_attr = 0o644 << 16
        archive.writestr(info, b"book")
    zip_store = ZipWritableStorageBackend(str(zip_path))

    zip_status = zip_store.startup()

    assert not zip_status.writable
    assert "member comments" in " ".join(zip_status.warnings)
    with pytest.raises(api.StoreUnsupportedOperation, match="allow_lossy_rebuild"):
        zip_store.store_bytes(b"new", location="new.epub")

    tar_path = tmp_path / "member-metadata.tar"
    with tarfile.open(tar_path, "w") as archive:
        info = tarfile.TarInfo("book.epub")
        info.size = 4
        info.mode = 0o644
        info.uid = 1000
        info.uname = "archivist"
        archive.addfile(info, io.BytesIO(b"book"))
    tar_store = TarWritableStorageBackend(str(tar_path))

    tar_status = tar_store.startup()

    assert not tar_status.writable
    assert "permissions" in " ".join(tar_status.warnings)
    assert "ownership" in " ".join(tar_status.warnings)
    with pytest.raises(api.StoreUnsupportedOperation, match="allow_lossy_rebuild"):
        tar_store.store_bytes(b"new", location="new.epub")


def test_archive_conditional_reads_detect_container_replacement(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "versions.zip"
    _write_zip(path, {"book.epub": b"first"})
    store = ZipReadOnlyStorageBackend(str(path))
    original = store.stat_file("book.epub")
    replacement = tmp_path / "replacement.zip"
    _write_zip(replacement, {"book.epub": b"second"})
    os.replace(replacement, path)

    with pytest.raises(api.StorePreconditionFailed):
        store.read_bytes(original.location, if_version=original.version)


def test_archive_readers_reject_ambiguous_or_escaping_members(
    tmp_path: pathlib.Path,
) -> None:
    duplicate = tmp_path / "duplicate.zip"
    with zipfile.ZipFile(duplicate, "w") as archive:
        archive.writestr("same.bin", b"one")
        with pytest.warns(UserWarning):
            archive.writestr("same.bin", b"two")
    with pytest.raises(api.StorageIntegrityError, match="duplicate"):
        ZipReadOnlyStorageBackend(str(duplicate)).startup()

    escaping = tmp_path / "escaping.tar"
    _write_tar(escaping, {"../escape.bin": b"bad"})
    with pytest.raises(api.StorageInvalidAddress):
        TarReadOnlyStorageBackend(str(escaping)).startup()


@pytest.mark.parametrize(
    "member_name",
    ("../outside.bin", "/absolute.bin", "books\\windows-escape.bin", "a/./b.bin"),
)
def test_zip_rejects_extraction_paths_without_touching_the_filesystem(
    tmp_path: pathlib.Path,
    member_name: str,
) -> None:
    archive_path = tmp_path / "escaping.zip"
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"sentinel")
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(member_name, b"hostile")

    with pytest.raises(api.StorageInvalidAddress):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()

    assert outside.read_bytes() == b"sentinel"
    assert not (tmp_path / "absolute.bin").exists()
    assert not (tmp_path / "books").exists()


@pytest.mark.parametrize(
    "members",
    (
        (("node", b"file"), ("node/child.bin", b"child")),
        (("node/child.bin", b"child"), ("node", b"file")),
        (("node/", b""), ("node", b"file")),
    ),
)
def test_zip_rejects_file_directory_overwrite_aliases(
    tmp_path: pathlib.Path,
    members: tuple[tuple[str, bytes], ...],
) -> None:
    archive_path = tmp_path / "conflicting.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for name, payload in members:
            archive.writestr(name, payload)

    with pytest.raises(api.StorageIntegrityError, match="conflicting|overwrite|descends"):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()


@pytest.mark.parametrize("trailing_slash", (False, True))
def test_zip_rejects_symbolic_link_members(
    tmp_path: pathlib.Path,
    trailing_slash: bool,
) -> None:
    archive_path = tmp_path / "linked.zip"
    link = zipfile.ZipInfo("link/" if trailing_slash else "link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(link, b"target" if not trailing_slash else b"")

    with pytest.raises(api.StorageUnsupportedOperation, match="non-directory|symbolic-link"):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()


def test_zip_bounds_member_total_and_ratio_before_decompression(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "expansion.zip"
    with zipfile.ZipFile(
        archive_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr("first.bin", b"0" * 128 * 1024)
        archive.writestr("second.bin", b"1" * 128 * 1024)

    with pytest.raises(api.StorageUnsupportedOperation, match="declared size"):
        ZipReadOnlyStorageBackend(
            str(archive_path),
            max_member_bytes=64 * 1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="total expanded size"):
        ZipReadOnlyStorageBackend(
            str(archive_path),
            max_total_uncompressed_bytes=200 * 1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="expansion ratio"):
        ZipReadOnlyStorageBackend(
            str(archive_path),
            max_compression_ratio=10,
        ).startup()


def test_zip_preflights_all_entries_and_central_directory_size(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "directory-heavy.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("one/", b"")
        archive.writestr("two/", b"")

    with pytest.raises(api.StorageUnsupportedOperation, match="declares 2 entries"):
        ZipReadOnlyStorageBackend(
            str(archive_path),
            max_inventory_entries=1,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="central directory"):
        ZipReadOnlyStorageBackend(
            str(archive_path),
            max_central_directory_bytes=1,
        ).startup()


@pytest.mark.parametrize(
    "members",
    (
        (("node", b"file", False), ("node/child.bin", b"child", False)),
        (("node/child.bin", b"child", False), ("node", b"file", False)),
        (("node", b"", True), ("node", b"file", False)),
    ),
)
def test_tar_rejects_file_directory_overwrite_aliases(
    tmp_path: pathlib.Path,
    members: tuple[tuple[str, bytes, bool], ...],
) -> None:
    archive_path = tmp_path / "conflicting.tar"
    with tarfile.open(archive_path, "w") as archive:
        for name, payload, is_directory in members:
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE if is_directory else tarfile.REGTYPE
            info.size = 0 if is_directory else len(payload)
            archive.addfile(info, None if is_directory else io.BytesIO(payload))

    with pytest.raises(api.StorageIntegrityError, match="conflicting|overwrite|descends"):
        TarReadOnlyStorageBackend(str(archive_path)).startup()


def test_tar_rejects_links_and_special_members(tmp_path: pathlib.Path) -> None:
    for entry_type, label in (
        (tarfile.SYMTYPE, "symbolic"),
        (tarfile.LNKTYPE, "hard-link"),
        (tarfile.FIFOTYPE, "non-regular"),
    ):
        archive_path = tmp_path / f"unsafe-{entry_type.hex()}.tar"
        with tarfile.open(archive_path, "w") as archive:
            info = tarfile.TarInfo("unsafe")
            info.type = entry_type
            info.linkname = "target"
            archive.addfile(info)

        with pytest.raises(api.StorageUnsupportedOperation, match=label):
            TarReadOnlyStorageBackend(str(archive_path)).startup()


def test_tar_bounds_member_total_ratio_and_all_entry_count(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "expansion.tar.gz"
    _write_tar(
        archive_path,
        {
            "first.bin": b"0" * 128 * 1024,
            "second.bin": b"1" * 128 * 1024,
        },
        mode="w:gz",
    )

    with pytest.raises(api.StorageUnsupportedOperation, match="declared size"):
        TarReadOnlyStorageBackend(
            str(archive_path),
            max_member_bytes=64 * 1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="total expanded size"):
        TarReadOnlyStorageBackend(
            str(archive_path),
            max_total_uncompressed_bytes=200 * 1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="expansion ratio"):
        TarReadOnlyStorageBackend(
            str(archive_path),
            max_compression_ratio=10,
        ).startup()

    directory_archive = tmp_path / "directory-heavy.tar"
    with tarfile.open(directory_archive, "w") as archive:
        for name in ("one", "two"):
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            archive.addfile(info)
    with pytest.raises(api.StorageUnsupportedOperation, match="inventory exceeds 1"):
        TarReadOnlyStorageBackend(
            str(directory_archive),
            max_inventory_entries=1,
        ).startup()


def test_tar_bounds_individual_pax_metadata_records(tmp_path: pathlib.Path) -> None:
    archive_path = tmp_path / "metadata.tar"
    with tarfile.open(archive_path, "w", format=tarfile.PAX_FORMAT) as archive:
        info = tarfile.TarInfo("book.epub")
        info.size = 4
        info.pax_headers = {"comment": "x" * 4096}
        archive.addfile(info, io.BytesIO(b"book"))

    with pytest.raises(api.StorageUnsupportedOperation, match="oversized metadata"):
        TarReadOnlyStorageBackend(
            str(archive_path),
            max_single_metadata_record_bytes=1024,
        ).startup()


def test_tar_failed_total_budget_write_keeps_original_archive(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "bounded-write.tar.gz"
    store = TarWritableStorageBackend(
        str(archive_path),
        compression="gz",
        max_total_uncompressed_bytes=10,
        max_compression_ratio=10_000,
    )
    store.store_bytes(b"123456", location="first.bin")
    before = archive_path.read_bytes()

    with pytest.raises(api.StorageUnsupportedOperation, match="total expanded size"):
        store.store_bytes(b"abcdef", location="second.bin")

    assert archive_path.read_bytes() == before
    assert store.read_file("first.bin") == b"123456"
    options = dict(store.configuration.backend_options)
    assert options["max_total_uncompressed_bytes"] == 10
    assert options["max_compression_ratio"] == 10_000.0


def test_zip_preflight_rejects_an_underreported_entry_count(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "underreported.zip"
    _write_zip(archive_path, {"one.bin": b"one", "two.bin": b"two"})
    payload = bytearray(archive_path.read_bytes())
    end_record = payload.rfind(b"PK\x05\x06")
    assert end_record >= 0
    payload[end_record + 8 : end_record + 10] = (1).to_bytes(2, "little")
    payload[end_record + 10 : end_record + 12] = (1).to_bytes(2, "little")
    archive_path.write_bytes(payload)

    with pytest.raises(api.StorageIntegrityError, match="declares 1.*contains 2"):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()


def test_zip_rejects_a_local_header_name_that_disagrees_with_inventory(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "header-mismatch.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("safe.bin", b"payload")
        info = archive.getinfo("safe.bin")
    with archive_path.open("r+b") as stream:
        stream.seek(info.header_offset + 30)
        stream.write(b"evil.bin")

    with pytest.raises(api.StorageIntegrityError, match="differ"):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()


def test_zip_translates_invalid_utf8_member_names_to_integrity_errors(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "invalid-utf8.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("safe.bin", b"payload")
    payload = bytearray(archive_path.read_bytes())
    local_header = payload.find(b"PK\x03\x04")
    central_header = payload.find(b"PK\x01\x02")
    assert local_header >= 0 and central_header >= 0
    for flag_offset in (local_header + 6, central_header + 8):
        flags = int.from_bytes(payload[flag_offset : flag_offset + 2], "little")
        payload[flag_offset : flag_offset + 2] = (flags | 0x800).to_bytes(2, "little")
    payload[local_header + 30 : local_header + 38] = b"\xffafe.bin"
    payload[central_header + 46 : central_header + 54] = b"\xffafe.bin"
    archive_path.write_bytes(payload)

    with pytest.raises(api.StorageIntegrityError, match="encoding is invalid"):
        ZipReadOnlyStorageBackend(str(archive_path)).startup()


def test_zip_writer_rejects_path_overwrite_plan_without_replacing_archive(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "writer-conflict.zip"
    store = ZipWritableStorageBackend(str(archive_path), deterministic=True)
    store.store_bytes(b"original", location="node")
    original_archive = archive_path.read_bytes()

    with pytest.raises(api.StorageIntegrityError, match="descends"):
        store.store_bytes(b"child", location="node/child.bin")

    assert archive_path.read_bytes() == original_archive
    assert store.read_file("node") == b"original"
    assert not store.file_exists("node/child.bin")


def test_zip_writer_bounds_unannounced_staging_and_preserves_archive(
    tmp_path: pathlib.Path,
) -> None:
    archive_path = tmp_path / "writer-limit.zip"
    store = ZipWritableStorageBackend(str(archive_path), max_member_bytes=4)
    original_archive = archive_path.read_bytes()

    with pytest.raises(api.StorageUnsupportedOperation, match="staging exceeds 4"):
        store.store_stream(io.BytesIO(b"12345"), location="too-large.bin")

    assert archive_path.read_bytes() == original_archive
    assert not store.file_exists("too-large.bin")


def test_zip_writer_does_not_overwrite_an_external_archive_replacement(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "concurrent.zip"
    store = ZipWritableStorageBackend(str(archive_path), deterministic=True)
    store.store_bytes(b"original", location="book.epub")
    external = tmp_path / "external.zip"
    _write_zip(external, {"book.epub": b"external"})
    original_copy_exact = zip_driver_module.copy_exact
    replacement_published = False

    def replace_during_rebuild(*args, **kwargs):
        nonlocal replacement_published
        result = original_copy_exact(*args, **kwargs)
        if not replacement_published:
            os.replace(external, archive_path)
            replacement_published = True
        return result

    monkeypatch.setattr(zip_driver_module, "copy_exact", replace_during_rebuild)

    with pytest.raises(api.StoragePreconditionFailed, match="changed during rebuild"):
        store.store_bytes(b"new", location="new.epub")

    assert replacement_published
    assert ZipReadOnlyStorageBackend(str(archive_path)).read_file("book.epub") == b"external"
    assert not ZipReadOnlyStorageBackend(str(archive_path)).file_exists("new.epub")


def test_zip_crc_failure_is_translated_to_storage_integrity_error(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "corrupt.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("book.epub", b"original bytes")
        info = archive.getinfo("book.epub")
    with path.open("r+b") as stream:
        stream.seek(info.header_offset)
        header = stream.read(30)
        name_length = int.from_bytes(header[26:28], "little")
        extra_length = int.from_bytes(header[28:30], "little")
        stream.seek(info.header_offset + 30 + name_length + extra_length)
        original = stream.read(1)
        stream.seek(-1, os.SEEK_CUR)
        stream.write(bytes((original[0] ^ 0xFF,)))
    store = ZipReadOnlyStorageBackend(str(path))

    with pytest.raises(api.StorageIntegrityError):
        store.read_file("book.epub")


def test_archive_driver_addresses_do_not_cross_format_boundaries(
    tmp_path: pathlib.Path,
) -> None:
    zip_path = tmp_path / "scoped.zip"
    tar_path = tmp_path / "scoped.tar"
    _write_zip(zip_path, {"book.epub": b"zip"})
    _write_tar(tar_path, {"book.epub": b"tar"})
    shared_uuid = uuid4()
    zip_store = ZipReadOnlyStorageBackend(str(zip_path), uuid=shared_uuid)
    tar_store = TarReadOnlyStorageBackend(str(tar_path), uuid=shared_uuid)
    zip_address = zip_store.driver.parse_object_address("book.epub")

    with pytest.raises(api.StorageInvalidAddress, match="TarObjectAddress"):
        tar_store.driver.stat(zip_address)


@pytest.mark.parametrize(
    ("kind", "suffix"),
    (("zip_writable", ".zip"), ("tar_writable", ".tar")),
)
def test_registry_masks_configured_read_only_archive_writers(
    tmp_path: pathlib.Path,
    kind: str,
    suffix: str,
) -> None:
    path = tmp_path / f"policy{suffix}"
    if kind == "zip_writable":
        _write_zip(path, {"book.epub": b"book"})
    else:
        _write_tar(path, {"book.epub": b"book"})
    configuration = api.StoreConfiguration(
        store_uuid=uuid4(),
        store_name="policy-pinned archive",
        store_kind=kind,
        store_root_uri=path.resolve().as_uri(),
        read_only=True,
    )

    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert store.read_file("book.epub") == b"book"
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"new", location="new.epub")


def test_rar_readonly_indexes_without_tool_and_reads_stored_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.shutil.which",
        lambda _value: None,
    )
    store = RarReadOnlyStorageBackend(str(_RAR_FIXTURE))

    inventory = {location.key for location in store.iter_locations()}

    assert inventory == {"stest1.txt", "stest2.txt"}
    assert len(store.read_file("stest2.txt")) == 2048
    assert store.read_file("stest2.txt", offset=2, length=4) == b"0\n00"
    status = store.startup()
    assert not status.available
    assert "extractor" in status.message
    with pytest.raises(api.StoreUnsupportedOperation, match="executable"):
        store.read_file("stest1.txt")
    assert store.characteristics.limitation(
        "modern_rarfile_required_for_rar5"
    ) is not None


def test_rar_compressed_member_uses_bounded_external_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = RarReadOnlyStorageBackend(str(_RAR_FIXTURE)).read_file("stest2.txt")
    calls: list[list[str]] = []

    def fake_which(value: str):
        return "/fake/unrar" if value in {"fake-unrar", "unrar"} else None

    class FakeProcess:
        def __init__(self, arguments, **kwargs):
            del kwargs
            calls.append(list(arguments))
            self.stdout = io.BytesIO(baseline)
            self.stderr = io.BytesIO()

        def wait(self, timeout=None):
            assert timeout == 12.0
            return 0

        def kill(self):
            raise AssertionError("successful extraction must not be killed")

        def poll(self):
            return 0

    monkeypatch.setattr("LiuXin_alpha.storage.drivers.rar.shutil.which", fake_which)
    monkeypatch.setattr("LiuXin_alpha.storage.drivers.rar.subprocess.Popen", FakeProcess)
    store = RarReadOnlyStorageBackend(
        str(_RAR_FIXTURE),
        extractor_exe="fake-unrar",
        extract_timeout_s=12.0,
    )

    assert store.startup().available
    assert store.read_file("stest1.txt", offset=10, length=20) == baseline[10:30]
    assert calls[0][:4] == ["/fake/unrar", "p", "-inul", "-p-"]


def test_rar_external_member_output_must_pass_crc_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = RarReadOnlyStorageBackend(str(_RAR_FIXTURE)).read_file("stest2.txt")

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.shutil.which",
        lambda _value: "/fake/unrar",
    )

    class CorruptProcess:
        def __init__(self, _arguments, **kwargs):
            del kwargs
            self.stdout = io.BytesIO(
                bytes((baseline[0] ^ 0xFF,)) + baseline[1:]
            )
            self.stderr = io.BytesIO()

        def wait(self, timeout=None):
            assert timeout is not None
            return 0

        def kill(self):
            raise AssertionError("successful process completion must not be killed")

        def poll(self):
            return 0

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.subprocess.Popen",
        CorruptProcess,
    )
    store = RarReadOnlyStorageBackend(str(_RAR_FIXTURE))

    with pytest.raises(api.StorageIntegrityError, match="CRC-32"):
        store.read_file("stest1.txt")


def test_rar_bounds_member_total_ratio_and_all_entry_count() -> None:
    with pytest.raises(api.StorageUnsupportedOperation, match="declared size"):
        RarReadOnlyStorageBackend(
            str(_RAR_FIXTURE),
            max_member_bytes=1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="total expanded size"):
        RarReadOnlyStorageBackend(
            str(_RAR_FIXTURE),
            max_total_uncompressed_bytes=3000,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="expansion ratio"):
        RarReadOnlyStorageBackend(
            str(_RAR_FIXTURE),
            max_compression_ratio=10,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="inventory exceeds 1"):
        RarReadOnlyStorageBackend(
            str(_RAR_FIXTURE),
            max_inventory_entries=1,
        ).startup()


def test_rar_external_output_cannot_exceed_indexed_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = RarReadOnlyStorageBackend(str(_RAR_FIXTURE)).read_file("stest2.txt")
    killed = False

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.shutil.which",
        lambda _value: "/fake/unrar",
    )

    class OversizedProcess:
        def __init__(self, _arguments, **kwargs):
            del kwargs
            self.stdout = io.BytesIO(baseline + b"!")
            self.stderr = io.BytesIO()

        def wait(self, timeout=None):
            assert timeout is not None
            return 0

        def kill(self):
            nonlocal killed
            killed = True

        def poll(self):
            return -9 if killed else 0

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.subprocess.Popen",
        OversizedProcess,
    )
    store = RarReadOnlyStorageBackend(str(_RAR_FIXTURE))

    with pytest.raises(api.StorageIntegrityError, match="exceeds the indexed"):
        store.read_file("stest1.txt")

    assert killed


def test_rar_safety_policy_is_durable() -> None:
    store = RarReadOnlyStorageBackend(
        str(_RAR_FIXTURE),
        max_member_bytes=4096,
        max_total_uncompressed_bytes=8192,
        max_compression_ratio=50,
        max_path_bytes=2048,
    )

    options = dict(store.configuration.backend_options)
    assert options["max_member_bytes"] == 4096
    assert options["max_total_uncompressed_bytes"] == 8192
    assert options["max_compression_ratio"] == 50.0
    assert options["max_path_bytes"] == 2048
    assert store.characteristics.temporary_space is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    assert store.characteristics.limitation("bounded_rar_expansion") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_rar5_stored_members_and_unicode_paths_are_readable(
    tmp_path: pathlib.Path,
) -> None:
    pytest.importorskip("rarfile")
    path = tmp_path / "rar5.rar"
    path.write_bytes(base64.b64decode(_RAR5_SUBDIRS))
    store = RarReadOnlyStorageBackend(str(path))

    assert {location.key for location in store.iter_locations()} == {
        "sub/dir1/file1.txt",
        "sub/dir2/file2.txt",
        "sub/with space/long fn.txt",
        "sub/üȵĩöḋè/file.txt",
    }
    assert store.read_file("sub/üȵĩöḋè/file.txt") == b"file\n"
    assert store.read_file("sub/with space/long fn.txt", offset=2, length=4) == b"ng f"


def test_rar5_missing_modern_parser_is_actionable(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "rar5.rar"
    path.write_bytes(base64.b64decode(_RAR5_SUBDIRS))

    def unavailable(name: str):
        if name == "rarfile":
            raise ImportError("not installed")
        raise AssertionError(name)

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.importlib.import_module",
        unavailable,
    )

    with pytest.raises(api.StorageUnsupportedOperation, match="archives"):
        RarReadOnlyStorageBackend(str(path)).startup()


def test_rar3_retains_the_embedded_parser_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(name: str):
        if name == "rarfile":
            raise ImportError("not installed")
        raise AssertionError(name)

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.importlib.import_module",
        unavailable,
    )
    store = RarReadOnlyStorageBackend(str(_RAR_FIXTURE))

    assert store.read_file("stest2.txt", offset=2, length=4) == b"0\n00"


def test_rar5_symbolic_links_reject_the_archive(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("rarfile")
    path = tmp_path / "rar5-links.rar"
    path.write_bytes(base64.b64decode(_RAR5_SYMLINKS))
    with pytest.raises(api.StorageUnsupportedOperation, match="symbolic-link"):
        RarReadOnlyStorageBackend(str(path)).startup()


def test_rar_preserves_embedded_unicode_names_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_names = {"уииоотивл.txt", "𝐀𝐁𝐁𝐂.txt"}

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.rar.shutil.which",
        lambda _value: "/fake/unrar",
    )

    class FakeProcess:
        def __init__(self, _arguments, **kwargs):
            del kwargs
            self.stdout = io.BytesIO(b"1\n")
            self.stderr = io.BytesIO()

        def wait(self, timeout=None):
            assert timeout is not None
            return 0

        def kill(self):
            raise AssertionError("successful extraction must not be killed")

        def poll(self):
            return 0

    monkeypatch.setattr("LiuXin_alpha.storage.drivers.rar.subprocess.Popen", FakeProcess)
    store = RarReadOnlyStorageBackend(str(_RAR_UNICODE_FIXTURE))

    discovered = {location.key for location in store.iter_locations()}

    assert discovered == expected_names
    for key in expected_names:
        assert store.locate(key).key == key
        assert store.stat_file(key).hints.suggested_filename == key
        assert store.read_file(key) == b"1\n"


def test_archive_plugins_build_from_registry_and_persist_options(
    tmp_path: pathlib.Path,
) -> None:
    zip_path = tmp_path / "registry.zip"
    tar_path = tmp_path / "registry.tar.gz"
    configurations = (
        api.StoreConfiguration(
            store_uuid=__import__("uuid").uuid4(),
            store_name="zip",
            store_kind="zip_writable",
            store_root_uri=zip_path.resolve().as_uri(),
            backend_options=(("compression", "stored"), ("deterministic", True)),
        ),
        api.StoreConfiguration(
            store_uuid=__import__("uuid").uuid4(),
            store_name="tar",
            store_kind="tar_writable",
            store_root_uri=tar_path.resolve().as_uri(),
            backend_options=(("compression", "gz"), ("deterministic", True)),
        ),
    )

    stores = [DEFAULT_BACKEND_REGISTRY.build(item) for item in configurations]

    assert stores[0].store_bytes(b"zip", location="book.epub").size == 3
    assert stores[1].store_bytes(b"tar", location="book.epub").size == 3
    assert stores[0].configuration is configurations[0]
    assert stores[1].configuration is configurations[1]


@pytest.mark.parametrize("format_name", ["zip", "tar"])
def test_archive_store_ingests_end_to_end_without_extraction_tree(
    tmp_path: pathlib.Path,
    format_name: str,
) -> None:
    payload = b"archive ingest payload"
    if format_name == "zip":
        path = tmp_path / "source.zip"
        _write_zip(path, {"books/source.epub": payload})
        source = ZipReadOnlyStorageBackend(str(path))
    else:
        path = tmp_path / "source.tar.gz"
        _write_tar(path, {"books/source.epub": payload}, mode="w:gz")
        source = TarReadOnlyStorageBackend(str(path))
    destination = FilesystemStore(tmp_path / "destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok
    assert report.ingested_files == 1
    [item] = report.items
    assert item.source_info.location.key == "books/source.epub"
    assert manager.read_file(item.result.asset_record) == payload
    assert not (tmp_path / "books").exists()
