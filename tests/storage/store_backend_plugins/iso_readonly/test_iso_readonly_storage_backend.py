"""
Exercise ISO namespace selection, direct/UDF reads, limits, and Store compatibility.

Direct fixtures use a dependency-free image builder. Real bridge fixtures require
optional pycdlib; injected import/extraction substitutes isolate fallback and spool
failure behavior. Tests distinguish malformed placeholders from valid images and
advertised configuration from exercised policy. POSIX byte names have a platform gate.
"""

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
    """
    Build a real Joliet fixture with two nonempty files and a nested empty file.

    Uses the dependency-free image builder and returns its output path.

    Example:
        >>> _basic_image(tmp_path).name  # doctest: +SKIP
        'library.iso'


    :param tmp_path: Temporary directory receiving the generated image.
    :return: Path to the successfully written image fixture.
    """
    return build_joliet_iso(
        tmp_path / "library.iso",
        {
            "book one.txt": b"hello",
            "nested/book_two.epub": b"EPUB-DATA",
            "nested/empty.bin": b"",
        },
    )


def _udf_bridge_image(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Write a real ISO/UDF 2.60 bridge with a Unicode UDF directory and filename.

    The ISO namespace uses BOOKS/BOOK.EPUB while UDF uses the book emoji and naïve.epub. Missing
    pycdlib skips the calling test; successful writing is followed by image.close.

    Example:
        >>> _udf_bridge_image(tmp_path).name  # doctest: +SKIP
        'udf-bridge.iso'


    :param tmp_path: Temporary directory receiving the generated image.
    :return: Path to the successfully written image fixture.
    """
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
    """
    Write a real ISO/Rock Ridge/UDF bridge with intentionally different namespace names.

    The Rock Ridge rr-books/rr-book.epub path and UDF emoji/udf-book.epub path refer to the same
    payload. Missing pycdlib skips the invoking case.

    Example:
        >>> _rock_ridge_udf_bridge_image(tmp_path).name  # doctest: +SKIP
        'rock-ridge-udf-bridge.iso'


    :param tmp_path: Temporary directory receiving the generated image.
    :return: Path to the successfully written image fixture.
    """
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
    """
    Check real Joliet startup, file count, durable defaults, registry aliases, and advertised
    limits.

    Configuration/characteristic assertions describe declared policy rather than independently
    exercising each limit.

    Example:
        >>> test_iso_readonly_status_configuration_and_registry(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Exercise real ISO locations, size/time/version hints, complete/prefix inventory,
    full/ranged/empty reads, and SHA-256 computation.

    Example:
        >>> test_iso_readonly_locate_stat_range_digest_and_inventory(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Read a real primary-only ISO fixture and require the iso9660 namespace selection.

    Example:
        >>> test_iso_readonly_falls_back_to_primary_iso9660_namespace(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    image = build_iso9660_iso(
        tmp_path / "primary.iso",
        {"BOOKS/NOVEL.EPUB": b"primary-volume"},
    )
    store = IsoReadOnlyStorageBackend(str(image))

    status = store.startup()

    assert dict(status.details)["namespace"] == "iso9660"
    assert store.read_file("BOOKS/NOVEL.EPUB") == b"primary-volume"


def test_iso_readonly_prefers_and_reads_the_udf_namespace(tmp_path) -> None:
    """
    Select UDF over a primary ISO bridge and read a staged range through its Unicode member path.

    Also checks complete selected-namespace inventory, aware modification metadata, and the spooling
    limitation.

    Example:
        >>> test_iso_readonly_prefers_and_reads_the_udf_namespace(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Require Rock Ridge selection and payload access when a real bridge also exposes UDF names.

    Example:
        >>> test_iso_readonly_keeps_rock_ridge_priority_over_udf(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    image = _rock_ridge_udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))

    assert dict(store.startup().details)["namespace"] == "rock-ridge"
    assert store.read_file("rr-books/rr-book.epub") == b"Rock Ridge payload"


def test_iso_readonly_can_disable_udf_and_persists_the_policy(tmp_path) -> None:
    """
    Disable UDF on a real bridge, read the primary ISO path, and check persisted enable/member-limit
    options.

    Example:
        >>> test_iso_readonly_can_disable_udf_and_persists_the_policy(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Inject a missing pycdlib import after fixture creation and retain readable primary ISO fallback.

    The bridge itself requires pycdlib to construct; the injected import isolates runtime fallback.

    Example:
        >>> test_iso_udf_bridge_falls_back_when_optional_parser_is_absent(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :param monkeypatch: Pytest fixture restoring injected optional-parser seams after the test.
    :return: None after the stated regression assertions pass.
    """
    image = _udf_bridge_image(tmp_path)

    def unavailable(name: str):
        """
        Simulate only the pycdlib import failure and reject unexpected imports.

        Example:
            >>> unavailable("pycdlib")  # doctest: +SKIP


        :param name: Requested module name.
        :return: Never returns: raises ImportError for pycdlib or AssertionError for any other request.
        """
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
    """
    Read literal semicolon-digit directory/file names from real Joliet and Rock Ridge fixtures.

    The builders encode each namespace appropriately; assertions cover the resulting caller-visible
    spelling.

    Example:
        >>> test_iso_names_retain_literal_terminal_version_like_text(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Exercise the shared Unicode inventory/address/full-range-read/hint contract against a real
    Joliet image.

    URI round-trip checks are not enabled by this call.

    Example:
        >>> test_iso_readonly_applies_generic_unicode_torture_contract(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    expected = {case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES}
    image = build_joliet_iso(tmp_path / "unicode.iso", expected)
    store = IsoReadOnlyStorageBackend(str(image))

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)

    assert {result.location.key for result in results} == set(expected)


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX byte-name contract")
def test_iso_rock_ridge_reads_undecodable_filename_bytes(tmp_path) -> None:
    """
    Preserve undecodable POSIX filename bytes through Rock Ridge inventory, hints, and content
    reads.

    The platform marker skips this filesystem-encoding contract outside POSIX.

    Example:
        >>> test_iso_rock_ridge_reads_undecodable_filename_bytes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Read 24 full/ranged requests through one Store using eight threads and compare ordered payloads.

    Example:
        >>> test_iso_readonly_supports_concurrent_reads(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    image = _basic_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))
    requests = [
        ("book one.txt", 0, None, b"hello"),
        ("nested/book_two.epub", 0, None, b"EPUB-DATA"),
        ("nested/book_two.epub", 2, 4, b"UB-D"),
    ] * 8

    def read_one(request):
        """
        Read the requested key/range through the enclosing shared Store, leaving expected bytes for
        the final assertion.

        Example:
            >>> read_one(("book one.txt", 0, None, b"hello"))  # doctest: +SKIP
            b'hello'


        :param request: Tuple of member key, offset, optional length, and expected payload.
        :return: Bytes returned by the Store for that request.
        """
        key, offset, length, _expected = request
        return store.read_file(key, offset=offset, length=length)

    with ThreadPoolExecutor(max_workers=8) as executor:
        observed = list(executor.map(read_one, requests))

    assert observed == [expected for _key, _offset, _length, expected in requests]


def test_iso_readonly_enforces_image_version_on_open(tmp_path) -> None:
    """
    Reject an earlier image version after appending a sector changes the real container metadata.

    Example:
        >>> test_iso_readonly_enforces_image_version_on_open(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject write/delete calls and representative empty, absolute, parent, duplicate-slash, and
    backslash keys.

    Example:
        >>> test_iso_readonly_rejects_mutation_and_noncanonical_paths(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Distinguish a truncated descriptor read from a complete zero-filled non-ISO image using typed
    failures.

    Example:
        >>> test_iso_readonly_reports_truncated_and_non_iso_images(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Require an explicit unsupported UDF-only error for synthetic recognition markers without an ISO
    bridge.

    The placeholder is not a fully valid UDF image; the assertion pins this detected boundary and
    its advertised limitation.

    Example:
        >>> test_iso_readonly_reports_udf_only_boundary_explicitly(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a real image after changing only the big-endian logical-block-size field to disagree.

    Example:
        >>> test_iso_readonly_rejects_corrupt_both_endian_fields(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    image = _basic_image(tmp_path)
    payload = bytearray(image.read_bytes())
    descriptor = 16 * 2048
    payload[descriptor + 130 : descriptor + 132] = (1024).to_bytes(2, "big")
    image.write_bytes(payload)

    with pytest.raises(api.StorageIntegrityError, match="byte orders disagree"):
        IsoReadOnlyStorageBackend(str(image)).startup()


def test_iso_readonly_enforces_inventory_and_directory_limits(tmp_path) -> None:
    """
    Require distinct typed startup failures for all-entry and per-directory byte caps on a real
    Joliet image.

    Example:
        >>> test_iso_readonly_enforces_inventory_and_directory_limits(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    image = _basic_image(tmp_path)

    with pytest.raises(api.StorageUnsupportedOperation, match="inventory limit"):
        IsoReadOnlyStorageBackend(str(image), max_inventory_entries=1).startup()
    with pytest.raises(api.StorageUnavailable, match="directory.*byte limit"):
        IsoReadOnlyStorageBackend(str(image), max_directory_bytes=1024).startup()


def test_iso_readonly_bounds_member_total_and_path_bytes(tmp_path) -> None:
    """
    Enforce individual logical size, aggregate size, and whole-key byte limits during direct ISO
    inventory.

    The member limit named max_udf_member_bytes also applies to this non-UDF fixture.

    Example:
        >>> test_iso_readonly_bounds_member_total_and_path_bytes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject a fake extractor offering one byte beyond a real UDF member's indexed size.

    The real bridge supplies metadata; injected extraction isolates the bounded writer failure.

    Example:
        >>> test_iso_udf_spool_rejects_output_beyond_indexed_size(tmp_path, monkeypatch)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :param monkeypatch: Pytest fixture restoring injected optional-parser seams after the test.
    :return: None after the stated regression assertions pass.
    """
    image = _udf_bridge_image(tmp_path)
    store = IsoReadOnlyStorageBackend(str(image))
    info = store.stat_file("📚/naïve.epub")

    class FakeImage:
        """
        Substitute a parser that offers oversized output using the enclosing real member size.

        Opening and closing are no-ops; no image is parsed by this double.

        Example:
            >>> image = FakeImage()  # doctest: +SKIP
        """
        def open(self, _path):
            """
            Accept the requested image path without opening a resource.

            Example:
                >>> image.open("fixture.iso")  # doctest: +SKIP


            :param _path: Ignored image pathname supplied by the driver.
            :return: None without filesystem access.
            """
            return None

        def get_file_from_iso_fp(self, destination, *, udf_path):
            """
            Offer one oversized chunk to the driver's bounded destination.

            The UDF path is ignored; the enclosing indexed size determines the chunk length.

            Example:
                >>> image.get_file_from_iso_fp(destination, udf_path="/book")  # doctest: +SKIP


            :param destination: Bounded sink expected to reject the oversized write.
            :param udf_path: Ignored extraction path accepted for call compatibility.
            :return: None only if the destination accepts the write; the test expects StorageIntegrityError to propagate.
            """
            del udf_path
            destination.write(b"x" * (info.size + 1))

        def close(self):
            """
            Accept parser cleanup without owning or closing any resource.

            Example:
                >>> image.close()  # doctest: +SKIP


            :return: None without changing state.
            """
            return None

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.iso._require_pycdlib",
        lambda _path: SimpleNamespace(PyCdlib=FakeImage),
    )

    with pytest.raises(api.StorageIntegrityError, match="exceeded its indexed size"):
        store.read_file(info)


def test_iso_readonly_missing_image_has_actionable_typed_error(tmp_path) -> None:
    """
    Require construction to identify a missing image through StorageNotFound with operation and
    filename context.

    Example:
        >>> test_iso_readonly_missing_image_has_actionable_typed_error(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
    missing = tmp_path / "missing-library.iso"

    with pytest.raises(api.StorageNotFound) as observed:
        IsoReadOnlyStorageBackend(str(missing))

    message = str(observed.value)
    assert "ISO configure failed" in message
    assert "missing-library.iso" in message


def test_registry_builds_iso_from_file_uri(tmp_path) -> None:
    """
    Build the ISO alias from a file-URI configuration and read a real member through the resulting
    Store.

    Example:
        >>> test_registry_builds_iso_from_file_uri(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory holding real or intentionally malformed image fixtures.
    :return: None after the stated regression assertions pass.
    """
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
