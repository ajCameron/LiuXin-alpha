"""
Provide test calibre zipfile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test calibre zipfile through a consuming regression::

        python -m pytest -q tests/utils/test_calibre_zipfile.py
"""

from __future__ import annotations

import io
import os
import struct
import zlib
from pathlib import Path

import pytest

from LiuXin_alpha.utils.libraries import calibre_zipfile as cz


def _make_zip_bytes(entries: list[tuple[str, bytes]], *, compression: int = cz.ZIP_DEFLATED) -> bytes:
    """
    Perform the make zip bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  make zip bytes through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param entries: Value supplied for entries under the utility contract.
    :param compression: Value supplied for compression under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    bio = io.BytesIO()
    z = cz.ZipFile(bio, "w", compression=compression)
    try:
        for name, data in entries:
            z.writestr(name, data, compression=compression)
    finally:
        z.close()
    return bio.getvalue()


def _corrupt_first(data: bytes, needle: bytes, replacement: bytes) -> bytes:
    """
    Perform the corrupt first utility operation under explicit compatibility rules.

    Example:
        Exercise  corrupt first through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param data: Value supplied for data under the utility contract.
    :param needle: Value supplied for needle under the utility contract.
    :param replacement: Value supplied for replacement under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    idx = data.find(needle)
    assert idx >= 0, "needle not found in test fixture"
    return data[:idx] + replacement + data[idx + len(needle) :]


def test_is_zipfile_path_and_fileobj(tmp_path: Path) -> None:
    """
    Perform the test is zipfile path and fileobj utility operation under explicit compatibility rules.

    Example:
        Exercise test is zipfile path and fileobj through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "t.zip"
    p.write_bytes(_make_zip_bytes([("a.txt", b"hello")]))

    assert cz.is_zipfile(str(p)) is True
    assert cz.is_zipfile(os.fspath(p)) is True

    with p.open("rb") as f:
        assert cz.is_zipfile(f) is True


@pytest.mark.parametrize(
    "payload",
    [
        b"not a zip at all",
        b"PK\x03\x04" + b"\x00" * 10,  # truncated local header
        b"PK\x05\x06" + b"\x00" * 10,  # truncated end record
    ],
)
def test_is_zipfile_false_for_obvious_invalid(payload: bytes, tmp_path: Path) -> None:
    """
    Perform the test is zipfile false for obvious invalid utility operation under explicit compatibility rules.

    Example:
        Exercise test is zipfile false for obvious invalid through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param payload: Value supplied for payload under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "bad.zip"
    p.write_bytes(payload)
    assert cz.is_zipfile(p) is False


def test_open_invalid_raises(tmp_path: Path) -> None:
    """
    Perform the test open invalid raises utility operation under explicit compatibility rules.

    Example:
        Exercise test open invalid raises through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "bad.zip"
    p.write_bytes(b"definitely not zip")

    with pytest.raises(cz.BadZipfile):
        cz.ZipFile(p, "r")


def test_open_truncated_valid_archive_raises(tmp_path: Path) -> None:
    """
    Perform the test open truncated valid archive raises utility operation under explicit compatibility rules.

    Example:
        Exercise test open truncated valid archive raises through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    good = _make_zip_bytes([("a.txt", b"hello"), ("b.txt", b"world")])
    truncated = good[:-20]

    p = tmp_path / "trunc.zip"
    p.write_bytes(truncated)

    assert cz.is_zipfile(p) is False
    with pytest.raises(cz.BadZipfile):
        cz.ZipFile(p, "r")


def test_open_bad_central_directory_signature_raises(tmp_path: Path) -> None:
    """
    Perform the test open bad central directory signature raises utility operation under explicit compatibility rules.

    Example:
        Exercise test open bad central directory signature raises through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    good = _make_zip_bytes([("a.txt", b"hello")])
    corrupted = _corrupt_first(good, b"PK\x01\x02", b"PX\x01\x02")

    p = tmp_path / "corrupt.zip"
    p.write_bytes(corrupted)

    with pytest.raises(cz.BadZipfile):
        cz.ZipFile(p, "r")


def test_write_read_roundtrip_bytesio() -> None:
    """
    Perform the test write read roundtrip bytesio utility operation under explicit compatibility rules.

    Example:
        Exercise test write read roundtrip bytesio through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    bio = io.BytesIO()
    z = cz.ZipFile(bio, "w")
    z.writestr("hello.txt", b"world")
    z.writestr("folder/nested.bin", b"\x00\x01\x02")
    z.close()

    bio.seek(0)
    zr = cz.ZipFile(bio, "r")
    assert set(zr.namelist()) == {"hello.txt", "folder/nested.bin"}
    assert zr.read("hello.txt") == b"world"
    assert zr.read("folder/nested.bin") == b"\x00\x01\x02"
    zr.close()


def test_unicode_filename_roundtrip_and_flag(tmp_path: Path) -> None:
    """
    Perform the test unicode filename roundtrip and flag utility operation under explicit compatibility rules.

    Example:
        Exercise test unicode filename roundtrip and flag through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "u.zip"

    z = cz.ZipFile(p, "w")
    z.writestr("café.txt", b"bonjour")
    z.close()

    r = cz.ZipFile(p, "r")
    assert "café.txt" in r.namelist()
    assert r.read("café.txt") == b"bonjour"
    info = r.getinfo("café.txt")
    assert (info.flag_bits & 0x800) == 0x800
    r.close()


def test_read_raw_deflated_roundtrip(tmp_path: Path) -> None:
    """
    Perform the test read raw deflated roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test read raw deflated roundtrip through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "raw.zip"

    original = b"A" * 10000 + b"B" * 10000
    z = cz.ZipFile(p, "w")
    z.writestr("data.bin", original, compression=cz.ZIP_DEFLATED)
    z.close()

    r = cz.ZipFile(p, "r")
    raw = r.read_raw("data.bin")
    assert raw != original

    inflated = zlib.decompress(raw, -15)
    assert inflated == original
    r.close()


def test_read_raw_stored_matches_original(tmp_path: Path) -> None:
    """
    Perform the test read raw stored matches original utility operation under explicit compatibility rules.

    Example:
        Exercise test read raw stored matches original through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "raw_store.zip"

    original = b"no compression please"
    z = cz.ZipFile(p, "w")
    z.writestr("plain.txt", original, compression=cz.ZIP_STORED)
    z.close()

    r = cz.ZipFile(p, "r")
    raw = r.read_raw("plain.txt")
    assert raw == original
    r.close()


def test_open_returns_stream_and_reads_in_chunks(tmp_path: Path) -> None:
    """
    Perform the test open returns stream and reads in chunks utility operation under explicit compatibility rules.

    Example:
        Exercise test open returns stream and reads in chunks through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "chunks.zip"

    payload = b"0123456789" * 1000
    z = cz.ZipFile(p, "w")
    z.writestr("big.txt", payload)
    z.close()

    r = cz.ZipFile(p, "r")
    f = r.open("big.txt", "r")
    try:
        out = b""
        while True:
            chunk = f.read(123)
            if not chunk:
                break
            out += chunk
        assert out == payload
    finally:
        f.close()
        r.close()


def test_open_universal_newlines_translates(tmp_path: Path) -> None:
    """
    Perform the test open universal newlines translates utility operation under explicit compatibility rules.

    Example:
        Exercise test open universal newlines translates through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    p = tmp_path / "nl.zip"
    payload = b"1\r\n2\r3\n4"

    z = cz.ZipFile(p, "w")
    z.writestr("nl.txt", payload)
    z.close()

    r = cz.ZipFile(p, "r")
    f = r.open("nl.txt", "U")
    try:
        assert f.readline() == b"1\n"
        assert f.readline() == b"2\n"
        assert f.readline() == b"3\n"
        assert f.readline() == b"4"
    finally:
        f.close()
        r.close()


def test_replace_and_replacestr(tmp_path: Path) -> None:
    """
    Perform the test replace and replacestr utility operation under explicit compatibility rules.

    Example:
        Exercise test replace and replacestr through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zpath = tmp_path / "rep.zip"
    z = cz.ZipFile(zpath, "w")
    z.writestr("a.txt", b"old")
    z.writestr("b.txt", b"stay")
    z.close()

    newfile = tmp_path / "new_a.txt"
    newfile.write_bytes(b"new")

    z = cz.ZipFile(zpath, "a")
    z.replace(str(newfile), "a.txt")
    z.close()

    r = cz.ZipFile(zpath, "r")
    assert r.read("a.txt") == b"new"
    assert r.read("b.txt") == b"stay"
    r.close()

    z = cz.ZipFile(zpath, "a")
    info = z.getinfo("a.txt")
    z.replacestr(info, b"newer")
    z.close()

    r = cz.ZipFile(zpath, "r")
    assert r.read("a.txt") == b"newer"
    r.close()


def test_delete_removes_first_duplicate_instance(tmp_path: Path) -> None:
    """
    Perform the test delete removes first duplicate instance utility operation under explicit compatibility rules.

    Example:
        Exercise test delete removes first duplicate instance through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zpath = tmp_path / "dupe.zip"

    z = cz.ZipFile(zpath, "w")
    z.writestr("d.txt", b"first")
    z.writestr("d.txt", b"second")
    assert z.namelist() == ["d.txt", "d.txt"]
    z.close()

    z = cz.ZipFile(zpath, "a")
    z.delete("d.txt")
    z.close()

    r = cz.ZipFile(zpath, "r")
    # One duplicate should remain
    assert r.namelist() == ["d.txt"]
    assert r.read("d.txt") in (b"first", b"second")
    r.close()


def test_append_mode_preserves_old_and_adds_new(tmp_path: Path) -> None:
    """
    Perform the test append mode preserves old and adds new utility operation under explicit compatibility rules.

    Example:
        Exercise test append mode preserves old and adds new through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zpath = tmp_path / "append.zip"

    z = cz.ZipFile(zpath, "w")
    z.writestr("old.txt", b"OLD")
    z.close()

    z = cz.ZipFile(zpath, "a")
    z.writestr("new.txt", b"NEW")
    z.close()

    r = cz.ZipFile(zpath, "r")
    assert set(r.namelist()) == {"old.txt", "new.txt"}
    assert r.read("old.txt") == b"OLD"
    assert r.read("new.txt") == b"NEW"
    r.close()


# Todo: stdlib zipfile tests to here

def test_extractall_sanitizes_path_traversal(tmp_path: Path) -> None:
    """
    Perform the test extractall sanitizes path traversal utility operation under explicit compatibility rules.

    Example:
        Exercise test extractall sanitizes path traversal through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zpath = tmp_path / "slip.zip"
    out = tmp_path / "out"

    z = cz.ZipFile(zpath, "w")
    z.writestr("../evil.txt", b"E1")
    z.writestr("sub/../../evil2.txt", b"E2")
    z.writestr("/abs/evil3.txt", b"E3")
    z.close()

    r = cz.ZipFile(zpath, "r")
    r.extractall(path=str(out))
    r.close()

    assert (out / "evil.txt").read_bytes() == b"E1"
    assert (out / "sub" / "evil2.txt").read_bytes() == b"E2"
    assert (out / "abs" / "evil3.txt").read_bytes() == b"E3"

    # Nothing should have escaped the extraction root
    assert not (tmp_path / "evil.txt").exists()
    assert not (tmp_path / "evil2.txt").exists()


def test_extract_rejects_empty_sanitized_name(tmp_path: Path) -> None:
    """
    Perform the test extract rejects empty sanitized name utility operation under explicit compatibility rules.

    Example:
        Exercise test extract rejects empty sanitized name through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    zpath = tmp_path / "badname.zip"
    out = tmp_path / "out"

    z = cz.ZipFile(zpath, "w")
    z.writestr("..", b"nope")
    z.close()

    r = cz.ZipFile(zpath, "r")
    with pytest.raises(cz.BadZipfile):
        r.extract("..", path=str(out))
    r.close()


def test_safe_replace_updates_zipstream_in_place() -> None:
    """
    Perform the test safe replace updates zipstream in place utility operation under explicit compatibility rules.

    Example:
        Exercise test safe replace updates zipstream in place through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    bio = io.BytesIO()
    z = cz.ZipFile(bio, "w")
    z.writestr("a.txt", b"aaa")
    z.writestr("b.txt", b"bbb")
    z.close()

    bio.seek(0)
    cz.safe_replace(bio, "a.txt", b"REPL")

    bio.seek(0)
    r = cz.ZipFile(bio, "r")
    assert r.read("a.txt") == b"REPL"
    assert r.read("b.txt") == b"bbb"
    r.close()


def test_safe_replace_add_missing(tmp_path: Path) -> None:
    """
    Perform the test safe replace add missing utility operation under explicit compatibility rules.

    Example:
        Exercise test safe replace add missing through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    bio = io.BytesIO()
    z = cz.ZipFile(bio, "w")
    z.writestr("a.txt", b"aaa")
    z.close()

    bio.seek(0)
    cz.safe_replace(bio, "missing.txt", b"M", add_missing=True)

    bio.seek(0)
    r = cz.ZipFile(bio, "r")
    assert set(r.namelist()) == {"a.txt", "missing.txt"}
    assert r.read("missing.txt") == b"M"
    r.close()


def test_add_dir_recursive(tmp_path: Path) -> None:
    """
    Perform the test add dir recursive utility operation under explicit compatibility rules.

    Example:
        Exercise test add dir recursive through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    root = tmp_path / "src"
    root.mkdir()
    (root / "a.txt").write_text("A", encoding="utf-8")
    sub = root / "sub"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"B")

    zpath = tmp_path / "dir.zip"
    z = cz.ZipFile(zpath, "w")
    z.add_dir(str(root), prefix="root")
    z.close()

    r = cz.ZipFile(zpath, "r")
    names = set(r.namelist())
    assert "root/a.txt" in names
    assert "root/sub/b.bin" in names
    assert r.read("root/a.txt") == b"A"
    assert r.read("root/sub/b.bin") == b"B"
    r.close()


def test_pack_format_signatures_are_bytes() -> None:
    # Smoke test: core signatures should be bytes-like, otherwise struct.pack will fail.
    """
    Perform the test pack format signatures are bytes utility operation under explicit compatibility rules.

    Example:
        Exercise test pack format signatures are bytes through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert isinstance(cz.stringFileHeader, (bytes, bytearray))
    assert isinstance(cz.stringCentralDir, (bytes, bytearray))
    assert isinstance(cz.stringEndArchive, (bytes, bytearray))


def test_struct_end_record_can_be_packed() -> None:
    # Another smoke: ensure the struct format and signature remain coherent.
    """
    Perform the test struct end record can be packed utility operation under explicit compatibility rules.

    Example:
        Exercise test struct end record can be packed through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    packed = struct.pack(cz.structEndArchive, cz.stringEndArchive, 0, 0, 0, 0, 0, 0, 0)
    assert packed.startswith(cz.stringEndArchive)
