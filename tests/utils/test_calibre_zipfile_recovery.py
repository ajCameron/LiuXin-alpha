"""
Provide test calibre zipfile recovery utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test calibre zipfile recovery through a consuming regression::

        python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py
"""

from __future__ import annotations

import io
import inspect
import random
import struct
from dataclasses import dataclass

import pytest

from LiuXin_alpha.utils.libraries import calibre_zipfile as cz


@dataclass(frozen=True)
class _LocalEntry:
    """
    Provide the LocalEntry utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  LocalEntry through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py
    """
    name: str
    header_offset: int
    data_offset: int
    data_end: int
    compress_size: int
    file_size: int
    flag_bits: int
    compress_type: int


def _make_zip_bytes(entries: list[tuple[str, bytes]], *, compression: int) -> bytes:
    """
    Perform the make zip bytes utility operation under explicit compatibility rules.

    Example:
        Exercise  make zip bytes through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


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


def _scan_local_headers(data: bytes, *, start_offset: int = 0) -> list[_LocalEntry]:
    """
    Sequentially scan local file headers.

    Example:
        Exercise  scan local headers through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param data: Value supplied for data under the utility contract.
    :param start_offset: Value supplied for start offset under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    out: list[_LocalEntry] = []
    off = start_offset

    # Local header is always 30 bytes.
    header_sz = struct.calcsize(cz.structFileHeader)

    while off + header_sz <= len(data):
        if data[off : off + 4] != cz.stringFileHeader:
            break

        fheader = struct.unpack(cz.structFileHeader, data[off : off + header_sz])
        flag_bits = int(fheader[cz._FH_GENERAL_PURPOSE_FLAG_BITS])
        compress_type = int(fheader[cz._FH_COMPRESSION_METHOD])
        compress_size = int(fheader[cz._FH_COMPRESSED_SIZE])
        file_size = int(fheader[cz._FH_UNCOMPRESSED_SIZE])
        fname_len = int(fheader[cz._FH_FILENAME_LENGTH])
        extra_len = int(fheader[cz._FH_EXTRA_FIELD_LENGTH])

        header_offset = off
        name_start = off + header_sz
        name_end = name_start + fname_len
        extra_end = name_end + extra_len

        # If data descriptor is used, the local header sizes may be zero.
        # Recovery via local-header scanning typically needs sizes; for these
        # tests, we treat descriptor-using zips as out-of-scope fixtures.
        if flag_bits & 0x08:
            pytest.xfail("Fixture zip uses data descriptors; local scanning needs sizes")

        try:
            name = data[name_start:name_end].decode("utf-8")
        except Exception:
            name = data[name_start:name_end].decode("utf-8", "replace")

        data_offset = extra_end
        data_end = data_offset + compress_size

        out.append(
            _LocalEntry(
                name=name,
                header_offset=header_offset,
                data_offset=data_offset,
                data_end=data_end,
                compress_size=compress_size,
                file_size=file_size,
                flag_bits=flag_bits,
                compress_type=compress_type,
            )
        )

        off = data_end

    if not out:
        raise AssertionError("Failed to parse any local headers in generated fixture")

    return out


def _find_first_local_header(data: bytes) -> int:
    """
    Find first local header under the documented compatibility and safety rules.

    Example:
        Exercise  find first local header through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    off = data.find(cz.stringFileHeader)
    if off < 0:
        raise AssertionError("No local file header signature found in fixture")
    return off


def _open_recovering_zip(fileobj_or_path):
    """
    Open a zip in recovery mode, if supported; otherwise xfail.

    Example:
        Exercise  open recovering zip through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param fileobj_or_path: Value supplied for fileobj or path under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    if hasattr(cz, "recover_zipfile"):
        return cz.recover_zipfile(fileobj_or_path)

    sig = inspect.signature(cz.ZipFile.__init__)
    if "recover" in sig.parameters:
        return cz.ZipFile(fileobj_or_path, "r", recover=True)

    pytest.xfail("Recovery API not implemented (no recover_zipfile and no ZipFile(..., recover=True))")


@pytest.mark.parametrize(
    "compression",
    [
        cz.ZIP_STORED,
        pytest.param(
            cz.ZIP_DEFLATED,
            marks=pytest.mark.skipif(cz.zlib is None, reason="zlib not available"),
        ),
    ],
)
def test_recover_truncated_missing_central_directory_recovers_all_complete_entries(tmp_path, compression: int) -> None:
    # Make a valid archive, then truncate it at the end of the *last* member's data
    # (i.e., remove the entire central directory + end record).
    """
    Perform the test recover truncated missing central directory recovers all complete entries utility operation under explicit compatibility rules.

    Example:
        Exercise test recover truncated missing central directory recovers all complete entries through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param compression: Value supplied for compression under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    entries = [("a.txt", b"A" * 10), ("b.txt", b"B" * 1000), ("c.txt", b"C" * 123)]
    good = _make_zip_bytes(entries, compression=compression)
    locals_ = _scan_local_headers(good)

    cut_at = locals_[-1].data_end
    truncated = good[:cut_at]

    p = tmp_path / "truncated_no_cd.zip"
    p.write_bytes(truncated)

    z = _open_recovering_zip(p)
    try:
        assert set(z.namelist()) == {"a.txt", "b.txt", "c.txt"}
        assert z.read("a.txt") == b"A" * 10
        assert z.read("b.txt") == b"B" * 1000
        assert z.read("c.txt") == b"C" * 123
    finally:
        z.close()


def test_recover_with_prefix_garbage_still_finds_entries(tmp_path) -> None:
    # Self-extracting archives / concatenated files can have junk before the first
    # local header. A recovery scanner should be able to locate the first header
    # and proceed.
    """
    Perform the test recover with prefix garbage still finds entries utility operation under explicit compatibility rules.

    Example:
        Exercise test recover with prefix garbage still finds entries through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    entries = [("a.txt", b"AAA"), ("b.txt", b"BBB")]
    good = _make_zip_bytes(entries, compression=cz.ZIP_STORED)

    prefix = b"JUNK" * 50
    prefixed = prefix + good

    # Truncate away the central directory to force recovery, but keep all member
    # data intact.
    start = _find_first_local_header(prefixed)
    locals_ = _scan_local_headers(prefixed, start_offset=start)
    truncated = prefixed[: locals_[-1].data_end]

    p = tmp_path / "prefixed_truncated.zip"
    p.write_bytes(truncated)

    z = _open_recovering_zip(p)
    try:
        assert set(z.namelist()) == {"a.txt", "b.txt"}
        assert z.read("a.txt") == b"AAA"
        assert z.read("b.txt") == b"BBB"
    finally:
        z.close()


@pytest.mark.parametrize(
    "compression",
    [
        cz.ZIP_STORED,
        pytest.param(
            cz.ZIP_DEFLATED,
            marks=pytest.mark.skipif(cz.zlib is None, reason="zlib not available"),
        ),
    ],
)
def test_recover_truncated_mid_archive_drops_missing_tail_entries(tmp_path, compression: int) -> None:
    """
    Perform the test recover truncated mid archive drops missing tail entries utility operation under explicit compatibility rules.

    Example:
        Exercise test recover truncated mid archive drops missing tail entries through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param compression: Value supplied for compression under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    entries = [("a.txt", b"A" * 10), ("b.txt", b"B" * 1000), ("c.txt", b"C" * 123)]
    good = _make_zip_bytes(entries, compression=compression)
    locals_ = _scan_local_headers(good)

    # Keep only the first two members' data; drop the third + the directory.
    cut_at = locals_[1].data_end
    truncated = good[:cut_at]
    p = tmp_path / "truncated_mid.zip"
    p.write_bytes(truncated)

    z = _open_recovering_zip(p)
    try:
        assert set(z.namelist()) == {"a.txt", "b.txt"}
        assert z.read("a.txt") == b"A" * 10
        assert z.read("b.txt") == b"B" * 1000
        with pytest.raises(KeyError):
            z.getinfo("c.txt")
    finally:
        z.close()


@pytest.mark.parametrize(
    "compression",
    [
        cz.ZIP_STORED,
        pytest.param(
            cz.ZIP_DEFLATED,
            marks=pytest.mark.skipif(cz.zlib is None, reason="zlib not available"),
        ),
    ],
)
def test_recover_truncated_mid_payload_recovers_prefix_or_skips_partial_entry(tmp_path, compression: int) -> None:
    """
    Perform the test recover truncated mid payload recovers prefix or skips partial entry utility operation under explicit compatibility rules.

    Example:
        Exercise test recover truncated mid payload recovers prefix or skips partial entry through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param compression: Value supplied for compression under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    entries = [("a.txt", b"A" * 10), ("b.txt", b"B" * 1000), ("c.txt", b"C" * 123)]
    good = _make_zip_bytes(entries, compression=compression)
    locals_ = _scan_local_headers(good)

    # Truncate in the middle of b.txt's compressed payload.
    b = locals_[1]
    cut_at = b.data_offset + max(1, (b.data_end - b.data_offset) // 2)
    truncated = good[:cut_at]
    p = tmp_path / "truncated_payload.zip"
    p.write_bytes(truncated)

    z = _open_recovering_zip(p)
    try:
        # a.txt must be fully recoverable.
        assert "a.txt" in set(z.namelist())
        assert z.read("a.txt") == b"A" * 10

        # b.txt may be either:
        # 1) omitted entirely (strict recovery), OR
        # 2) present but only partially readable (best-effort recovery).
        if "b.txt" in set(z.namelist()):
            try:
                got = z.read("b.txt")
            except Exception:
                # Accept failures for a partially-present member; recovery is still
                # useful so long as earlier members are intact and the reader does
                # not deadlock.
                got = b""

            if got:
                assert (b"B" * 1000).startswith(got)
    finally:
        z.close()


def test_recover_skips_corrupt_local_header_and_keeps_others(tmp_path) -> None:
    # Stored is easiest for deterministic header parsing.
    """
    Perform the test recover skips corrupt local header and keeps others utility operation under explicit compatibility rules.

    Example:
        Exercise test recover skips corrupt local header and keeps others through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    entries = [("a.txt", b"A" * 10), ("b.txt", b"B" * 20), ("c.txt", b"C" * 30)]
    good = _make_zip_bytes(entries, compression=cz.ZIP_STORED)
    locals_ = _scan_local_headers(good)

    # Corrupt the second local header signature.
    b_off = locals_[1].header_offset
    corrupted = bytearray(good)
    corrupted[b_off : b_off + 4] = b"PX\x03\x04"

    # Truncate away the directory to force recovery via local headers.
    truncated = bytes(corrupted[: locals_[-1].data_end])
    p = tmp_path / "corrupt_header.zip"
    p.write_bytes(truncated)

    z = _open_recovering_zip(p)
    try:
        names = set(z.namelist())
        assert "a.txt" in names
        assert z.read("a.txt") == b"A" * 10

        # b.txt should not be recoverable due to corrupt header.
        assert "b.txt" not in names

        # c.txt might or might not be found depending on how robust the scanner is.
        # If it is found, it should read correctly.
        if "c.txt" in names:
            assert z.read("c.txt") == b"C" * 30
    finally:
        z.close()


def test_recovery_fuzz_cutpoints_monotonic_prefix(tmp_path) -> None:
    # A light "property" style check: as we increase truncation length, recovered
    # name sets should be monotonic (only gain members, never lose previously
    # recoverable complete members).
    """
    Perform the test recovery fuzz cutpoints monotonic prefix utility operation under explicit compatibility rules.

    Example:
        Exercise test recovery fuzz cutpoints monotonic prefix through a consuming regression::

            python -m pytest -q tests/utils/test_calibre_zipfile_recovery.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    rnd = random.Random(0)
    entries = [(f"f{i}.bin", bytes([i]) * (50 + i)) for i in range(10)]
    good = _make_zip_bytes(entries, compression=cz.ZIP_STORED)
    locals_ = _scan_local_headers(good)

    # Consider cutpoints that are within the data region, excluding the central dir.
    cutpoints = sorted({rnd.randint(locals_[0].data_offset, locals_[-1].data_end) for _ in range(12)})

    prev: set[str] = set()
    for i, cut_at in enumerate(cutpoints):
        p = tmp_path / f"fuzz_{i}.zip"
        p.write_bytes(good[:cut_at])
        z = _open_recovering_zip(p)
        try:
            cur = set(z.namelist())
            assert prev.issubset(cur)
            # Any member reported should be *attemptable* to read. Partial members
            # may raise, but recovery must not deadlock/hang.
            for name in cur:
                try:
                    _ = z.read(name)
                except Exception:
                    pass
        finally:
            z.close()
        prev = cur
