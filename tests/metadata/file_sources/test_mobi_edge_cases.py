"""
Exercise MOBI headers, sections, encodings and malformed-record edge cases.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test mobi edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
"""
from __future__ import annotations

import io
import sys
import types
from struct import pack, unpack
from types import SimpleNamespace

import pytest

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.metadata.file_sources import mobi
from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


class _NoSeekTell:
    """
    Provide the NoSeekTell test fixture or double with explicit deterministic behavior.

    Example:
        Exercise NoSeekTell through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
    """
    pass


class _TellBrokenStream(io.BytesIO):
    """
    Provide the TellBrokenStream test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TellBrokenStream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
    """
    name = "tell-broken.mobi"

    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise TellBrokenStream.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")


class _RestoreBrokenStream(io.BytesIO):
    """
    Provide the RestoreBrokenStream test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RestoreBrokenStream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
    """
    name = "restore-broken.mobi"

    def seek(self, pos, whence=0):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise RestoreBrokenStream.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param pos: Value supplied for pos in the focused test operation.
        :param whence: Value supplied for whence in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _HeaderWithCoverOffset:
    """
    Provide the HeaderWithCoverOffset test fixture or double with explicit deterministic behavior.

    Example:
        Exercise HeaderWithCoverOffset through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
    """
    def __init__(self):
        """
        Initialize the HeaderWithCoverOffset test double.

        Example:
            Exercise HeaderWithCoverOffset.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :return: None; the function records state or raises through its assertions.
        """
        self.exth = SimpleNamespace(cover_offset=2)
        self.first_image_index = 5
        self.calls = []

    def section_data(self, index):
        """
        Perform the section data test-helper operation with deterministic inputs.

        Example:
            Exercise HeaderWithCoverOffset.section data through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param index: Value supplied for index in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.calls.append(index)
        return b"cover-offset"


class _HeaderWithoutCoverOffset:
    """
    Provide the HeaderWithoutCoverOffset test fixture or double with explicit deterministic behavior.

    Example:
        Exercise HeaderWithoutCoverOffset through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py
    """
    first_image_index = 7
    exth = SimpleNamespace()

    def __init__(self, raises=False):
        """
        Initialize the HeaderWithoutCoverOffset test double.

        Example:
            Exercise HeaderWithoutCoverOffset.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param raises: Value supplied for raises in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.raises = raises

    def section_data(self, index):
        """
        Perform the section data test-helper operation with deterministic inputs.

        Example:
            Exercise HeaderWithoutCoverOffset.section data through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param index: Value supplied for index in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if self.raises:
            raise RuntimeError("missing section")
        return f"section-{index}".encode()


def _metadata_for_update():
    """
    Perform the metadata for update test-helper operation with deterministic inputs.

    Example:
        Exercise metadata for update through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :return: The deterministic value, row, identity or collection described above.
    """
    mi = calibreMetaInformation("Unicode MOBI — café — 世界 😀", ["Alice Δ", "李白"])
    mi.publisher = "Publisher Ω"
    mi.comments = "Visible comments <div class=\"user_annotations\">drop me</div>"
    mi.isbn = "9780306406157"
    mi.tags = ["fiction", "δοκιμή"]
    mi.pubdate = "2026-05-16T10:20:30+00:00"
    mi.language = "en"
    mi.uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    mi.book_producer = "Producer 測試"
    mi.cover_data = (None, None)
    mi.cover = None
    return mi


def _fake_updater():
    """
    Perform the fake updater test-helper operation with deterministic inputs.

    Example:
        Exercise fake updater through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :return: The deterministic value, row, identity or collection described above.
    """
    updater = object.__new__(mobi.MetadataUpdater)
    updater.type = b"BOOKMOBI"
    updater.codec = "utf-8"
    updater.original_exth_records = {501: b"EBOK", 503: b"old-title", 999: b"keep"}
    updater.timestamp = b"old-timestamp"
    updater.cover_record = None
    updater.thumbnail_record = None
    updater.record0 = bytearray(256)
    updater.exth = b"old-exth"
    updater.calls = []

    def create_exth(*, exth=None, new_title=None):
        """
        Create exth for a focused assertion.

        Example:
            Exercise fake updater.create exth through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param exth: Value supplied for exth in the focused test operation.
        :param new_title: Value supplied for new title in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        updater.calls.append(("create_exth", exth, new_title))

    def fetch_exth_fields():
        """
        Perform the fetch exth fields test-helper operation with deterministic inputs.

        Example:
            Exercise fake updater.fetch exth fields through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        updater.calls.append(("fetchEXTHFields",))

    updater.create_exth = create_exth
    updater.fetchEXTHFields = fetch_exth_fields
    return updater


def _exth_records(exth_blob: bytes) -> dict[int, list[bytes]]:
    """
    Perform the exth records test-helper operation with deterministic inputs.

    Example:
        Exercise exth records through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param exth_blob: Value supplied for exth blob in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    assert exth_blob.startswith(b"EXTH")
    count = unpack(">I", exth_blob[8:12])[0]
    pos = 12
    out: dict[int, list[bytes]] = {}
    for _ in range(count):
        code, size = unpack(">II", exth_blob[pos : pos + 8])
        payload = exth_blob[pos + 8 : pos + size]
        out.setdefault(code, []).append(payload)
        pos += size
    return out


def _has_forbidden_payload_byte(payload: bytes) -> bool:
    """
    Perform the has forbidden payload byte test-helper operation with deterministic inputs.

    Example:
        Exercise has forbidden payload byte through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param payload: Value supplied for payload in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return any(byte < 0x20 and byte not in {0x09, 0x0A, 0x0D} for byte in payload)


def test_mobi_stream_slicer_get_set_update_and_error_edges() -> None:
    """
    Verify mobi stream slicer get set update and error edges.

    Example:
        Exercise test mobi stream slicer get set update and error edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    stream = io.BytesIO(bytearray(b"0123456789abcdef"))
    slicer = mobi.StreamSlicer(stream, start=2, stop=12)
    assert len(slicer) == 10
    assert slicer[0] == b"2"
    assert slicer[1:5] == b"3456"
    assert slicer[1:8:2] == b"3579"
    assert slicer[8:1:-1] == b"9876543"
    assert slicer[20:25] == b""

    slicer[0] = b"X"
    slicer[1:3] = b"YZ"
    slicer[8:1:-1] = b"abcdefg"
    assert stream.getvalue() == b"01Xgfedcbaabcdef"
    with pytest.raises(ValueError):
        slicer[0] = b"too-long"
    with pytest.raises(ValueError):
        slicer[0:2] = b"x"
    with pytest.raises(TypeError):
        _ = slicer["bad"]
    with pytest.raises(TypeError):
        slicer["bad"] = b"x"

    slicer.update([b"abc", b"def"])
    assert stream.getvalue() == b"01abcdef"
    slicer.truncate(4)
    assert stream.getvalue() == b"01ab"


def test_mobi_helpers_for_images_sizes_covers_and_get_metadata_edges(monkeypatch) -> None:
    """
    Verify mobi helpers for images sizes covers and get metadata edges.

    Example:
        Exercise test mobi helpers for images sizes covers and get metadata edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    assert not mobi.is_image(None)
    assert mobi.is_image(b"\x89PNG\r\n\x1a\n" + b"\0" * 64)
    assert mobi._stream_size(_NoSeekTell(), fallback=123) == 123

    stream = io.BytesIO(b"123456789")
    stream.seek(4)
    assert mobi._stream_size(stream) == 9
    assert stream.tell() == 4

    header = _HeaderWithCoverOffset()
    assert mobi._read_cover_from_header(header) == b"cover-offset"
    assert header.calls == [7]
    assert mobi._read_cover_from_header(_HeaderWithoutCoverOffset()) == b"section-7"
    assert mobi._read_cover_from_header(_HeaderWithoutCoverOffset(raises=True)) == b""

    with pytest.raises(TypeError):
        mobi.get_metadata(object())

    expected = calibreMetaInformation("Patched Reader", ["Reader Author"])
    monkeypatch.setattr(mobi, "read_metadata_from_stream", lambda *_args, **_kwargs: expected)
    tell_broken = _TellBrokenStream(b"payload")
    assert mobi.get_metadata(tell_broken) is expected
    restore_broken = _RestoreBrokenStream(b"payload")
    restore_broken.seek(3)
    restore_broken._break_restore = True
    assert mobi.get_metadata(restore_broken) is expected


def test_mobi_topaz_failure_paths_raise_by_default_and_can_opt_into_fallback(monkeypatch) -> None:
    """
    Verify mobi topaz failure paths raise by default and can opt into fallback.

    Example:
        Exercise test mobi topaz failure paths raise by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    events = []
    monkeypatch.setattr(mobi.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))

    fake_topaz = types.ModuleType("LiuXin_alpha.metadata.file_sources.topaz")

    def explode_topaz(_stream):
        """
        Perform the explode topaz test-helper operation with deterministic inputs.

        Example:
            Exercise test mobi topaz failure paths raise by default and can opt into fallback.explode topaz through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


        :param _stream: Value supplied for stream in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("topaz exploded")

    fake_topaz.get_metadata = explode_topaz
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.metadata.file_sources.topaz", fake_topaz)

    with pytest.raises(MobiError):
        mobi.read_metadata_from_stream(io.BytesIO(b"TPZ malformed"), source_name="topaz-case.tpz")

    md = mobi.read_metadata_from_stream(
        io.BytesIO(b"TPZ malformed"),
        source_name="topaz-case.tpz",
        fallback_on_parse_error=True,
    )
    assert md.title == "topaz-case"
    assert _values(md.authors) == ["Unknown"]
    assert any("embedded Topaz" in str(event[0]) for event in events)


def test_mobi_metadata_updater_update_writes_broad_exth_records(monkeypatch) -> None:
    """
    Verify mobi metadata updater update writes broad exth records.

    Example:
        Exercise test mobi metadata updater update writes broad exth records through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    updater = _fake_updater()
    mi = _metadata_for_update()

    monkeypatch.setattr("uuid.uuid4", lambda: "fixed-sync-id")
    updater.update(mi, asin="ASIN-123")

    create_call = next(call for call in updater.calls if call[0] == "create_exth")
    _name, exth_blob, new_title = create_call
    records = _exth_records(exth_blob)

    assert new_title == "Unicode MOBI — café — 世界 😀"
    assert b"Alice" in records[100][0]
    assert b"Publisher" in records[101][0]
    assert b"Visible comments" in records[103][0]
    assert b"user_annotations" not in records[103][0]
    assert records[104] == [b"9780306406157"]
    assert b"fiction" in records[105][0]
    assert records[112] == [b"calibre:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"]
    assert records[113][-1] == b"ASIN-123"
    assert records[501] == [b"EBOK"]
    assert records[503] == ["Unicode MOBI — café — 世界 😀".encode()]
    assert records[504] == [b"ASIN-123"]
    assert records[524] == [b"eng"]
    assert records[999] == [b"keep"]
    assert any(call[0] == "fetchEXTHFields" for call in updater.calls)
    assert updater.record0[92:96] != b"\0\0\0\0"


def test_mobi_metadata_updater_sanitizes_hostile_text_without_mutating_input() -> None:
    """
    Verify mobi metadata updater sanitizes hostile text without mutating input.

    Example:
        Exercise test mobi metadata updater sanitizes hostile text without mutating input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    updater = _fake_updater()
    title = "MOBI\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    comments = "Visible\x02 comments <div class=\"user_annotations\">drop me</div>"
    tags = ["tag\x03one", "emoji 😀"]

    mi = calibreMetaInformation(title, authors)
    mi.publisher = "Pub\x04lisher"
    mi.comments = comments
    mi.isbn = "978\x050306406157"
    mi.tags = tags
    mi.pubdate = None
    mi.timestamp = "2026-05-16T10:20:30\x06"
    mi.language = "en"
    mi.uuid = "aaaaaaaa-bbbb\x07"
    mi.book_producer = "Producer\x08Name"
    mi.cover_data = (None, None)
    mi.cover = None

    updater.update(mi)

    create_call = next(call for call in updater.calls if call[0] == "create_exth")
    _name, exth_blob, new_title = create_call
    records = _exth_records(exth_blob)

    assert new_title == "MOBITitle 😀"
    assert records[100] == [b"Alice One", b"Bob Two"]
    assert records[101] == [b"Publisher"]
    assert records[103] == [b"Visible comments "]
    assert records[104] == [b"9780306406157"]
    assert records[105] == ["tagone; emoji 😀".encode()]
    assert records[106] == [b"2026-05-16T10:20:30"]
    assert records[108] == [b"ProducerName"]
    assert records[112] == [b"calibre:aaaaaaaa-bbbb"]

    for code in (100, 101, 103, 104, 105, 106, 108, 112):
        assert not any(_has_forbidden_payload_byte(payload) for payload in records[code])

    assert mi.title == title
    assert mi.authors == authors
    assert mi.comments == comments
    assert mi.tags == tags


def test_mobi_metadata_updater_update_author_sort_pdoc_timestamp_and_cover_paths(monkeypatch) -> None:
    """
    Verify mobi metadata updater update author sort pdoc timestamp and cover paths.

    Example:
        Exercise test mobi metadata updater update author sort pdoc timestamp and cover paths through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.file_formats.conversion.config as conversion_config

    monkeypatch.setattr(
        conversion_config,
        "load_defaults",
        lambda _name: {"prefer_author_sort": True, "personal_doc": "*", "share_not_sync": False},
    )

    updater = _fake_updater()
    updater.original_exth_records = {501: b"EBOK"}
    updater.timestamp = None
    updater.cover_record = bytearray(b"C" * 16)
    updater.thumbnail_record = bytearray(b"T" * 12)
    updater.cover_rindex = 3
    updater.thumbnail_rindex = 4

    mi = _metadata_for_update()
    mi.author_sort = "Sort Ω & Sort 世界"
    mi.comments = "Keep this <hr class=\"annotations_divider\" /> drop this"
    mi.pubdate = None
    mi.timestamp = None
    mi.cover_data = ("png", b"raw-cover")

    monkeypatch.setattr(mobi, "is_image", lambda value: value is updater.cover_record or value is updater.thumbnail_record)
    monkeypatch.setattr(mobi, "rescale_image", lambda _data, size, dimen=None: b"R" * min(4, size))

    wrapper = SimpleNamespace(to_calibre=lambda: mi)
    updater.update(wrapper)

    records = _exth_records(next(call[1] for call in updater.calls if call[0] == "create_exth"))
    assert records[100] == ["Sort Ω".encode(), "Sort 世界".encode()]
    assert b"Keep this" in records[103][0]
    assert b"drop this" not in records[103][0]
    assert records[201] == [b"\x00\x00\x00\x03"]
    assert records[202] == [b"\x00\x00\x00\x04"]
    assert records[203] == [b"\x00\x00\x00\x00"]
    assert records[501] == [b"PDOC"]
    assert 106 in records
    assert updater.cover_record.startswith(b"RRRR")
    assert updater.thumbnail_record.startswith(b"RRRR")


def test_mobi_metadata_updater_update_timestamp_fallbacks_and_errors() -> None:
    """
    Verify mobi metadata updater update timestamp fallbacks and errors.

    Example:
        Exercise test mobi metadata updater update timestamp fallbacks and errors through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    updater = _fake_updater()
    mi = calibreMetaInformation("Timestamp Title", ["Timestamp Author"])
    mi.publisher = None
    mi.comments = None
    mi.isbn = None
    mi.tags = None
    mi.pubdate = None
    mi.timestamp = "mi-timestamp"
    mi.language = None
    mi.cover_data = (None, None)
    mi.cover = None
    updater.update(mi)
    records = _exth_records(next(call[1] for call in updater.calls if call[0] == "create_exth"))
    assert records[106] == [b"mi-timestamp"]

    updater = _fake_updater()
    updater.timestamp = b"existing-timestamp"
    mi.timestamp = None
    mi.cover = "missing-cover.jpg"
    updater.update(mi)
    records = _exth_records(next(call[1] for call in updater.calls if call[0] == "create_exth"))
    assert records[106] == [b"existing-timestamp"]

    bad_type = _fake_updater()
    bad_type.type = b"TEXTREAD"
    with pytest.raises(MobiError, match="Setting metadata only supported"):
        bad_type.update(mi)

    no_exth = _fake_updater()
    no_exth.exth = None
    with pytest.raises(MobiError, match="No existing EXTH"):
        no_exth.update(mi)


def test_mobi_metadata_updater_low_level_binary_helpers(capsys) -> None:
    """
    Verify mobi metadata updater low level binary helpers.

    Example:
        Exercise test mobi metadata updater low level binary helpers through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_mobi_edge_cases.py


    :param capsys: Pytest fixture capturing standard output and error streams.
    :return: None; the function records state or raises through its assertions.
    """
    updater = object.__new__(mobi.MetadataUpdater)
    record0 = bytearray(512)
    record0[0xA8:0xAC] = pack(">I", 200)
    record0[0xAC:0xB0] = pack(">I", 2)
    record0[200:248] = b"A" * updater.DRM_KEY_SIZE
    record0[248:296] = b"B" * updater.DRM_KEY_SIZE
    updater.record0 = record0
    assert updater.fetchDRMdata() == b"A" * updater.DRM_KEY_SIZE + b"B" * updater.DRM_KEY_SIZE
    assert updater.drm_key_count == 2

    data = bytearray(260)
    data[20:24] = pack(">I", 24)
    data[108:112] = pack(">I", 10)
    exth_records = [
        pack(">II", 106, 8 + len(b"timestamp")) + b"timestamp",
        pack(">II", 201, 12) + pack(">I", 2),
        pack(">II", 202, 12) + pack(">I", 3),
    ]
    exth = b"EXTH" + pack(">II", 12 + sum(len(x) for x in exth_records), len(exth_records)) + b"".join(exth_records)
    data[40 : 40 + len(exth)] = exth
    stream = io.BytesIO(data)
    updater.stream = stream
    updater.record0 = mobi.StreamSlicer(stream, 0, len(data))
    updater.record = lambda index: f"record-{index}".encode()
    updater.original_exth_records = {}

    updater.fetchEXTHFields()
    assert updater.timestamp == b"timestamp"
    assert updater.cover_record == b"record-12"
    assert updater.thumbnail_record == b"record-13"

    pdb_data = bytearray(140)
    pdb_data[78:86] = pack(">LBBBB", 100, 1, 0, 0, 7)
    pdb_data[86:94] = pack(">LBBBB", 120, 2, 0, 0, 8)
    pdb_stream = io.BytesIO(pdb_data + b"x" * 40)
    updater.data = mobi.StreamSlicer(pdb_stream)
    updater.nrecs = 2
    assert updater.get_pdbrecords() == [[100, 1, 7], [120, 2, 8]]
    updater.update_pdbrecords([104, 128])
    assert updater.pdbrecords == [[104, 1, 7], [128, 2, 8]]
    updater.record = mobi.MetadataUpdater.record.__get__(updater, mobi.MetadataUpdater)
    assert updater.record(0)[:4] == b"\0\0\0\0"
    with pytest.raises(ValueError, match="non-existent record"):
        updater.record(2)

    calls = []
    updater.pdbrecords = [[11, 0, 0]]
    updater.patch = lambda off, new: calls.append((off, new))
    updater.patchSection(0, b"new-record")
    assert calls == [(11, b"new-record")]

    updater.hexdump("AB")
    assert "41 42" in capsys.readouterr().out
