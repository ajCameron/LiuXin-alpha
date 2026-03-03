from __future__ import annotations

import importlib
import io
import types


class _DummyLog:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def debug(self, message: str) -> None:
        self.messages.append(("debug", message))

    def info(self, message: str) -> None:
        self.messages.append(("info", message))

    def warning(self, message: str) -> None:
        self.messages.append(("warning", message))

    def error(self, message: str) -> None:
        self.messages.append(("error", message))


def test_pdb_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.pdb",
        "LiuXin_alpha.file_formats.pdb.header",
        "LiuXin_alpha.file_formats.pdb.palmdoc.reader",
        "LiuXin_alpha.file_formats.pdb.palmdoc.writer",
        "LiuXin_alpha.file_formats.pdb.ztxt.reader",
        "LiuXin_alpha.file_formats.pdb.ztxt.writer",
        "LiuXin_alpha.file_formats.pdb.ereader.reader",
        "LiuXin_alpha.file_formats.pdb.ereader.writer",
        "LiuXin_alpha.file_formats.pdb.pdf.reader",
        "LiuXin_alpha.file_formats.pdb.plucker.reader",
        "LiuXin_alpha.file_formats.pdb.haodoo.reader",
        "LiuXin_alpha.file_formats.conversion.plugins.pdb_input",
        "LiuXin_alpha.file_formats.conversion.plugins.pdb_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_pdb_header_builder_reader_roundtrip() -> None:
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder, PdbHeaderReader

    section_payloads = [b"abc", b"defgh"]
    stream = io.BytesIO()
    PdbHeaderBuilder("zTXTGPlm", "Roundtrip").build_header([len(x) for x in section_payloads], stream)
    for payload in section_payloads:
        stream.write(payload)

    stream.seek(0)
    reader = PdbHeaderReader(stream)
    assert reader.ident == "zTXTGPlm"
    assert reader.num_sections == 2
    assert reader.section_data(0) == b"abc"
    assert reader.section_data(1) == b"defgh"


def test_pdb_registry_lookup_smoke() -> None:
    from LiuXin_alpha.file_formats.pdb import get_reader, get_writer

    assert get_reader("TEXtREAd") is not None
    assert get_reader("zTXTGPlm") is not None
    assert get_reader("PNRdPPrs") is not None
    assert get_writer("doc") is not None
    assert get_writer("ztxt") is not None
    assert get_writer("ereader") is not None


def test_palmdoc_and_ztxt_header_records_are_binary() -> None:
    from LiuXin_alpha.file_formats.pdb.palmdoc.writer import Writer as PalmDocWriter
    from LiuXin_alpha.file_formats.pdb.ztxt.writer import Writer as ZtxtWriter

    opts = types.SimpleNamespace(title=None, pdb_output_encoding="cp1252")
    log = _DummyLog()

    palm = PalmDocWriter(opts, log)
    ztxt = ZtxtWriter(opts, log)

    palm_header = palm._header_record(txt_length=1234, record_count=7)
    ztxt_header = ztxt._header_record(txt_length=1234, record_count=7, crc32=0xDEADBEEF)

    assert isinstance(palm_header, bytes)
    assert isinstance(ztxt_header, bytes)
    assert len(palm_header) == 16
    assert len(ztxt_header) == 32


def test_palmdoc_and_ztxt_chunking_uses_integer_record_math(monkeypatch) -> None:
    palmdoc_mod = importlib.import_module("LiuXin_alpha.file_formats.pdb.palmdoc.writer")
    ztxt_mod = importlib.import_module("LiuXin_alpha.file_formats.pdb.ztxt.writer")

    class _FakeTXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb_book, _opts):
            return self.sample_text

    monkeypatch.setattr(palmdoc_mod, "TXTMLizer", _FakeTXTMLizer)
    monkeypatch.setattr(ztxt_mod, "TXTMLizer", _FakeTXTMLizer)

    opts = types.SimpleNamespace(title=None, pdb_output_encoding="cp1252")
    log = _DummyLog()

    _FakeTXTMLizer.sample_text = "A" * (palmdoc_mod.MAX_RECORD_SIZE + 3)
    palm = palmdoc_mod.Writer(opts, log)
    palm_records, palm_len = palm._generate_text(object())
    assert palm_len == len(_FakeTXTMLizer.sample_text.encode("cp1252", "replace"))
    assert len(palm_records) == 2

    _FakeTXTMLizer.sample_text = "B" * (ztxt_mod.MAX_RECORD_SIZE + 3)
    ztxt = ztxt_mod.Writer(opts, log)
    ztxt_records, ztxt_len = ztxt._generate_text(object())
    assert ztxt_len == len(_FakeTXTMLizer.sample_text.encode("cp1252", "replace"))
    assert len(ztxt_records) == 2


def test_ereader_helpers_are_binary_and_handle_missing_pillow(monkeypatch) -> None:
    ereader_mod = importlib.import_module("LiuXin_alpha.file_formats.pdb.ereader.writer")
    opts = types.SimpleNamespace(title=None, pdb_output_encoding="cp1252")
    log = _DummyLog()
    writer = ereader_mod.Writer(opts, log)

    record = writer._header_record(text_count=2, chapter_count=0, link_count=0, image_count=0)
    assert isinstance(record, bytes)
    assert len(record) == 132

    items = writer._index_item(br"(?s)\\x(?P<text>.+?)\\x", b"\\xChapter\\x")
    assert items and items[0].endswith(b"\x00")

    monkeypatch.setattr(ereader_mod, "_PILImage", None)
    images = writer._images(
        [types.SimpleNamespace(media_type="image/png", href="cover.png", data=b"not-an-image")],
        {"cover.png": "cover"},
    )
    assert images == []
