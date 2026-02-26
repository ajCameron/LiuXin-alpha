from __future__ import annotations

import importlib
import io
import sys
import types
import zipfile
from pathlib import Path

import pytest


class _Opt:
    def __init__(self, name, value):
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = value


def test_compression_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.compression")
    importlib.import_module("LiuXin_alpha.file_formats.compression.compressed_ebooks")
    importlib.import_module("LiuXin_alpha.file_formats.compression.palmdoc")
    importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")


def test_compressed_ebook_heuristics_for_lists() -> None:
    from LiuXin_alpha.file_formats.compression.compressed_ebooks import is_comic, is_ebook

    assert is_comic(["Page1.JPG", "nested/page2.png", "thumbs.db"]) is True
    assert is_ebook(["index.html", "images/p1.jpg", "book.opf", "styles.css"]) is True
    assert is_ebook(["index.html", "run.exe"]) is False


def test_zip_archive_book_detection(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.compression.compressed_ebooks import is_file_book, is_zip_archive_book

    good = tmp_path / "good.zip"
    bad = tmp_path / "bad.zip"

    with zipfile.ZipFile(good, "w") as zf:
        zf.writestr("index.html", "<html/>")
        zf.writestr("content.opf", "<opf/>")
        zf.writestr("toc.ncx", "<ncx/>")

    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("index.html", "<html/>")
        zf.writestr("payload.exe", "MZ")

    assert is_zip_archive_book(str(good)) is True
    assert is_file_book(str(good)) is True
    assert is_zip_archive_book(str(bad)) is False
    assert is_file_book(str(bad)) is False


def test_palmdoc_roundtrip_and_reference_compressor() -> None:
    from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc, decompress_doc, py_compress_doc

    data = b"PalmDOC test payload with repeats repeats repeats\n" * 4

    compressed = compress_doc(data)
    assert isinstance(compressed, (bytes, bytearray))
    assert decompress_doc(compressed) == data

    reference = py_compress_doc(data)
    assert decompress_doc(reference) == data


def test_palmdoc_accepts_legacy_text_input() -> None:
    from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc, decompress_doc

    data = "abc\u00ff"
    compressed = compress_doc(data)
    assert decompress_doc(compressed) == b"abc\xff"


def test_tcr_roundtrip_and_invalid_header() -> None:
    from LiuXin_alpha.file_formats.compression.tcr import compress, decompress

    payload = b"hello tcr world\n" * 5
    encoded = compress(payload)
    decoded = decompress(io.BytesIO(encoded))

    assert isinstance(encoded, (bytes, bytearray))
    assert decoded == payload

    with pytest.raises(ValueError, match="invalid TCR header"):
        decompress(io.BytesIO(b"not a tcr stream"))


def test_tcr_input_plugin_decodes_bytes_and_applies_recommended_options(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.tcr_input as tcr_input_mod

    plugin = tcr_input_mod.TCRInput(None)

    class _TxtPlugin:
        options = (_Opt("max_line_length", 80),)

        def convert(self, stream, options, file_ext, log, accelerators):
            assert file_ext == "txt"
            assert stream.read() == "decoded payload"
            return "converted.oeb"

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: _TxtPlugin()
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)

    stream = io.BytesIO(b"ignored")
    options = types.SimpleNamespace(input_encoding="utf-8")

    monkeypatch.setattr(
        "LiuXin_alpha.file_formats.compression.tcr.decompress",
        lambda s: b"decoded payload",
    )

    result = plugin.convert(
        stream,
        options,
        "tcr",
        log=types.SimpleNamespace(info=lambda *a, **k: None),
        accelerators={},
    )

    assert result == "converted.oeb"
    assert options.max_line_length == 80
