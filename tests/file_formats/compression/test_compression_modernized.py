"""
Provide test compression modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test compression modernized through a consuming regression::

        python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
"""
from __future__ import annotations

import importlib
import io
import sys
import types
import zipfile
from pathlib import Path

import pytest


class _Opt:
    """
    Provide the opt contract for validated ebook processing.

    Example:
        Exercise  Opt through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
    """
    def __init__(self, name, value):
        """
        Initialize and validate the opt state.

        Example:
            Exercise  Opt.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; validated state is stored on the receiving object.
        """
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = value


def test_compression_modules_import_smoke() -> None:
    """
    Perform the test compression modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test compression modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.compression")
    importlib.import_module("LiuXin_alpha.file_formats.compression.compressed_ebooks")
    importlib.import_module("LiuXin_alpha.file_formats.compression.palmdoc")
    importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")


def test_compressed_ebook_heuristics_for_lists() -> None:
    """
    Perform the test compressed ebook heuristics for lists operation under explicit file-format and conversion rules.

    Example:
        Exercise test compressed ebook heuristics for lists through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.compression.compressed_ebooks import is_comic, is_ebook

    assert is_comic(["Page1.JPG", "nested/page2.png", "thumbs.db"]) is True
    assert is_ebook(["index.html", "images/p1.jpg", "book.opf", "styles.css"]) is True
    assert is_ebook(["index.html", "run.exe"]) is False


def test_zip_archive_book_detection(tmp_path: Path) -> None:
    """
    Perform the test zip archive book detection operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip archive book detection through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test palmdoc roundtrip and reference compressor operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc roundtrip and reference compressor through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc, decompress_doc, py_compress_doc

    data = b"PalmDOC test payload with repeats repeats repeats\n" * 4

    compressed = compress_doc(data)
    assert isinstance(compressed, (bytes, bytearray))
    assert decompress_doc(compressed) == data

    reference = py_compress_doc(data)
    assert decompress_doc(reference) == data


def test_palmdoc_accepts_legacy_text_input() -> None:
    """
    Perform the test palmdoc accepts legacy text input operation under explicit file-format and conversion rules.

    Example:
        Exercise test palmdoc accepts legacy text input through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc, decompress_doc

    data = "abc\u00ff"
    compressed = compress_doc(data)
    assert decompress_doc(compressed) == b"abc\xff"


def test_tcr_roundtrip_and_invalid_header() -> None:
    """
    Perform the test tcr roundtrip and invalid header operation under explicit file-format and conversion rules.

    Example:
        Exercise test tcr roundtrip and invalid header through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.compression.tcr import compress, decompress

    payload = b"hello tcr world\n" * 5
    encoded = compress(payload)
    decoded = decompress(io.BytesIO(encoded))

    assert isinstance(encoded, (bytes, bytearray))
    assert decoded == payload

    with pytest.raises(ValueError, match="invalid TCR header"):
        decompress(io.BytesIO(b"not a tcr stream"))


def test_tcr_input_plugin_decodes_bytes_and_applies_recommended_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test tcr input plugin decodes bytes and applies recommended options operation under explicit file-format and conversion rules.

    Example:
        Exercise test tcr input plugin decodes bytes and applies recommended options through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.tcr_input as tcr_input_mod

    plugin = tcr_input_mod.TCRInput(None)

    class _TxtPlugin:
        """
        Provide the txtplugin contract for validated ebook processing.

        Example:
            Exercise test tcr input plugin decodes bytes and applies recommended options. TxtPlugin through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
        """
        options = (_Opt("max_line_length", 80),)

        def convert(self, stream, options, file_ext, log, accelerators):
            """
            Convert the supplied source into the stage's normalized output representation.

            Example:
                Exercise test tcr input plugin decodes bytes and applies recommended options. TxtPlugin.convert through a consuming regression::

                    python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param options: Value supplied for options under the utility contract.
            :param file_ext: Value supplied for file ext under the utility contract.
            :param log: Value supplied for log under the utility contract.
            :param accelerators: Value supplied for accelerators under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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
