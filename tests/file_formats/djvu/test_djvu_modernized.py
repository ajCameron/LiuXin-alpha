"""
Provide test djvu modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test djvu modernized through a consuming regression::

        python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
"""
from __future__ import annotations

import io
import importlib
import struct
import sys
import types

import pytest


def _chunk(chunk_type: bytes, payload: bytes) -> bytes:
    """
    Perform the chunk operation under explicit file-format and conversion rules.

    Example:
        Exercise  chunk through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :param chunk_type: Value supplied for chunk type under the utility contract.
    :param payload: Value supplied for payload under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return chunk_type + struct.pack(">L", len(payload)) + payload


def test_djvu_modules_import_smoke() -> None:
    """
    Perform the test djvu modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.djvu")
    importlib.import_module("LiuXin_alpha.file_formats.djvu.djvu")
    importlib.import_module("LiuXin_alpha.file_formats.djvu.djvubzzdec")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.djvu_input")


def test_djvu_chunk_dump_txtz_uses_bzz_plugin_decompress() -> None:
    """
    Perform the test djvu chunk dump txtz uses bzz plugin decompress operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu chunk dump txtz uses bzz plugin decompress through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    compressed_payload = b"\xAA\xBB\xCC"
    chunk = DjvuChunk(_chunk(b"TXTz", compressed_payload), 0, 0)

    seen: dict[str, bytes] = {}

    def fake_decompress(raw: bytes) -> bytes:
        """
        Perform the fake decompress operation under explicit file-format and conversion rules.

        Example:
            Exercise test djvu chunk dump txtz uses bzz plugin decompress.fake decompress through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        seen["raw"] = bytes(raw)
        return b"decoded text"

    chunk.speedup = types.SimpleNamespace(decompress=fake_decompress)
    txtout = io.BytesIO()
    chunk.dump(txtout=txtout)

    assert seen["raw"] == compressed_payload
    assert txtout.getvalue() == b"decoded text\x1f"


def test_djvu_chunk_dump_txta_parses_three_byte_length_header() -> None:
    """
    Perform the test djvu chunk dump txta parses three byte length header operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu chunk dump txta parses three byte length header through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    payload = b"\x00\x00\x05helloextra"
    chunk = DjvuChunk(_chunk(b"TXTa", payload), 0, 0)
    txtout = io.BytesIO()
    chunk.dump(txtout=txtout)

    assert txtout.getvalue() == b"hello\x1f"


def test_djvu_chunk_dump_txta_rejects_missing_header() -> None:
    """
    Perform the test djvu chunk dump txta rejects missing header operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu chunk dump txta rejects missing header through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    chunk = DjvuChunk(_chunk(b"TXTa", b"\x00\x01"), 0, 0)
    with pytest.raises(ValueError, match="missing length header"):
        chunk.dump(txtout=io.BytesIO())


def test_djvu_file_get_text_reads_txta_from_stream() -> None:
    """
    Perform the test djvu file get text reads txta from stream operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu file get text reads txta from stream through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.djvu.djvu import DJVUFile

    stream = io.BytesIO(b"AT&T" + _chunk(b"TXTa", b"\x00\x00\x04test"))
    out = io.BytesIO()

    djvu = DJVUFile(stream)
    djvu.get_text(out)

    assert out.getvalue() == b"test\x1f"


def test_djvu_input_convert_glue_with_fakes(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test djvu input convert glue with fakes operation under explicit file-format and conversion rules.

    Example:
        Exercise test djvu input convert glue with fakes through a consuming regression::

            python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.djvu_input as djvu_input_mod
    from LiuXin_alpha.metadata.utils import calibreMetaInformation

    class _Opt:
        """
        Provide the opt contract for validated ebook processing.

        Example:
            Exercise test djvu input convert glue with fakes. Opt through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
        """
        def __init__(self, name, value):
            """
            Initialize and validate the opt state.

            Example:
                Exercise test djvu input convert glue with fakes. Opt.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    sentinel_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())

    class _HTMLInput:
        """
        Convert htmlinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test djvu input convert glue with fakes. HTMLInput through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
        """
        options = (_Opt("breadth_first", False),)

        def convert(self, stream, options, file_ext, log, accelerators):
            """
            Convert the supplied source into the stage's normalized output representation.

            Example:
                Exercise test djvu input convert glue with fakes. HTMLInput.convert through a consuming regression::

                    python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param options: Value supplied for options under the utility contract.
            :param file_ext: Value supplied for file ext under the utility contract.
            :param log: Value supplied for log under the utility contract.
            :param accelerators: Value supplied for accelerators under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            assert file_ext == "html"
            return sentinel_oeb

    class _DJVUFile:
        """
        Provide the djvufile contract for validated ebook processing.

        Example:
            Exercise test djvu input convert glue with fakes. DJVUFile through a consuming regression::

                python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py
        """
        def __init__(self, stream):
            """
            Initialize and validate the djvufile state.

            Example:
                Exercise test djvu input convert glue with fakes. DJVUFile.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :return: None; validated state is stored on the receiving object.
            """
            self.stream = stream

        def get_text(self, outfile):
            """
            Return text under the format's safety and compatibility rules.

            Example:
                Exercise test djvu input convert glue with fakes. DJVUFile.get text through a consuming regression::

                    python -m pytest -q tests/file_formats/djvu/test_djvu_modernized.py


            :param outfile: Value supplied for outfile under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            outfile.write(b"Fake DjVu text.\x1f")

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: _HTMLInput() if fmt == "html" else None
    fake_ui.get_file_type_metadata = lambda stream, file_ext, calibre=True: calibreMetaInformation(
        "DjVu Smoke", ["Smoke Author"]
    )

    fake_txt_processor = types.ModuleType("LiuXin_alpha.file_formats.txt.processor")
    fake_txt_processor.convert_basic = lambda text: "<html><body>%s</body></html>" % text.decode("utf-8")

    fake_djvu_mod = types.ModuleType("LiuXin_alpha.file_formats.djvu.djvu")
    fake_djvu_mod.DJVUFile = _DJVUFile

    captured = {}
    fake_oeb_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_oeb_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: captured.update(
        {"title": mi.title, "authors": tuple(mi.authors)}
    )

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.processor", fake_txt_processor)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.djvu.djvu", fake_djvu_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_oeb_meta)
    monkeypatch.chdir(tmp_path)

    options = types.SimpleNamespace(debug_pipeline="keep-me")
    plugin = djvu_input_mod.DJVUInput(None)
    out = plugin.convert(io.BytesIO(b"fake"), options, "djvu", log=types.SimpleNamespace(), accelerators={})

    assert out is sentinel_oeb
    assert options.input_encoding == "utf-8"
    assert options.debug_pipeline == "keep-me"
    assert captured["title"] == "DjVu Smoke"
