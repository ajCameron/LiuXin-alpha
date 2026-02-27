from __future__ import annotations

import io
import importlib
import struct
import sys
import types

import pytest


def _chunk(chunk_type: bytes, payload: bytes) -> bytes:
    return chunk_type + struct.pack(">L", len(payload)) + payload


def test_djvu_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.djvu")
    importlib.import_module("LiuXin_alpha.file_formats.djvu.djvu")
    importlib.import_module("LiuXin_alpha.file_formats.djvu.djvubzzdec")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.djvu_input")


def test_djvu_chunk_dump_txtz_uses_bzz_plugin_decompress() -> None:
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    compressed_payload = b"\xAA\xBB\xCC"
    chunk = DjvuChunk(_chunk(b"TXTz", compressed_payload), 0, 0)

    seen: dict[str, bytes] = {}

    def fake_decompress(raw: bytes) -> bytes:
        seen["raw"] = bytes(raw)
        return b"decoded text"

    chunk.speedup = types.SimpleNamespace(decompress=fake_decompress)
    txtout = io.BytesIO()
    chunk.dump(txtout=txtout)

    assert seen["raw"] == compressed_payload
    assert txtout.getvalue() == b"decoded text\x1f"


def test_djvu_chunk_dump_txta_parses_three_byte_length_header() -> None:
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    payload = b"\x00\x00\x05helloextra"
    chunk = DjvuChunk(_chunk(b"TXTa", payload), 0, 0)
    txtout = io.BytesIO()
    chunk.dump(txtout=txtout)

    assert txtout.getvalue() == b"hello\x1f"


def test_djvu_chunk_dump_txta_rejects_missing_header() -> None:
    from LiuXin_alpha.file_formats.djvu.djvu import DjvuChunk

    chunk = DjvuChunk(_chunk(b"TXTa", b"\x00\x01"), 0, 0)
    with pytest.raises(ValueError, match="missing length header"):
        chunk.dump(txtout=io.BytesIO())


def test_djvu_file_get_text_reads_txta_from_stream() -> None:
    from LiuXin_alpha.file_formats.djvu.djvu import DJVUFile

    stream = io.BytesIO(b"AT&T" + _chunk(b"TXTa", b"\x00\x00\x04test"))
    out = io.BytesIO()

    djvu = DJVUFile(stream)
    djvu.get_text(out)

    assert out.getvalue() == b"test\x1f"


def test_djvu_input_convert_glue_with_fakes(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.djvu_input as djvu_input_mod
    from LiuXin_alpha.metadata.utils import calibreMetaInformation

    class _Opt:
        def __init__(self, name, value):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    sentinel_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())

    class _HTMLInput:
        options = (_Opt("breadth_first", False),)

        def convert(self, stream, options, file_ext, log, accelerators):
            assert file_ext == "html"
            return sentinel_oeb

    class _DJVUFile:
        def __init__(self, stream):
            self.stream = stream

        def get_text(self, outfile):
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
