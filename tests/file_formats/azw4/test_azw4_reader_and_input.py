from __future__ import annotations

import io
import types
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.azw4.reader import Reader, extract_embedded_pdf_bytes, unwrap


def _sample_azw4_like_payload() -> bytes:
    return b"AZW4PREFIX\x00\x01%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\nAZW4SUFFIX"


class _Log:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


def test_extract_embedded_pdf_bytes_extracts_pdf_slice() -> None:
    raw = _sample_azw4_like_payload()
    extracted = extract_embedded_pdf_bytes(raw)

    assert extracted.startswith(b"%PDF")
    assert extracted.rstrip().endswith(b"%%EOF")


def test_extract_embedded_pdf_bytes_raises_for_missing_pdf() -> None:
    with pytest.raises(ValueError, match="No embedded PDF"):
        extract_embedded_pdf_bytes(b"not-a-pdf-container")


def test_unwrap_writes_embedded_pdf(tmp_path: Path) -> None:
    out = tmp_path / "out.pdf"
    unwrap(io.BytesIO(_sample_azw4_like_payload()), out)

    data = out.read_bytes()
    assert data.startswith(b"%PDF")
    assert data.rstrip().endswith(b"%%EOF")


def test_reader_extract_content_uses_pdf_plugin_and_cleans_temp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.azw4.reader as reader_mod

    converted = {"called": False, "pdf_path": None}

    class _Option:
        def __init__(self, name, value):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    class _PdfPlugin:
        options = (_Option("new_option", 123),)

        def convert(self, stream, options, file_ext, log, accelerators):
            converted["called"] = True
            converted["pdf_path"] = Path(stream.name)
            payload = stream.read()
            assert payload.startswith(b"%PDF")
            assert file_ext == "pdf"
            return "metadata.opf"

    monkeypatch.setattr(reader_mod, "_plugin_for_input_format", lambda fmt: _PdfPlugin())

    options = types.SimpleNamespace(existing_option=True)
    log = _Log()
    reader = Reader(header=object(), stream=io.BytesIO(_sample_azw4_like_payload()), log=log, options=options)

    result = reader.extract_content(tmp_path)

    assert result == "metadata.opf"
    assert converted["called"] is True
    assert options.new_option == 123
    assert converted["pdf_path"] is not None
    assert not converted["pdf_path"].exists()


def test_azw4_input_plugin_delegates_to_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.azw4_input as plugin_mod
    import sys

    calls = {}

    class _FakeReader:
        def __init__(self, header, stream, log, options):
            calls["reader_init"] = {
                "header": header,
                "stream": stream,
                "log": log,
                "options": options,
            }

        def extract_content(self, output_dir):
            calls["output_dir"] = output_dir
            return "delegated.opf"

    fake_reader_module = types.ModuleType("LiuXin_alpha.file_formats.azw4.reader")
    fake_reader_module.Reader = _FakeReader

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.azw4.reader", fake_reader_module)
    monkeypatch.setattr(plugin_mod.os, "getcwd", lambda: "/tmp/azw4-test-cwd")

    plugin = plugin_mod.AZW4Input(None)
    stream = io.BytesIO(b"fake-stream")
    options = types.SimpleNamespace()
    log = _Log()

    result = plugin.convert(stream, options, "azw4", log, {})

    assert result == "delegated.opf"
    assert calls["reader_init"]["header"] is None
    assert calls["reader_init"]["stream"] is stream
    assert calls["reader_init"]["options"] is options
    assert calls["output_dir"] == "/tmp/azw4-test-cwd"
