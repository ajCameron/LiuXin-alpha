from __future__ import annotations

import importlib
import io
import sys
import types
from pathlib import Path

import pytest

from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def __call__(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def debug(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def info(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def warn(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def warning(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def exception(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))


def test_rtf_tokenizer_accepts_signed_unicode_numeric_arguments() -> None:
    preprocess = importlib.import_module("LiuXin_alpha.file_formats.rtf.preprocess")

    tokenizer = preprocess.RtfTokenizer(r"{\rtf1 \uc1 \u-945? signed unicode}")
    rebuilt = preprocess.RtfTokenParser(tokenizer.tokens).toRTF()

    assert r"\u-945" in rebuilt
    assert "signed unicode" in rebuilt


@pytest.mark.parametrize(
    "payload",
    (
        r"{\rtf1 \u- signed unicode}",
        r"{\rtf1 \u12345678901 too many digits}",
        "{\\rtf1 trailing control\\",
    ),
)
def test_rtf_tokenizer_rejects_malformed_control_words_deterministically(payload: str) -> None:
    preprocess = importlib.import_module("LiuXin_alpha.file_formats.rtf.preprocess")

    with pytest.raises(Exception, match="Error"):
        preprocess.RtfTokenizer(payload)


def test_rtf_input_wraps_invalid_rtf_parser_errors(tmp_path: Path, monkeypatch) -> None:
    rtf_input = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")
    fake_parse = types.ModuleType("LiuXin_alpha.file_formats.rtf2xml.ParseRtf")

    class _InvalidRtfException(Exception):
        pass

    class _RtfInvalidCodeException(Exception):
        pass

    fake_parse.InvalidRtfException = _InvalidRtfException
    fake_parse.RtfInvalidCodeException = _RtfInvalidCodeException
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.rtf2xml.ParseRtf", fake_parse)

    plugin = rtf_input.RTFInput(None)

    def _raise_invalid(_stream):
        raise _InvalidRtfException("unbalanced hostile groups")

    plugin.generate_xml = _raise_invalid
    source = tmp_path / "hostile.rtf"
    source.write_bytes(b"{\\rtf1{\\fonttbl")
    stream = io.BytesIO(source.read_bytes())
    stream.name = str(source)

    with pytest.raises(ValueError, match=r"malformed[\s\S]*unbalanced hostile groups"):
        plugin.convert(
            stream,
            types.SimpleNamespace(input_encoding=None, debug_pipeline=None, ignore_wmf=True),
            "rtf",
            _Log(),
            {},
        )


def test_rtf_input_extract_images_tolerates_corrupt_pict_payloads(tmp_path: Path, monkeypatch) -> None:
    rtf_input = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")
    monkeypatch.chdir(tmp_path)
    picts = tmp_path / "picts.rtf"
    picts.write_text(
        r"{\pict\wmetafile8 zzz not-hex 123}" "\n"
        r"{\pict\pngblip odd nibble fff zzz}" "\n",
        encoding="latin-1",
    )
    plugin = rtf_input.RTFInput(None)
    plugin.opts = types.SimpleNamespace(ignore_wmf=True)
    plugin.log = _Log()

    imap = plugin.extract_images(str(picts))

    assert imap == {1: "__REMOVE_ME__", 2: "__REMOVE_ME__"}
    assert not list(tmp_path.glob("*.wmf"))
    assert any("Extracting images" in message for message in plugin.log.messages)


def test_rtfmlizer_drops_corrupt_images_without_leaking_placeholders(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    rtfml = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")

    def _raise_corrupt(_data):
        raise ValueError("hostile image payload")

    monkeypatch.setattr(rtfml, "_convert_image_to_jpeg_bytes", _raise_corrupt)
    log = _Log()

    rendered = rtfml.RTFMLizer(log).extract_content(build_text_output_book(), text_output_options())

    assert "SPECIAL_IMAGE-" not in rendered
    assert r"\jpegblip" not in rendered
    assert any("corrupted" in message for message in log.messages)
