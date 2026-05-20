from __future__ import annotations

import importlib
import io
import os
import sys
import types
import unicodedata
from contextlib import contextmanager
from pathlib import Path

import pytest

from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)
from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS


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


def _rtf_escape(text: str) -> str:
    escaped = []
    for char in text:
        if char == "\\":
            escaped.append(r"\\")
        elif char == "{":
            escaped.append(r"\{")
        elif char == "}":
            escaped.append(r"\}")
        elif ord(char) > 127:
            escaped.append(r"\u%d?" % ord(char))
        else:
            escaped.append(char)
    return "".join(escaped)


def _normalized_contains(text: str, probe: str) -> bool:
    return unicodedata.normalize("NFC", probe) in unicodedata.normalize("NFC", text)


@contextmanager
def _chdir(path: Path):
    old = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def _run_rtf_input_payload(payload: bytes, tmp_path: Path):
    rtf_input = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")
    source = tmp_path / "hostile_multilingual.rtf"
    workdir = tmp_path / "run_hostile_multilingual"
    source.write_bytes(payload)
    workdir.mkdir()
    log = _Log()

    with _chdir(workdir):
        with source.open("rb") as stream:
            opf_path = rtf_input.RTFInput(None).convert(
                stream,
                types.SimpleNamespace(input_encoding=None, debug_pipeline=None, ignore_wmf=True),
                "rtf",
                log,
                {},
            )
    opf_path = Path(opf_path)
    return log, opf_path, opf_path.parent / "index.xhtml"


def test_rtf_tokenizer_accepts_signed_unicode_numeric_arguments() -> None:
    preprocess = importlib.import_module("LiuXin_alpha.file_formats.rtf.preprocess")

    tokenizer = preprocess.RtfTokenizer(
        r"{\rtf1 \uc1 \u-945? signed unicode \u1605? Arabic \u1513? Hebrew \u-10916? Hangul}"
    )
    rebuilt = preprocess.RtfTokenParser(tokenizer.tokens).toRTF()

    assert r"\u-945" in rebuilt
    assert r"\u1605" in rebuilt
    assert r"\u1513" in rebuilt
    assert r"\u-10916" in rebuilt
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
        raise _InvalidRtfException("unbalanced hostile groups Καλημέρα مرحبا שלום")

    plugin.generate_xml = _raise_invalid
    source = tmp_path / "hostile.rtf"
    source.write_bytes(b"{\\rtf1{\\fonttbl")
    stream = io.BytesIO(source.read_bytes())
    stream.name = str(source)

    with pytest.raises(ValueError, match=r"malformed[\s\S]*unbalanced hostile groups Καλημέρα مرحبا שלום"):
        plugin.convert(
            stream,
            types.SimpleNamespace(input_encoding=None, debug_pipeline=None, ignore_wmf=True),
            "rtf",
            _Log(),
            {},
        )


def test_rtf_input_preserves_foreign_text_in_noisy_hostile_document(tmp_path: Path) -> None:
    body = (
        "Safe lead Καλημέρα مرحبا שלום नमस्ते 你好，世界 cafe\u0301. "
        r"Escaped braces \{ \} and slash \\ survive. "
        "Tail สวัสดีโลก 안녕하세요 세계."
    )
    payload = (
        "{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        "{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        "{\\info{\\title Hostile Καλημέρα}{\\author José Иван}}\n"
        "\\viewkind4\\uc1\\pard\n"
        + _rtf_escape(body)
        + "\\par\n"
        r"{\*\unknown-destination "
        + _rtf_escape("ignored مرحبا שלום")
        + "}\n"
        + "Broken bytes: "
    ).encode("latin-1", "replace") + bytes([0x81, 0x8D, 0x90, 0x9D, 0xFF]) + b" tail.\\par\n}\n"

    _log, opf_path, html_path = _run_rtf_input_payload(payload, tmp_path)

    assert opf_path.exists()
    assert html_path.exists()
    html = html_path.read_text("utf-8", "replace")
    for probe in ("Καλημέρα", "مرحبا", "שלום", "नमस्ते", "你好", "cafe\u0301", "สวัสดีโลก", "안녕하세요"):
        assert _normalized_contains(html, probe)
    assert "Broken bytes" in html
    assert "\ufffd" in html


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
    for fragment in COMMON_TEXT_FRAGMENTS:
        assert rtfml.txt2rtf(fragment) in rendered
    assert any("corrupted" in message for message in log.messages)
