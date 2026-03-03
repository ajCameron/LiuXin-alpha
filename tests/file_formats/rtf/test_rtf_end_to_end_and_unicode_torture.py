from __future__ import annotations

import os
import re
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        self._record(*parts)

    def debug(self, *parts) -> None:
        self._record(*parts)

    def info(self, *parts) -> None:
        self._record(*parts)

    def warn(self, *parts) -> None:
        self._record(*parts)

    def warning(self, *parts) -> None:
        self._record(*parts)

    def error(self, *parts) -> None:
        self._record(*parts)

    def exception(self, *parts) -> None:
        self._record(*parts)


def _opts() -> SimpleNamespace:
    return SimpleNamespace(input_encoding=None, debug_pipeline=None, ignore_wmf=True)


def _rtf_escape(text: str) -> str:
    out = []
    for ch in text:
        if ch == "\\":
            out.append(r"\\")
        elif ch == "{":
            out.append(r"\{")
        elif ch == "}":
            out.append(r"\}")
        elif ord(ch) > 127:
            out.append(r"\u%d?" % ord(ch))
        else:
            out.append(ch)
    return "".join(out)


def _build_rtf(title: str, author: str, paragraphs: list[str]) -> bytes:
    body = "\\par\n".join(_rtf_escape(p) for p in paragraphs)
    raw = (
        "{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        "{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        "{\\info{\\title %s}{\\author %s}}\n"
        "\\viewkind4\\uc1\\pard\n"
        "%s\\par\n"
        "}\n"
    ) % (_rtf_escape(title), _rtf_escape(author), body)
    return raw.encode("latin-1", "replace")


@contextmanager
def _chdir(path: Path):
    old = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def _run_rtf_input_convert(input_path: Path, workdir: Path):
    from LiuXin_alpha.file_formats.conversion.plugins.rtf_input import RTFInput

    workdir.mkdir(parents=True, exist_ok=True)
    log = _Log()
    plugin = RTFInput(None)
    with _chdir(workdir):
        with input_path.open("rb") as stream:
            out = plugin.convert(stream, _opts(), "rtf", log, {})
    opf_path = Path(out)
    return log, opf_path, opf_path.parent / "index.xhtml", opf_path.parent / "styles.css"


def _normalize_opf(opf_text: str) -> str:
    opf_text = re.sub(
        r"<dc:identifier[^>]*>.*?</dc:identifier>",
        "<dc:identifier id='LiuXin_id'>NORMALIZED-ID</dc:identifier>",
        opf_text,
        flags=re.DOTALL,
    )
    opf_text = re.sub(
        r'(<meta name="calibre:timestamp" content=")[^"]+(")',
        r"\1NORMALIZED-TS\2",
        opf_text,
    )
    return opf_text


def test_rtf_input_end_to_end_unicode_torture(tmp_path: Path) -> None:
    paragraphs = [
        "Latin accents: naïve coöperate façade déjà vu.",
        "Greek: Καλημέρα κόσμε.",
        "Cyrillic: Здравствуйте, мир.",
        "Arabic: مرحبا بالعالم.",
        "Hebrew: שלום עולם.",
        "Devanagari: नमस्ते दुनिया।",
        "CJK: 你好，世界。",
        "Combining marks: cafe\u0301 co\u0308perate A\u030A.",
    ]
    source = tmp_path / "unicode_torture.rtf"
    source.write_bytes(_build_rtf("Unicode Stress", "Alice & Bob", paragraphs))

    _log, opf_path, html_path, css_path = _run_rtf_input_convert(source, tmp_path / "run_unicode")

    assert opf_path.exists()
    assert html_path.exists()
    assert css_path.exists()

    html = html_path.read_text("utf-8", "replace")
    probes = ["naïve", "Καλημέρα", "Здравствуйте", "مرحبا", "שלום", "नमस्ते", "你好"]
    hits = sum(1 for p in probes if p in html)
    assert hits >= 5


def test_rtf_input_end_to_end_deterministic_output(tmp_path: Path) -> None:
    paragraphs = [
        "Deterministic run one.",
        "More text, with punctuation, commas, and Ω marker.",
    ]
    source = tmp_path / "deterministic.rtf"
    source.write_bytes(_build_rtf("Deterministic", "Tester", paragraphs))

    _log_a, opf_a, html_a, css_a = _run_rtf_input_convert(source, tmp_path / "run_a")
    _log_b, opf_b, html_b, css_b = _run_rtf_input_convert(source, tmp_path / "run_b")

    assert html_a.read_bytes() == html_b.read_bytes()
    assert css_a.read_bytes() == css_b.read_bytes()
    assert _normalize_opf(opf_a.read_text("utf-8", "replace")) == _normalize_opf(
        opf_b.read_text("utf-8", "replace")
    )


def test_rtf_input_end_to_end_broken_encoding_tolerant(tmp_path: Path) -> None:
    broken = (
        b"{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        b"{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        b"{\\info{\\title Broken Encoding}{\\author Author}}\n"
        b"\\viewkind4\\uc1\\pard\n"
        + b"Broken bytes: "
        + bytes([0x81, 0x8D, 0x90, 0x9D, 0xFF])
        + b" and tail text.\\par\n"
        + b"}\n"
    )
    source = tmp_path / "broken_encoding.rtf"
    source.write_bytes(broken)

    log, opf_path, html_path, _css_path = _run_rtf_input_convert(source, tmp_path / "run_broken")

    assert opf_path.exists()
    assert html_path.exists()
    html = html_path.read_text("utf-8", "replace")
    assert "Broken bytes" in html
    assert "tail text" in html
    # Metadata stack is still partially legacy; conversion must still complete noisily.
    assert any("Failed to read RTF metadata" in msg for msg in log.messages)
