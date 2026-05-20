from __future__ import annotations

import importlib
import io
import sys
import types
import zipfile
from pathlib import Path

import pytest

from tests.support.file_format_unicode import assert_no_replacement_chars


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def info(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def warning(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def warn(self, message: str, *args) -> None:
        self.warning(message, *args)

    def error(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def __call__(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)


def _process_pml_bytes(payload: bytes, tmp_path: Path, *, encoding: str | None = "utf-8") -> str:
    pml_input = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_input")
    plugin = pml_input.PMLInput(None)
    plugin.options = types.SimpleNamespace(input_encoding=encoding)
    plugin.log = _Log()
    output = tmp_path / "index.html"

    plugin.process_pml(io.BytesIO(payload), str(output))

    return output.read_text("utf-8", "replace")


def test_pml_parser_preserves_multilingual_text_around_malformed_controls() -> None:
    converter = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlconverter")
    hostile = "\n".join(
        (
            r'\x="Καλημέρα مرحبا שלום"Καλημέρα مرحبا שלום\x',
            r'\FN="note-مرحبا"Broken note שלום without close',
            r'\Sd="side-שלום"Sidebar नमस्ते without close',
            r'\q="#missing-close Καλημέρα',
            r'\m="../../../../evil.png"',
            r'\T="500%"Indented 你好，世界 with huge indent',
            r'\U03A9 \a169 valid escapes, \UZZZZ bad escape, \a999 odd escape',
            r'Literal braces { } and backslash \\ with café',
        )
    )

    hizer = converter.PML_HTMLizer()
    html = hizer.parse_pml(hostile, "hostile_multilingual.pml")
    toc = hizer.get_toc()

    assert "Καλημέρα" in html
    assert "مرحبا" in html
    assert "שלום" in html
    assert "नमस्ते" in html
    assert "你好，世界" in html
    assert "café" in html
    assert "Ω" in html
    assert "©" in html
    assert "bad escape" in html
    assert "\x00" not in html
    assert toc is not None


def test_pml_input_process_pml_replaces_bad_bytes_without_losing_foreign_text(tmp_path: Path) -> None:
    payload = (
        "\\xΚαλημέρα مرحبا שלום\\x\n"
        "Body café 你好，世界 "
    ).encode("utf-8") + bytes([0x81, 0x8D, 0x90, 0x9D, 0xFF]) + " tail नमस्ते".encode("utf-8")

    html = _process_pml_bytes(payload, tmp_path, encoding="utf-8")

    assert "Καλημέρα" in html
    assert "مرحبا" in html
    assert "שלום" in html
    assert "café" in html
    assert "你好，世界" in html
    assert "tail नमस्ते" in html
    assert "\ufffd" in html


@pytest.mark.parametrize(
    ("encoding", "text", "expected"),
    (
        ("utf-8", "\\xΚαλημέρα שלום\\x\nBody café", ("Καλημέρα", "שלום", "café")),
        ("cp1252", "\\xCafé déjà vu\\x\nBody naïve façade", ("Café", "déjà", "naïve", "façade")),
    ),
)
def test_pml_input_process_pml_honors_declared_input_encoding(
    tmp_path: Path,
    encoding: str,
    text: str,
    expected: tuple[str, ...],
) -> None:
    html = _process_pml_bytes(text.encode(encoding), tmp_path, encoding=encoding)

    for fragment in expected:
        assert fragment in html
    assert_no_replacement_chars(html, context=encoding)


def test_pml_input_convert_pmlz_preserves_multilingual_pages_and_images(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pml_input = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_input")
    metadata_utils = importlib.import_module("LiuXin_alpha.metadata.utils")
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")

    def _metadata(_stream, _file_ext):
        mi = metadata_utils.calibreMetaInformation("PML hostile Καλημέρα", ["José Иван"])
        mi.cover = None
        return mi

    fake_ui.get_file_type_metadata = _metadata
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)

    archive = tmp_path / "book.pmlz"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("b_page.pml", "\\xSecond שלום\\x\nBody مرحبا")
        zf.writestr("a_page.pml", "\\xFirst Καλημέρα\\x\nBody café 你好")
        zf.writestr("cover.png", b"not-a-real-image-but-copied")

    plugin = pml_input.PMLInput(None)
    log = _Log()
    opts = types.SimpleNamespace(input_encoding="utf-8", debug_pipeline=None)
    with archive.open("rb") as stream:
        opf_path = Path(plugin.convert(stream, opts, "pmlz", log, {}))

    root = opf_path.parent
    first_html = (root / "a_page.html").read_text("utf-8", "replace")
    second_html = (root / "b_page.html").read_text("utf-8", "replace")

    assert opf_path.exists()
    assert (root / "toc.ncx").exists()
    assert "Καλημέρα" in first_html
    assert "café" in first_html
    assert "你好" in first_html
    assert "שלום" in second_html
    assert "مرحبا" in second_html
    assert (root / "images" / "cover.png").read_bytes() == b"not-a-real-image-but-copied"
