from __future__ import annotations

import base64
import importlib
from pathlib import Path

from lxml import etree

from tests.support.file_format_oeb import (
    MinimalMetadataValue,
    build_rich_oeb_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)
from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, assert_no_replacement_chars


def _fb2_output_options(**overrides):
    return text_output_options(
        sectionize="files",
        pretty_print=False,
        insert_blank_line=False,
        fb2_genre="sf",
        **overrides,
    )


def _parse_fb2(payload: bytes | str):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return etree.fromstring(payload)


def _render_fb2ml(monkeypatch, book=None) -> str:
    install_minimal_stylizers(monkeypatch)
    fb2ml = importlib.import_module("LiuXin_alpha.file_formats.fb2.fb2ml")
    monkeypatch.setattr(fb2ml, "_convert_to_jpeg", lambda raw_data, quality=70: None)
    return fb2ml.FB2MLizer(null_log()).extract_content(
        book or build_rich_oeb_output_book(),
        _fb2_output_options(),
    )


def _patch_output_transforms(monkeypatch) -> None:
    rasterize = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.rasterize")
    jacket = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.jacket")

    class _Rasterizer:
        def __call__(self, oeb_book, opts):
            return None

    monkeypatch.setattr(rasterize, "SVGRasterizer", _Rasterizer)
    monkeypatch.setattr(jacket, "linearize_jacket", lambda oeb_book: None)


def test_fb2mlizer_serializes_unicode_metadata_body_styles_and_images(monkeypatch) -> None:
    rendered = _render_fb2ml(monkeypatch)
    root = _parse_fb2(rendered)
    text = etree.tostring(root, encoding="unicode")

    assert root.tag.endswith("FictionBook")
    assert "OEB Output Καλημέρα 世界" in text
    assert "José" in text
    assert "Niño" in text
    assert "Иван" in text
    assert "Петров" in text
    assert "Éditions Δ" in text
    assert "Series Καλημέρα" in text
    assert "bold Ω" in text
    assert "italic שלום" in text
    for fragment in COMMON_TEXT_FRAGMENTS:
        assert fragment in text
    assert '<image xlink:href="#_0.jpg"' in rendered
    assert 'content-type="image/png"' in rendered
    assert_no_replacement_chars(text, context="FB2MLizer output")

    binary = root.find("{http://www.gribuser.ru/xml/fictionbook/2.0}binary")
    assert binary is not None
    assert binary.attrib["id"] == "_0.jpg"
    assert base64.b64decode("".join(binary.text.split())).startswith(b"\x89PNG")


def test_fb2_output_convert_writes_valid_utf8_xml_with_unicode_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fb2_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_output")
    fb2ml = importlib.import_module("LiuXin_alpha.file_formats.fb2.fb2ml")
    install_minimal_stylizers(monkeypatch)
    _patch_output_transforms(monkeypatch)
    monkeypatch.setattr(fb2ml, "_convert_to_jpeg", lambda raw_data, quality=70: None)

    out_file = tmp_path / "unicode_output.fb2"
    fb2_output.FB2Output(None).convert(
        build_rich_oeb_output_book(),
        str(out_file),
        None,
        _fb2_output_options(),
        null_log(),
    )

    payload = out_file.read_bytes()
    assert payload.startswith(b"<?xml")
    rendered = payload.decode("utf-8", "strict")
    root = _parse_fb2(payload)
    text = etree.tostring(root, encoding="unicode")

    assert "OEB Output Καλημέρα 世界" in text
    assert "مرحبا" in text
    assert "שלום" in text
    assert "你好，世界" in text
    assert "_0.jpg" in rendered
    assert_no_replacement_chars(rendered, context="FB2Output file")


def test_fb2_output_replaces_unserializable_surrogate_metadata_on_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fb2_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_output")
    fb2ml = importlib.import_module("LiuXin_alpha.file_formats.fb2.fb2ml")
    install_minimal_stylizers(monkeypatch)
    _patch_output_transforms(monkeypatch)
    monkeypatch.setattr(fb2ml, "_convert_to_jpeg", lambda raw_data, quality=70: None)

    book = build_rich_oeb_output_book(title="Surrogate boundary Καλημέρα")
    book.metadata.title = [MinimalMetadataValue("Surrogate boundary \ud800 Καλημέρα")]
    out_file = tmp_path / "surrogate_output.fb2"

    fb2_output.FB2Output(None).convert(
        book,
        str(out_file),
        None,
        _fb2_output_options(),
        null_log(),
    )

    rendered = out_file.read_bytes().decode("utf-8", "strict")
    root = _parse_fb2(rendered)
    text = etree.tostring(root, encoding="unicode")

    assert "\ud800" not in rendered
    assert "Surrogate boundary ? Καλημέρα" in text
    assert_no_replacement_chars(rendered, context="FB2Output surrogate boundary")
