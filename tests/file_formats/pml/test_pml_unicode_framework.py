from __future__ import annotations

import importlib
import zipfile
from pathlib import Path

from tests.support.deterministic_conversion import assert_bytes_deterministic
from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)
from tests.support.file_format_unicode import assert_no_replacement_chars


SUPPORTED_ROUNDTRIP_FRAGMENTS = (
    "café",
    "Καλημ?ρα",
    "שלום",
    "cafe",
    "A",
)


def _pml_options(**overrides):
    return text_output_options(
        pml_output_encoding="cp1252",
        full_image_depth=False,
        remove_paragraph_spacing=False,
        **overrides,
    )


def _assert_ascii_pml(rendered: str) -> None:
    rendered.encode("ascii", "strict")
    assert_no_replacement_chars(rendered, context="PML output")


def test_pmlmlizer_serializes_shared_oeb_with_pml_unicode_escapes(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    pmlml = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlml")

    rendered_a = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())
    rendered_b = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())

    assert rendered_a == rendered_b
    _assert_ascii_pml(rendered_a)
    assert r"\a233" in rendered_a
    assert r"\U039A\U03B1\U03BB\U03B7\U03BC?\U03C1\U03B1" in rendered_a
    assert r"\U05E9\U05DC\U05D5\U05DD" in rendered_a
    assert "??????" in rendered_a
    assert r"\m=" in rendered_a
    assert r"\Bbold \U03A9\B" in rendered_a
    assert r"\iitalic \U05E9\U05DC\U05D5\U05DD\i" in rendered_a


def test_pmlmlizer_output_roundtrips_supported_foreign_language_fragments(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    pmlml = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlml")
    converter = importlib.import_module("LiuXin_alpha.file_formats.pml.pmlconverter")

    pml = pmlml.PMLMLizer(null_log()).extract_content(build_text_output_book(), _pml_options())
    html = converter.pml_to_html(pml)

    for fragment in SUPPORTED_ROUNDTRIP_FRAGMENTS:
        assert fragment in html
    assert_no_replacement_chars(html, context="PML roundtrip")


def test_pml_output_writes_deterministic_pmlz_with_unicode_escaped_pml(tmp_path: Path, monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    pml_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.pml_output")

    def render_once(run_name: str) -> bytes:
        output_path = tmp_path / f"{run_name}.pmlz"
        pml_output.PMLOutput(None).convert(build_text_output_book(), str(output_path), None, _pml_options(), null_log())
        return output_path.read_bytes()

    payload = assert_bytes_deterministic(render_once)

    output_path = tmp_path / "inspect.pmlz"
    output_path.write_bytes(payload)
    with zipfile.ZipFile(output_path) as zf:
        names = sorted(zf.namelist())
        index = zf.read("index.pml").decode("cp1252", "strict")

    assert "index.pml" in names
    assert "index_img/cover.png" not in names
    _assert_ascii_pml(index)
    assert r"\a233" in index
    assert r"\U039A\U03B1\U03BB\U03B7\U03BC?\U03C1\U03B1" in index
