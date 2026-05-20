from __future__ import annotations

import importlib
import io

from tests.support.deterministic_conversion import assert_bytes_deterministic
from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)
from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_output_deterministic,
)


def _assert_ascii_rtf(rendered: str) -> None:
    rendered.encode("ascii", "strict")
    assert "\ufffd" not in rendered


def _assert_rtf_escaped_fragments(rendered: str, fragments=COMMON_TEXT_FRAGMENTS) -> None:
    rtfml = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    missing = [fragment for fragment in fragments if rtfml.txt2rtf(fragment) not in rendered]
    if missing:
        raise AssertionError(f"missing RTF-escaped fragments: {missing!r}")


def _install_deterministic_image_backend(monkeypatch) -> None:
    rtfml = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    monkeypatch.setattr(rtfml, "_convert_image_to_jpeg_bytes", lambda data: b"\x01\xab\xfe\x10")
    monkeypatch.setattr(rtfml, "_identify_data", lambda data: (320, 240, "jpeg"))


def test_txt2rtf_preserves_shared_unicode_corpus_as_ascii_escapes() -> None:
    rtfml = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")

    rendered = assert_output_deterministic(
        rtfml.txt2rtf,
        MULTISCRIPT_TEXT,
        context="rtf.txt2rtf",
    )

    _assert_ascii_rtf(rendered)
    _assert_rtf_escaped_fragments(rendered)
    assert r"\u" in rendered


def test_rtfmlizer_serializes_shared_unicode_oeb_as_ascii_rtf(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    _install_deterministic_image_backend(monkeypatch)
    rtfml = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    options = text_output_options()

    rendered_a = rtfml.RTFMLizer(null_log()).extract_content(build_text_output_book(), options)
    rendered_b = rtfml.RTFMLizer(null_log()).extract_content(build_text_output_book(), options)

    assert rendered_a == rendered_b
    assert rendered_a.startswith(r"{\rtf1")
    _assert_ascii_rtf(rendered_a)
    _assert_rtf_escaped_fragments(rendered_a)
    assert r"{\title Unicode \u922?" in rendered_a
    assert r"{\author Jos\u233? \u1048?\u1074?\u1072?\u1085?}" in rendered_a
    assert r"\b" in rendered_a
    assert r"\i" in rendered_a
    assert r"\jpegblip\picw320\pich240" in rendered_a
    assert "01abfe10" in rendered_a


def test_rtf_output_uses_real_serializer_for_shared_unicode_oeb(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    _install_deterministic_image_backend(monkeypatch)
    rtf_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_output")
    options = text_output_options()

    def render_once(_run_name: str) -> bytes:
        out = io.BytesIO()
        rtf_output.RTFOutput(None).convert(build_text_output_book(), out, None, options, null_log())
        return out.getvalue()

    payload = assert_bytes_deterministic(render_once)
    rendered = payload.decode("ascii", "strict")

    _assert_ascii_rtf(rendered)
    _assert_rtf_escaped_fragments(rendered)
    assert rendered.startswith(r"{\rtf1")
    assert r"\jpegblip" in rendered
