from __future__ import annotations

import sys
import types
import unicodedata

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput
from LiuXin_alpha.file_formats.conversion.plugins.mobi_output import MOBIOutput
from LiuXin_alpha.file_formats.oeb.base import OEBBook, XHTML
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.utils.logging import default_log
from tests.support.deterministic_conversion import assert_bytes_deterministic, freeze_uuid4, sha256_hex


UNICODE_TORTURE_LINES = [
    "Latin accents: naïve coöperate façade déjà vu.",
    "Greek: Καλημέρα κόσμε.",
    "Cyrillic: Здравствуйте, мир.",
    "Arabic RTL: مرحبا بالعالم.",
    "Hebrew RTL: שלום עולם.",
    "Devanagari: नमस्ते दुनिया।",
    "CJK: 你好，世界。こんにちは世界。안녕하세요 세계.",
    "Combining marks: cafe\u0301 co\u0308perate A\u030A.",
    "Emoji and ZWJ: 👩🏽\u200d🔬 👨\u200d👩\u200d👧\u200d👦 🏳️\u200d🌈 🙂.",
    "Directionality: \u202bRTL block\u202c and \u200fmarks\u200f.",
]


class _Log:
    def __call__(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    warn = warning

    def error(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def _install_customize_ui_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    cbz_plugin = object()
    fake_ui.plugin_for_input_format = lambda fmt: cbz_plugin if fmt == "cbz" else object()
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)


def _profile() -> SimpleNamespace:
    # Minimal profile fields used by MOBI transforms in tests.
    fnums = {8: 3, 10: 4, 12: 5, 14: 6, 16: 7}
    return SimpleNamespace(width=600, height=800, dpi=96, fbase=16, fnums=fnums, mobi_ems_per_blockquote=2)


def _mobi_output_opts(*, mode: str = "old") -> SimpleNamespace:
    return SimpleNamespace(
        mobi_file_type=mode,
        prefer_author_sort=False,
        no_inline_toc=False,
        toc_title=None,
        dont_compress=False,
        mobi_ignore_margins=False,
        mobi_toc_at_start=False,
        extract_to=None,
        share_not_sync=False,
        mobi_keep_original_images=False,
        linearize_tables=False,
        pretty_print=False,
        mobi_periodical=False,
        expand_css=False,
        source=_profile(),
        dest=_profile(),
    )


def _mobi_input_opts() -> SimpleNamespace:
    return SimpleNamespace(input_encoding="utf-8", debug_pipeline=False)


def _write_unicode_oeb_dir(base: Path) -> Path:
    title = "主題 🙂 — Καλημέρα — مرحبا — 漢字"
    author = "Äuthor Ω — लेखक — 著者"
    body = "\n".join(f"<p>{line}</p>" for line in UNICODE_TORTURE_LINES)
    chapter = f"""<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>{title}</title></head>
  <body>
    <h1 id="top">{title}</h1>
    {body}
  </body>
</html>
"""
    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">{author}</dc:creator>
    <dc:language>en</dc:language>
    <dc:date>2001-02-03</dc:date>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:description>{" | ".join(UNICODE_TORTURE_LINES[:4])}</dc:description>
  </metadata>
  <manifest>
    <item id="chap" href="chapter.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap"/>
  </spine>
</package>
"""
    ncx = """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>Unicode Torture</text></docTitle>
  <navMap>
    <navPoint id="n1" playOrder="1">
      <navLabel><text>Start</text></navLabel>
      <content src="chapter.xhtml#top"/>
    </navPoint>
  </navMap>
</ncx>
"""
    (base / "metadata.opf").write_text(opf, encoding="utf-8")
    (base / "chapter.xhtml").write_text(chapter, encoding="utf-8")
    (base / "toc.ncx").write_text(ncx, encoding="utf-8")
    return base / "metadata.opf"


def _load_oeb(opf_path: Path) -> OEBBook:
    oeb = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb, str(opf_path))
    return oeb


def _extract_roundtrip_html(workdir: Path, mobi_path: Path) -> str:
    with mobi_path.open("rb") as stream:
        out = MOBIInput(None).convert(stream, _mobi_input_opts(), "mobi", _Log(), {})
    opf_path = Path(out) if Path(out).is_absolute() else workdir / out
    assert opf_path.exists()
    ET.parse(opf_path)

    html_root = opf_path.parent
    html_candidates = sorted(
        list(html_root.rglob("*.html")) + list(html_root.rglob("*.xhtml")), key=lambda p: p.stat().st_size
    )
    assert html_candidates, "No extracted HTML files found after MOBIInput roundtrip"
    return html_candidates[-1].read_text(encoding="utf-8", errors="replace")


def test_mobi_output_end_to_end_unicode_torture_old_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_customize_ui_stub(monkeypatch)

    workdir = tmp_path / "mobi_unicode_old"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    oeb = _load_oeb(_write_unicode_oeb_dir(workdir))
    out = workdir / "unicode_torture_old.mobi"
    MOBIOutput(None).convert(oeb, str(out), None, _mobi_output_opts(mode="old"), _Log())

    assert out.exists()
    assert out.stat().st_size > 4096

    roundtrip_html = unicodedata.normalize("NFC", _extract_roundtrip_html(workdir, out))
    probes = [
        "naïve",
        "Καλημέρα",
        "مرحبا",
        "Здравствуйте",
        "漢字",
        "🙂",
    ]
    hits = sum(1 for p in probes if unicodedata.normalize("NFC", p) in roundtrip_html)
    assert hits >= 4


def test_mobi_output_handles_lone_surrogate_by_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_customize_ui_stub(monkeypatch)

    workdir = tmp_path / "mobi_surrogate"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    oeb = _load_oeb(_write_unicode_oeb_dir(workdir))
    oeb.metadata.title[0].value = str(oeb.metadata.title[0]) + "\ud800"

    out = workdir / "unicode_surrogate_old.mobi"
    MOBIOutput(None).convert(oeb, str(out), None, _mobi_output_opts(mode="old"), _Log())

    assert out.exists()
    assert out.stat().st_size > 4096

    html = _extract_roundtrip_html(workdir, out)
    assert "\ud800" not in html


def test_mobi_output_new_mode_fails_cleanly_without_cssutils(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_customize_ui_stub(monkeypatch)

    workdir = tmp_path / "mobi_new_mode"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    oeb = _load_oeb(_write_unicode_oeb_dir(workdir))
    out = workdir / "unicode_torture_new.mobi"

    try:
        MOBIOutput(None).convert(oeb, str(out), None, _mobi_output_opts(mode="new"), _Log())
    except RuntimeError as exc:
        assert "cssutils" in str(exc).lower()
    else:
        assert out.exists()
        assert out.stat().st_size > 4096


def test_mobi_output_old_mode_is_deterministic_with_frozen_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_customize_ui_stub(monkeypatch)

    import LiuXin_alpha.file_formats.mobi.writer2.main as writer2_main

    monkeypatch.setattr(writer2_main.random, "randint", lambda a, b: 0x12345678)
    monkeypatch.setattr(writer2_main.time, "time", lambda: 946684800.0)  # 2000-01-01T00:00:00Z
    freeze_uuid4(monkeypatch, "11111111-2222-3333-4444-555555555555")

    input_dir = tmp_path / "mobi_det_input"
    input_dir.mkdir()
    opf_path = _write_unicode_oeb_dir(input_dir)

    def render_once(name: str) -> bytes:
        run_dir = tmp_path / name
        run_dir.mkdir()
        monkeypatch.chdir(run_dir)
        oeb = _load_oeb(opf_path)
        # EXTH date/timestamp fields must be stable to get byte-identical containers.
        oeb.metadata.clear("date")
        oeb.metadata.add("date", "2001-02-03T00:00:00+00:00")
        oeb.metadata.clear("timestamp")
        oeb.metadata.add("timestamp", "2001-02-03T00:00:00+00:00")
        out = run_dir / "deterministic.mobi"
        MOBIOutput(None).convert(oeb, str(out), None, _mobi_output_opts(mode="old"), _Log())
        return out.read_bytes()

    first = assert_bytes_deterministic(
        render_once,
        run_names=("det_run_1", "det_run_2"),
    )
    assert len(first) > 4096
    assert sha256_hex(first) == "fc8223387984b38d012bb9d2e3b9336de53854ff9cd696cb1359b3ec14277aeb"
