from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats import ConversionError
from LiuXin_alpha.file_formats.conversion.plugins.lrf_output import LRFOptions, LRFOutput
from tests.support.deterministic_conversion import (
    assert_bytes_deterministic,
    freeze_module_date_today,
    freeze_uuid4,
    sha256_hex,
)


UNICODE_TORTURE_LINES = [
    "Latin accents: naïve coöperate façade déjà vu.",
    "Greek: Καλημέρα κόσμε.",
    "Cyrillic: Здравствуйте, мир.",
    "Arabic RTL: مرحبا بالعالم.",
    "Hebrew RTL: שלום עולם.",
    "Devanagari: नमस्ते दुनिया।",
    "CJK: 你好，世界。こんにちは世界。안녕하세요 세계.",
    "Emoji ZWJ: 👩🏽\u200d🔬 👨\u200d👩\u200d👧\u200d👦 🏳️\u200d🌈.",
    "Combining marks: a\u0301 e\u0308 o\u0302 n\u0303.",
    "Entities and punctuation: &amp; &nbsp; … — «»",
    "Broken surrogate: \ud800 inside text.",
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

    def exception(self, *args, **kwargs):
        return None


class _Creator:
    role = "aut"

    def __init__(self, value, file_as=""):
        self.value = value
        self.file_as = file_as

    def __str__(self):
        return self.value


class _Title:
    def __init__(self, value, file_as=""):
        self.value = value
        self.file_as = file_as

    def __str__(self):
        return self.value


class _TocEntry:
    def __init__(self, title, href):
        self.title = title
        self.href = href


class _Toc:
    def __init__(self, entries):
        self._entries = list(entries)

    def iterdescendants(self):
        return list(self._entries)


class _InputPlugin:
    is_image_collection = False


def _has_surrogate(s: str) -> bool:
    return any(0xD800 <= ord(ch) <= 0xDFFF for ch in s)


def _make_opts():
    return types.SimpleNamespace(
        margin_left=5,
        margin_top=6,
        margin_right=7,
        margin_bottom=8,
        linearize_tables=False,
        disable_font_rescaling=True,
        base_font_size=0,
        insert_blank_line=False,
        verbose=0,
        enable_autorotation=False,
        header_separation=0,
        header_format="%t by %a",
        wordspace=2.5,
        header=False,
        minimum_indent=0,
        serif_family=None,
        render_tables_as_images=False,
        sans_family=None,
        mono_family=None,
        text_size_multiplier_for_rendered_tables=1.0,
    )


def _make_oeb_book():
    metadata = types.SimpleNamespace(
        creator=[_Creator(" / ".join(UNICODE_TORTURE_LINES), file_as=b"Author\xffSort")],
        title=[_Title("主題 😎 \ud800", file_as=b"Title\xfeSort")],
        description=[("\n".join(UNICODE_TORTURE_LINES)).encode("utf-8", "surrogatepass") + b"\xff\xfe"],
        subject=["Sci-Fi / Unicode Torture"],
    )
    toc = _Toc([_TocEntry("Καλημέρα", "chapter.xhtml#one"), _TocEntry("مرحبا", "chapter.xhtml#two")])
    return types.SimpleNamespace(metadata=metadata, toc=toc)


def _install_fake_oeb_output(monkeypatch: pytest.MonkeyPatch, write_opf: bool = True) -> None:
    class _OEBOutput:
        def convert(self, oeb_book, tdir, input_plugin, opts, log):
            root = Path(tdir)
            xhtml = (
                "<?xml version='1.0' encoding='utf-8'?>\n"
                "<html xmlns='http://www.w3.org/1999/xhtml'><body>\n"
                f"<p>{' '.join(UNICODE_TORTURE_LINES)}</p>\n"
                "</body></html>\n"
            ).encode("utf-8", "surrogatepass") + b"\xff\xfe"
            (root / "chapter.xhtml").write_bytes(xhtml)

            if not write_opf:
                return

            (root / "content.opf").write_text(
                """<?xml version='1.0' encoding='utf-8'?>
<package xmlns='http://www.idpf.org/2007/opf'
         xmlns:dc='http://purl.org/dc/elements/1.1/'
         version='2.0'
         unique-identifier='bookid'>
  <metadata>
    <dc:title>Unicode Torture Output</dc:title>
    <dc:creator xmlns:opf='http://www.idpf.org/2007/opf' opf:role='aut'>Author</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id='bookid'>urn:uuid:11111111-2222-3333-4444-555555555555</dc:identifier>
  </metadata>
  <manifest>
    <item id='chap' href='chapter.xhtml' media-type='application/xhtml+xml'/>
  </manifest>
  <spine>
    <itemref idref='chap'/>
  </spine>
</package>
""",
                encoding="utf-8",
            )

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_output_format = lambda fmt: _OEBOutput() if fmt == "oeb" else None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)


def test_lrf_options_unicode_torture_sanitizes_text_fields() -> None:
    opts = _make_opts()
    opts.margin_left = -10
    options = LRFOptions("out.lrf", opts, _make_oeb_book())

    assert opts.margin_left == 0
    assert isinstance(options.author, str)
    assert isinstance(options.title, str)
    assert isinstance(options.title_sort, str)
    assert isinstance(options.freetext, str)
    assert isinstance(options.category, str)
    assert not _has_surrogate(options.author)
    assert not _has_surrogate(options.title)
    assert not _has_surrogate(options.freetext)


def test_lrf_output_end_to_end_unicode_torture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_oeb_output(monkeypatch, write_opf=True)

    plugin = LRFOutput(None)
    out_file = tmp_path / "unicode_torture.lrf"
    plugin.convert(_make_oeb_book(), str(out_file), _InputPlugin(), _make_opts(), _Log())

    assert out_file.exists()
    payload = out_file.read_bytes()
    assert payload.startswith(b"L\x00R\x00F\x00")
    assert len(payload) > 128


def test_lrf_output_fails_loudly_when_oeb_output_has_no_opf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_oeb_output(monkeypatch, write_opf=False)

    plugin = LRFOutput(None)
    out_file = tmp_path / "no_opf.lrf"

    with pytest.raises(ConversionError, match="OPF"):
        plugin.convert(_make_oeb_book(), str(out_file), _InputPlugin(), _make_opts(), _Log())


def test_lrf_output_bytes_are_deterministic_with_frozen_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import LiuXin_alpha.file_formats.lrf.pylrs.pylrs as pylrs_mod

    freeze_module_date_today(monkeypatch, pylrs_mod, year=2001, month=2, day=3)
    freeze_uuid4(monkeypatch, "11111111-2222-3333-4444-555555555555")
    _install_fake_oeb_output(monkeypatch, write_opf=True)

    def render_once(name: str) -> bytes:
        out_file = tmp_path / name
        LRFOutput(None).convert(_make_oeb_book(), str(out_file), _InputPlugin(), _make_opts(), _Log())
        return out_file.read_bytes()

    first = assert_bytes_deterministic(
        lambda name: render_once(name + ".lrf"),
        run_names=("deterministic_1", "deterministic_2"),
    )
    assert sha256_hex(first) == "96c5f20fba03ab67a47320e9f503898b2fe067db93f91f19de169a49a6cb8c91"
