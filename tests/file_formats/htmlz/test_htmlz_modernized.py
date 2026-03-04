from __future__ import annotations

import importlib
import sys
import types

from pathlib import Path
from zipfile import ZipFile

import pytest


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


def test_htmlz_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.htmlz.oeb2html")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.htmlz_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.htmlz_output")


def test_oeb2html_get_css_handles_bytes_and_text() -> None:
    from LiuXin_alpha.file_formats.htmlz.oeb2html import OEB2HTML

    css_item_1 = types.SimpleNamespace(media_type="text/css", data=types.SimpleNamespace(cssText=b"a{b:c;}"))
    css_item_2 = types.SimpleNamespace(media_type="text/css", data=types.SimpleNamespace(cssText="x{y:z;}"))
    oeb_book = types.SimpleNamespace(manifest=[css_item_1, css_item_2])

    out = OEB2HTML(_Log()).get_css(oeb_book)

    assert isinstance(out, str)
    assert "a{b:c;}" in out
    assert "x{y:z;}" in out


def test_oeb2html_class_css_inline_mode_embeds_style(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.htmlz.oeb2html as oeb2html_mod

    class _FakeData:
        def find(self, *args, **kwargs):
            return object()

    fake_item = types.SimpleNamespace(href="text/ch1.xhtml", data=_FakeData())
    oeb_book = types.SimpleNamespace(spine=[fake_item], manifest=[fake_item])

    monkeypatch.setattr(oeb2html_mod, "Stylizer", lambda *args, **kwargs: object())
    monkeypatch.setattr(oeb2html_mod, "rewrite_links", lambda *args, **kwargs: None)
    monkeypatch.setattr(oeb2html_mod.OEB2HTMLClassCSSizer, "map_resources", lambda self, oeb_book: None)
    monkeypatch.setattr(oeb2html_mod.OEB2HTMLClassCSSizer, "rewrite_ids", lambda self, root, page: None)
    monkeypatch.setattr(oeb2html_mod.OEB2HTMLClassCSSizer, "dump_text", lambda self, elem, stylizer, page: ["<p>x</p>"])
    monkeypatch.setattr(oeb2html_mod.OEB2HTMLClassCSSizer, "get_css", lambda self, oeb: ".x { color: red; }")

    opts = types.SimpleNamespace(htmlz_class_style="inline")
    out = oeb2html_mod.OEB2HTMLClassCSSizer(_Log()).oeb2html(oeb_book, opts)

    assert '<style type="text/css">.x { color: red; }</style>' in out


def test_oeb2html_class_css_helper_sets_inline_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.htmlz.oeb2html as oeb2html_mod

    monkeypatch.setattr(
        oeb2html_mod.OEB2HTMLClassCSSizer,
        "oeb2html",
        lambda self, oeb_book, opts: "<html><body>ok</body></html>",
    )

    opts = types.SimpleNamespace(htmlz_class_style="external")
    out, images = oeb2html_mod.oeb2html_class_css(types.SimpleNamespace(), _Log(), opts)

    assert out.startswith("<html>")
    assert images == {}
    assert opts.htmlz_class_style == "inline"


def test_htmlz_output_convert_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.htmlz_output as htmlz_output_mod
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    fake_oeb2html_mod = types.ModuleType("LiuXin_alpha.file_formats.htmlz.oeb2html")

    class _HTMLizer:
        def __init__(self, log):
            self.log = log
            self.images = {}

        def oeb2html(self, oeb_book, opts):
            return "<html><body>Smoke HTMLZ output</body></html>"

        def get_css(self, oeb_book):
            return ".x { color: red; }"

    fake_oeb2html_mod.OEB2HTMLClassCSSizer = _HTMLizer
    fake_oeb2html_mod.OEB2HTMLInlineCSSizer = _HTMLizer
    fake_oeb2html_mod.OEB2HTMLNoCSSizer = _HTMLizer

    fake_opf2 = types.ModuleType("LiuXin_alpha.file_formats.opf.opf2")

    class _OPF:
        def __init__(self, stream):
            self.stream = stream

        def to_book_metadata(self):
            return types.SimpleNamespace(cover=None)

    fake_opf2.OPF = _OPF
    fake_opf2.metadata_to_opf = lambda mi: b"<package/>"

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.htmlz.oeb2html", fake_oeb2html_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.opf.opf2", fake_opf2)

    class _Metadata:
        title = ["Smoke Title"]
        cover = []

        @staticmethod
        def to_opf1():
            return etree.Element("package")

    oeb_book = types.SimpleNamespace(metadata=_Metadata(), guide={}, manifest=[])

    opts = types.SimpleNamespace(
        htmlz_css_type="class",
        htmlz_class_style="external",
        htmlz_title_filename=False,
    )
    out_file = tmp_path / "out.htmlz"

    plugin = htmlz_output_mod.HTMLZOutput(None)
    plugin.convert(oeb_book, str(out_file), None, opts, _Log())

    assert out_file.exists()
    with ZipFile(out_file, "r") as zf:
        names = set(zf.namelist())

    assert "index.html" in names
    assert "style.css" in names
    assert "metadata.opf" in names


def test_htmlz_input_convert_uses_utf8_fallback_and_rewinds_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.htmlz_input as htmlz_input_mod
    import LiuXin_alpha.file_formats.opf.opf2 as _opf2  # Preload OPF stack before chardet monkeypatching.
    import LiuXin_alpha.file_formats.chardet as chardet_mod

    archive = tmp_path / "book.htmlz"
    with ZipFile(archive, "w") as zf:
        zf.writestr("index.html", "<html><body>café</body></html>".encode("utf-8"))

    class _Opt:
        def __init__(self, name: str, value):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    sentinel_oeb = types.SimpleNamespace(
        metadata=types.SimpleNamespace(),
        manifest=types.SimpleNamespace(generate=lambda *a, **k: ("id", "href"), add=lambda *a, **k: None),
        guide=types.SimpleNamespace(add=lambda *a, **k: None),
    )

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: types.SimpleNamespace(
        options=(_Opt("breadth_first", False),),
        convert=lambda stream, options, file_ext, log, accelerators: sentinel_oeb,
    )

    called = {"seeked": False}

    def _get_file_type_metadata(stream, file_ext):
        called["seeked"] = stream.tell() == 0
        return types.SimpleNamespace()

    fake_ui.get_file_type_metadata = _get_file_type_metadata

    fake_meta_transform = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta_transform.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta_transform)
    monkeypatch.setattr(chardet_mod, "xml_to_unicode", lambda raw, *a, **k: ("", None))

    monkeypatch.chdir(tmp_path)
    options = types.SimpleNamespace(input_encoding=None, debug_pipeline="keep")
    plugin = htmlz_input_mod.HTMLZInput(None)

    with archive.open("rb") as stream:
        out = plugin.convert(stream, options, "htmlz", _Log(), {})

    assert out is sentinel_oeb
    assert options.input_encoding == "utf-8"
    assert options.debug_pipeline == "keep"
    assert called["seeked"] is True


def test_htmlz_input_does_not_extract_archive_into_project_root_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    project_root: Path,
) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.htmlz_input as htmlz_input_mod

    archive = tmp_path / "leak-check.htmlz"
    leak_names = {
        "__htmlz_leak_probe_57.gif",
        "__htmlz_leak_probe_61.xhtml",
        "__htmlz_leak_probe_metadata.opf",
    }
    with ZipFile(archive, "w") as zf:
        zf.writestr("index.html", "<html><body>test</body></html>".encode("utf-8"))
        zf.writestr("__htmlz_leak_probe_57.gif", b"GIF89a")
        zf.writestr("__htmlz_leak_probe_61.xhtml", b"<html/>")
        zf.writestr("__htmlz_leak_probe_metadata.opf", b"<package/>")

    class _Opt:
        def __init__(self, name: str, value):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: types.SimpleNamespace(
        options=(_Opt("breadth_first", False),),
        convert=lambda stream, options, file_ext, log, accelerators: (_ for _ in ()).throw(
            RuntimeError("forced html conversion failure")
        ),
    )
    fake_ui.get_file_type_metadata = lambda stream, file_ext: types.SimpleNamespace()

    fake_meta_transform = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta_transform.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta_transform)
    monkeypatch.chdir(project_root)

    before = {name for name in leak_names if (project_root / name).exists()}
    options = types.SimpleNamespace(input_encoding=None, debug_pipeline="keep")
    plugin = htmlz_input_mod.HTMLZInput(None)
    with archive.open("rb") as stream:
        with pytest.raises(RuntimeError, match="forced html conversion failure"):
            plugin.convert(stream, options, "htmlz", _Log(), {})
    after = {name for name in leak_names if (project_root / name).exists()}

    assert after == before
