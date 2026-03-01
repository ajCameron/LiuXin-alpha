from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _write_minimal_oeb_dir(base: Path) -> Path:
    opf = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Smoke OEB</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Tester</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
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
    html = """<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter One</title></head>
  <body><h1>Page 1</h1><h2>Page 2</h2></body>
</html>
"""
    ncx = """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>Smoke OEB</text></docTitle>
  <navMap>
    <navPoint id="n1" playOrder="1">
      <navLabel><text>Chapter One</text></navLabel>
      <content src="chapter.xhtml"/>
    </navPoint>
  </navMap>
</ncx>
"""
    opf_path = base / "metadata.opf"
    opf_path.write_text(opf, encoding="utf-8")
    (base / "chapter.xhtml").write_text(html, encoding="utf-8")
    (base / "toc.ncx").write_text(ncx, encoding="utf-8")
    return opf_path


def test_oeb_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.oeb.base",
        "LiuXin_alpha.file_formats.oeb.normalize_css",
        "LiuXin_alpha.file_formats.oeb.stylizer",
        "LiuXin_alpha.file_formats.oeb.reader",
        "LiuXin_alpha.file_formats.oeb.writer",
        "LiuXin_alpha.file_formats.oeb.iterator.book",
        "LiuXin_alpha.file_formats.oeb.iterator.bookmarks",
        "LiuXin_alpha.file_formats.oeb.transforms.flatcss",
        "LiuXin_alpha.file_formats.oeb.transforms.embed_fonts",
        "LiuXin_alpha.file_formats.oeb.transforms.subset",
        "LiuXin_alpha.file_formats.oeb.transforms.unsmarten",
        "LiuXin_alpha.file_formats.oeb.transforms.rasterize_safe",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_stylizer_fails_cleanly_without_cssutils() -> None:
    stylizer_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.stylizer")
    if getattr(stylizer_mod, "_HAS_CSSUTILS", False):
        pytest.skip("cssutils available in this environment")
    with pytest.raises(ModuleNotFoundError):
        stylizer_mod.Stylizer(None, None, None, None)


def test_oeb_reader_writer_roundtrip_smoke(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.oeb.base import OEBBook
    from LiuXin_alpha.file_formats.oeb.reader import OEBReader
    from LiuXin_alpha.file_formats.oeb.writer import OEBWriter
    from LiuXin_alpha.utils.logging import default_log

    opf_path = _write_minimal_oeb_dir(tmp_path)

    oeb = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb, str(opf_path))
    assert len(oeb.spine) == 1
    assert len(oeb.toc.nodes) >= 1
    assert str(oeb.metadata.title[0]) == "Smoke OEB"

    out_dir = tmp_path / "out"
    OEBWriter(version="2.0", page_map=True)(oeb, str(out_dir))
    assert (out_dir / "content.opf").exists()
    assert (out_dir / "chapter.xhtml").exists()


def test_oeb_subset_transform_fallback_noops_when_subsetter_missing() -> None:
    subset_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.subset")
    if getattr(subset_mod, "_HAS_FONT_SUBSETTER", False):
        pytest.skip("font subsetter available in this environment")

    class _Log:
        def __init__(self):
            self.messages = []

        def warn(self, *args):
            self.messages.append(" ".join(str(x) for x in args))

    log = _Log()
    subset_mod.SubsetFonts()(oeb=object(), log=log, opts=object())
    assert any("Font subsetter is unavailable" in m for m in log.messages)


def test_oeb_unsmarten_transform_replaces_common_punctuation() -> None:
    unsmarten_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.unsmarten")
    out = unsmarten_mod.unsmarten_text("“quote”—ellipses…")
    assert out == '"quote"---ellipses...'


def test_oeb_safe_rasterizer_imports_and_handles_missing_wand() -> None:
    rasterize_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.rasterize")
    rasterize_safe_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.transforms.rasterize_safe")
    if getattr(rasterize_safe_mod, "_HAS_WAND", False):
        assert rasterize_safe_mod.SVGRasterizerSafe() is not None
    else:
        with pytest.raises(rasterize_mod.Unavailable):
            rasterize_safe_mod.SVGRasterizerSafe()
