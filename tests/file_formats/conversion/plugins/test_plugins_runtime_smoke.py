from __future__ import annotations

import sys
import types
import zipfile

from pathlib import Path


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

    def exception(self, *parts) -> None:
        self._record(*parts)


class _Option:
    def __init__(self, name: str, value) -> None:
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = value


class _HTMLInputStub:
    options = (_Option("breadth_first", False), _Option("dont_package", False))

    def __init__(self, returned_oeb):
        self._returned_oeb = returned_oeb

    def convert(self, stream, options, file_ext, log, accelerators):
        return self._returned_oeb


def _install_html_pipeline_stubs(monkeypatch, returned_oeb) -> None:
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: _HTMLInputStub(returned_oeb) if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Smoke Title", authors=["Smoke Author"])
    )

    fake_oeb_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_oeb_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_oeb_meta)


def test_txt_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.txt_input as txt_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    source = tmp_path / "book.txt"
    source.write_bytes(b"Smoke test paragraph.\n\nAnother line.")

    options = types.SimpleNamespace(
        input_encoding=None,
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
    )
    plugin = txt_input_mod.TXTInput(None)
    log = _Log()

    with source.open("rb") as stream:
        out = plugin.convert(stream, options, "txt", log, {})

    assert out is fake_oeb
    assert plugin.html_postprocess_title == "Smoke Title"


def test_txt_input_textile_fallback_smoke(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.txt_input as txt_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    source = tmp_path / "book.textile"
    source.write_bytes(b"h1. Smoke textile\\n\\nSimple body")

    options = types.SimpleNamespace(
        input_encoding=None,
        paragraph_type="off",
        formatting_type="textile",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
    )
    plugin = txt_input_mod.TXTInput(None)
    log = _Log()

    with source.open("rb") as stream:
        out = plugin.convert(stream, options, "textile", log, {})

    assert out is fake_oeb
    assert plugin.html_postprocess_title == "Smoke Title"


def test_htmlz_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.htmlz_input as htmlz_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    archive = tmp_path / "book.htmlz"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("index.html", "<html><body><p>Smoke</p></body></html>")

    options = types.SimpleNamespace(input_encoding=None, debug_pipeline=None)
    plugin = htmlz_input_mod.HTMLZInput(None)
    log = _Log()

    monkeypatch.chdir(tmp_path)
    with archive.open("rb") as stream:
        out = plugin.convert(stream, options, "htmlz", log, {})

    assert out is fake_oeb


def test_epub_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.epub_input as epub_input_mod

    epub_path = tmp_path / "book.epub"
    with zipfile.ZipFile(epub_path, "w") as zf:
        mimetype = zipfile.ZipInfo("mimetype")
        mimetype.compress_type = zipfile.ZIP_STORED
        zf.writestr(mimetype, b"application/epub+zip")
        zf.writestr(
            "META-INF/container.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
""",
        )
        zf.writestr(
            "content.opf",
            """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>EPUB Smoke</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Smoke Author</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:12121212-3434-5656-7878-909090909090</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap1"/>
  </spine>
</package>
""",
        )
        zf.writestr("text/chap1.xhtml", "<html xmlns='http://www.w3.org/1999/xhtml'><body><p>Smoke</p></body></html>")
        zf.writestr(
            "toc.ncx",
            """<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>EPUB Smoke</text></docTitle>
  <navMap/>
</ncx>
""",
        )

    plugin = epub_input_mod.EPUBInput(None)
    options = types.SimpleNamespace(input_encoding=None, debug_pipeline=None)
    log = _Log()

    monkeypatch.chdir(tmp_path)
    with epub_path.open("rb") as stream:
        out = plugin.convert(stream, options, "epub", log, {})

    assert Path(out).exists()
    assert Path(out).name == "content.opf"
