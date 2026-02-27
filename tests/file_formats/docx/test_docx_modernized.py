from __future__ import annotations

import importlib
import io
import pkgutil
import sys
import types
import zipfile
from pathlib import Path


def _write_minimal_docx(path: Path, *, include_styles: bool = False) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
</Types>
"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""
    doc = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>Hello DOCX smoke</w:t></w:r></w:p>
    <w:sectPr/>
  </w:body>
</w:document>
"""
    styles = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:docDefaults>
    <w:rPrDefault><w:rPr><w:lang w:val="en-US"/></w:rPr></w:rPrDefault>
  </w:docDefaults>
</w:styles>
"""
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("word/document.xml", doc)
        if include_styles:
            zf.writestr("word/styles.xml", styles)


class _FakeGuide:
    def set_cover(self, _cover):
        return None


class _FakeOPFCreator:
    def __init__(self, dest_dir, mi):
        self.dest_dir = dest_dir
        self.mi = mi
        self.manifest = []
        self.guide = _FakeGuide()
        self.toc = None

    def create_manifest_from_files_in(self, dirs):
        base = Path(dirs[0])
        for p in base.iterdir():
            if p.is_file():
                media_type = "application/octet-stream"
                if p.suffix == ".html":
                    media_type = "text/html"
                if p.suffix == ".css":
                    media_type = "text/css"
                self.manifest.append(types.SimpleNamespace(path=str(p), media_type=media_type))

    def create_spine(self, entries):
        self.spine = list(entries)

    def render(self, of, ncx, ncx_name, process_guide=None):
        of.write(b"<package/>")
        ncx.write(b"<ncx/>")


def test_docx_modules_import_smoke() -> None:
    pkg = importlib.import_module("LiuXin_alpha.file_formats.docx")
    for module in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        importlib.import_module(module.name)
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.docx_input")


def test_docx_input_convert_delegates_to_convert_callable(monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.docx_input as plugin_mod

    calls = {}

    class _FakeConvert:
        def __init__(self, stream, detect_cover, log):
            calls["stream"] = stream
            calls["detect_cover"] = detect_cover
            calls["log"] = log

        def __call__(self):
            return "docx-result"

    fake_to_html = types.ModuleType("LiuXin_alpha.file_formats.docx.to_html")
    fake_to_html.Convert = _FakeConvert
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.docx.to_html", fake_to_html)

    plugin = plugin_mod.DOCXInput(None)
    options = types.SimpleNamespace(docx_no_cover=False)
    log = types.SimpleNamespace()
    stream = io.BytesIO(b"docx")

    out = plugin.convert(stream, options, "docx", log, {})
    assert out == "docx-result"
    assert calls["stream"] is stream
    assert calls["detect_cover"] is True

    options = types.SimpleNamespace(docx_no_cover=True)
    plugin.convert(stream, options, "docx", log, {})
    assert calls["detect_cover"] is False


def test_docx_writer_color_and_font_family_helpers() -> None:
    from LiuXin_alpha.file_formats.docx.writer.styles import css_font_family_to_docx, parse_css_font_family
    from LiuXin_alpha.file_formats.docx.writer.utils import convert_color

    assert convert_color("currentColor") == "auto"
    assert convert_color("transparent") is None
    assert convert_color("#001") == "000011"
    assert convert_color("rgb(255, 255, 255)") == "FFFFFF"
    assert convert_color("rgba(255, 0, 0, 23)") == "FF0000"

    families = list(parse_css_font_family('"Times New Roman", serif'))
    assert families and families[0] == "Times New Roman"
    assert css_font_family_to_docx("sans-serif") == "Candara"


def test_docx_convert_runtime_smoke_generates_html(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.file_formats.docx.to_html as to_html_mod

    monkeypatch.setattr(to_html_mod, "OPFCreator", _FakeOPFCreator)

    docx_path = tmp_path / "sample.docx"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_minimal_docx(docx_path)

    log = types.SimpleNamespace(
        debug=lambda *args, **kwargs: None,
        warn=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
    )

    result = to_html_mod.Convert(str(docx_path), dest_dir=str(out_dir), log=log)()
    assert result == str(out_dir / "metadata.opf")
    assert (out_dir / "index.html").exists()
    assert "Hello DOCX smoke" in (out_dir / "index.html").read_text(encoding="utf-8")
    assert (out_dir / "docx.css").exists()
    assert (out_dir / "metadata.opf").exists()


def test_docx_convert_reads_styles_without_relationship(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.file_formats.docx.to_html as to_html_mod

    monkeypatch.setattr(to_html_mod, "OPFCreator", _FakeOPFCreator)

    docx_path = tmp_path / "sample_styles.docx"
    out_dir = tmp_path / "out_styles"
    out_dir.mkdir()
    _write_minimal_docx(docx_path, include_styles=True)

    log = types.SimpleNamespace(
        debug=lambda *args, **kwargs: None,
        warn=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
    )

    to_html_mod.Convert(str(docx_path), dest_dir=str(out_dir), log=log)()
    assert (out_dir / "index.html").exists()
    assert (out_dir / "metadata.opf").exists()


def test_docx_input_convert_runtime_smoke_with_minimal_docx(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.docx_input as plugin_mod
    import LiuXin_alpha.file_formats.docx.to_html as to_html_mod

    monkeypatch.setattr(to_html_mod, "OPFCreator", _FakeOPFCreator)
    monkeypatch.chdir(tmp_path)

    docx_path = tmp_path / "input.docx"
    _write_minimal_docx(docx_path)

    log = types.SimpleNamespace(
        debug=lambda *args, **kwargs: None,
        warn=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
    )
    plugin = plugin_mod.DOCXInput(None)
    options = types.SimpleNamespace(docx_no_cover=False)
    with docx_path.open("rb") as stream:
        out = plugin.convert(stream, options, "docx", log, {})

    assert out == str(tmp_path / "metadata.opf")
    assert (tmp_path / "metadata.opf").exists()
    assert (tmp_path / "index.html").exists()
    assert "Hello DOCX smoke" in (tmp_path / "index.html").read_text(encoding="utf-8")
