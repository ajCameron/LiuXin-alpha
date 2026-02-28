from __future__ import annotations

import importlib
import pkgutil


def test_odf_modules_import_smoke() -> None:
    import LiuXin_alpha.file_formats.odf as odf_pkg

    for mod in pkgutil.iter_modules(odf_pkg.__path__):
        importlib.import_module(f"LiuXin_alpha.file_formats.odf.{mod.name}")


def test_odf_text_roundtrip_via_odt_container(tmp_path) -> None:
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText, load
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement, extractText
    from LiuXin_alpha.file_formats.odf.text import P

    src = "Hello  world\tΩ\nLine2"
    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, src)
    doc.text.addElement(para)

    path = tmp_path / "sample.odt"
    doc.save(str(path))

    loaded = load(str(path))
    paragraphs = loaded.getElementsByType(P)
    assert paragraphs
    assert extractText(paragraphs[0]) == src


def test_odf2xhtml_smoke_with_unicode(tmp_path) -> None:
    from LiuXin_alpha.file_formats.odf.odf2xhtml import ODF2XHTML
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
    from LiuXin_alpha.file_formats.odf.text import P

    doc = OpenDocumentText()
    para = P()
    addTextToElement(para, "naïve Καλημέρα こんにちは")
    doc.text.addElement(para)

    html = ODF2XHTML(generate_css=True).odf2xhtml(doc)
    assert "<html" in html
    assert "na" in html
    assert "こんにちは" in html


def test_manifestlist_accepts_bytes_and_str() -> None:
    from LiuXin_alpha.file_formats.odf.odfmanifest import manifestlist

    manifest_xml = """<?xml version="1.0" encoding="utf-8"?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
  <manifest:file-entry manifest:media-type="application/vnd.oasis.opendocument.text" manifest:full-path="/"/>
</manifest:manifest>
"""
    expected = {
        "/": {
            "media-type": "application/vnd.oasis.opendocument.text",
            "full-path": "/",
        }
    }
    assert manifestlist(manifest_xml) == expected
    assert manifestlist(manifest_xml.encode("utf-8")) == expected
