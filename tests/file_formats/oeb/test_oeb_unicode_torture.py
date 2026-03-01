from __future__ import annotations

import unicodedata
from pathlib import Path

from LiuXin_alpha.file_formats.oeb.base import OEBBook, XHTML_NS, xml2text
from LiuXin_alpha.file_formats.oeb.parse_utils import parse_html
from LiuXin_alpha.file_formats.oeb.polish.check.links import check_link_destinations, check_links
from LiuXin_alpha.file_formats.oeb.polish.parsing import parse as polish_parse
from LiuXin_alpha.file_formats.oeb.polish.parsing import parse_html5 as polish_parse_html5
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.file_formats.oeb.writer import OEBWriter
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.logging import default_log


UNICODE_TITLE = "Unicode Torture — Καλημέρα мир مرحبا हिन्दी 中文 日本語 😀"
UNICODE_BODY = (
    "cafe\u0301 | Ελληνικά | Русский | العربية | हिन्दी | 中文 | 日本語 | 한글 | עברית | ไทย | 𝄞 | 👩‍💻 | "
    "ZWJ:\u200d | RLM:\u200f"
)


def _nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def _write_unicode_oeb_dir(base: Path) -> tuple[Path, str]:
    chapter_name = "章节-über-Δ.xhtml"
    style_name = "样式-ß.css"
    anchor = "sec-main"

    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{UNICODE_TITLE}</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">作者 — Ægir</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap" href="{chapter_name}" media-type="application/xhtml+xml"/>
    <item id="style" href="{style_name}" media-type="text/css"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap"/>
  </spine>
</package>
"""
    html = f"""<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <title>{UNICODE_TITLE}</title>
    <link rel="stylesheet" type="text/css" href="{style_name}" />
  </head>
  <body>
    <h1>{UNICODE_TITLE}</h1>
    <p id="{anchor}" class="класс">{UNICODE_BODY}</p>
    <p><a href="#{anchor}">Jump</a></p>
  </body>
</html>
"""
    css = ".класс { font-style: italic; }\n"
    ncx = f"""<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>{UNICODE_TITLE}</text></docTitle>
  <navMap>
    <navPoint id="n1" playOrder="1">
      <navLabel><text>章節 😀</text></navLabel>
      <content src="{chapter_name}#{anchor}"/>
    </navPoint>
  </navMap>
</ncx>
"""
    (base / "metadata.opf").write_text(opf, encoding="utf-8")
    (base / chapter_name).write_text(html, encoding="utf-8")
    (base / style_name).write_text(css, encoding="utf-8")
    (base / "toc.ncx").write_text(ncx, encoding="utf-8")
    return base / "metadata.opf", chapter_name


def test_oeb_reader_writer_unicode_torture_roundtrip(tmp_path: Path) -> None:
    opf_path, chapter_name = _write_unicode_oeb_dir(tmp_path)

    oeb = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb, str(opf_path))
    assert str(oeb.metadata.title[0]) == UNICODE_TITLE
    body_text = _nfc(xml2text(oeb.spine[0].data))
    for token in ("café", "Ελληνικά", "Русский", "العربية", "हिन्दी", "中文", "日本語", "👩‍💻", "𝄞"):
        assert token in body_text

    out_dir = tmp_path / "out"
    OEBWriter(version="2.0", page_map=True)(oeb, str(out_dir))
    assert (out_dir / "content.opf").exists()
    assert (out_dir / chapter_name).exists()

    oeb2 = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb2, str(out_dir / "content.opf"))
    body_text2 = _nfc(xml2text(oeb2.spine[0].data))
    for token in ("café", "Ελληνικά", "Русский", "العربية", "हिन्दी", "中文", "日本語", "👩‍💻", "𝄞"):
        assert token in body_text2


def test_oeb_parse_utils_unicode_torture_handles_mixed_markup() -> None:
    raw = (
        "<!DOCTYPE html><HTML><HEAD><TITLE></TITLE></HEAD><BODY>"
        "<P>Καλημέρα &hellip; 😀 cafe\u0301 \0 العربية</P>"
        "<DIV>Русский 中文 日本語 한글 עברית ไทย</DIV>"
        "</BODY></HTML>"
    )
    root = parse_html(raw, filename="ユニコード.html")
    assert root.tag == "{%s}html" % XHTML_NS
    title = root.xpath('/h:html/h:head/h:title', namespaces={"h": XHTML_NS})[0]
    assert title.text and title.text.strip()
    text = _nfc(xml2text(root))
    for token in ("Καλημέρα", "…", "😀", "café", "العربية", "Русский", "中文", "日本語", "한글", "עברית", "ไทย"):
        assert token in text
    assert "\0" not in text


def test_oeb_polish_parsing_unicode_torture_handles_namespace_and_surrogates() -> None:
    namespaced = "<ns.1:html><ns.1:body><p>😀 cafe\u0301 \ud800 العربية 中文</p></ns.1:body></ns.1:html>"
    root = polish_parse_html5(namespaced, discard_namespaces=False)
    assert root.tag == "{http://www.w3.org/1999/xhtml}html"
    text = _nfc("".join(root.xpath('//h:body//text()', namespaces={"h": XHTML_NS})))
    assert "😀" in text and "café" in text and "العربية" in text and "中文" in text
    assert "\ud800" not in text

    broken = "<html><body><p>עברית 😀 <div>中文 cafe\u0301"
    parsed = polish_parse(broken, force_html5_parse=True)
    assert parsed.tag == "{http://www.w3.org/1999/xhtml}html"
    parsed_text = _nfc("".join(parsed.xpath('//h:body//text()', namespaces={"h": XHTML_NS})))
    assert "עברית" in parsed_text and "😀" in parsed_text and "中文" in parsed_text and "café" in parsed_text


def test_oeb_link_checks_handle_unicode_paths_and_anchors() -> None:
    chapter = _xhtml_doc(
        '<p id="節-α">مرحبا</p><a href="文本/章.xhtml#節-α">go</a><a href="图像/封面-😀.jpg">img</a>'
    )

    class _Container:
        mime_map = {
            "文本/章.xhtml": "application/xhtml+xml",
            "图像/封面-😀.jpg": "image/jpeg",
        }
        spine_names = [("文本/章.xhtml", True)]
        guide_type_map = {}
        manifest_id_map = {"i1": "文本/章.xhtml", "i2": "图像/封面-😀.jpg"}
        book_type = "epub"

        def parsed(self, name):
            if name == "文本/章.xhtml":
                return chapter
            raise KeyError(name)

        def iterlinks(self, name):
            if name == "文本/章.xhtml":
                return iter(
                    [
                        ("文本/章.xhtml#節-α", 1, 1),
                        ("图像/封面-😀.jpg", 2, 1),
                    ]
                )
            return iter(())

        def href_to_name(self, href, base=None):
            return href.split("#", 1)[0] if href else None

        def exists(self, name):
            return name in self.mime_map

        def opf_xpath(self, expr):
            return []

        def ok_to_be_unmanifested(self, name):
            return True

    assert check_links(_Container()) == []
    dest_errors = check_link_destinations(_Container())
    assert len(dest_errors) == 1
    assert getattr(dest_errors[0], "bad_href", None) == "图像/封面-😀.jpg"


def _xhtml_doc(body_inner: str):
    raw = '<html xmlns="http://www.w3.org/1999/xhtml"><body>' + body_inner + "</body></html>"
    return etree.fromstring(raw.encode("utf-8"))
