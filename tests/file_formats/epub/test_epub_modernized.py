from __future__ import annotations

import importlib
import types
import zipfile
from dataclasses import dataclass, field
from pathlib import Path


def test_epub_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.epub",
        "LiuXin_alpha.file_formats.epub.pages",
        "LiuXin_alpha.file_formats.epub.periodical",
        "LiuXin_alpha.file_formats.epub.cfi.parse",
        "LiuXin_alpha.file_formats.conversion.plugins.epub_input",
        "LiuXin_alpha.file_formats.conversion.plugins.epub_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_initialize_container_writes_minimal_epub(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.epub import initialize_container

    out = tmp_path / "sample.epub"
    extra_entries = [
        ("OPS/alt.opf", "application/oebps-package+xml", b"<package/>"),
        ("META-INF/custom.xml", "application/xml", b"<custom/>"),
    ]
    with initialize_container(str(out), opf_name="OPS/content.opf", extra_entries=extra_entries):
        pass

    with zipfile.ZipFile(out, "r") as zf:
        assert zf.read("mimetype") == b"application/epub+zip"
        container_xml = zf.read("META-INF/container.xml").decode("utf-8")
        assert 'full-path="OPS/content.opf"' in container_xml
        assert 'full-path="OPS/alt.opf"' in container_xml
        assert zf.read("META-INF/custom.xml") == b"<custom/>"


def test_pages_helpers_smoke() -> None:
    from LiuXin_alpha.file_formats.epub.pages import build_name_for, filter_name
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    assert filter_name(" Page XIV ") == "XIV"
    assert filter_name("chapter one") == "chapter one"

    auto = build_name_for(None)
    assert auto(object()) == "1"
    assert auto(object()) == "2"

    root = etree.fromstring(
        b"""
        <html xmlns="http://www.w3.org/1999/xhtml">
          <body><h1>Page 12</h1></body>
        </html>
        """
    )
    name_for = build_name_for("string(.//h:h1)")
    assert name_for(root) == "12"


@dataclass
class _Identifier:
    text: str
    attrib: dict[str, str]

    def __str__(self) -> str:
        return self.text


@dataclass
class _Article:
    href: str
    title: str
    author: str = ""


@dataclass
class _Section:
    href: str
    title: str
    description: str = ""
    articles: list[_Article] = field(default_factory=list)

    def __iter__(self):
        return iter(self.articles)


def test_periodical_sony_metadata_smoke() -> None:
    from LiuXin_alpha.file_formats.epub.periodical import sony_metadata

    metadata = types.SimpleNamespace(
        title=["Daily News"],
        publication_type=["periodical:newspaper:Daily News"],
        date=["2024-05-01"],
        language=["en_US"],
        identifier=[_Identifier(text="urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", attrib={"opf:scheme": "uuid"})],
    )
    toc = [
        _Section(
            href="sec1.xhtml",
            title="World",
            description="World desk",
            articles=[_Article(href="a1.xhtml", title="Alpha", author="Reporter")],
        ),
    ]
    oeb = types.SimpleNamespace(metadata=metadata, toc=toc)

    metadata_xml, atom = sony_metadata(oeb)
    assert "<dc:title>Daily News</dc:title>" in metadata_xml
    assert "<dc:language>en-US</dc:language>" in metadata_xml
    assert b"<feed " in atom
    assert b"World" in atom
    assert b"Alpha" in atom


def test_cfi_parser_handles_paths_offsets_and_params() -> None:
    from LiuXin_alpha.file_formats.epub.cfi.parse import cfi_sort_key, parser

    p = parser()
    path, leftover = p.parse_path("/1:3[a;s=a^,b,c^;d;x=y]")
    assert leftover == ""
    assert path["steps"][0]["num"] == 1
    assert path["steps"][0]["text_offset"] == 3
    ta = path["steps"][0]["text_assertion"]
    assert ta["before"] == "a"
    assert ta["params"]["s"] == ("a,b", "c;d")
    assert ta["params"]["x"] == ("y",)
    assert cfi_sort_key("/1/2/3") == ((1, 2, 3), (0, (0, 0), 0))


def test_epub_input_find_opf_smoke(monkeypatch, tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    (tmp_path / "META-INF").mkdir(parents=True)
    (tmp_path / "OPS").mkdir(parents=True)
    (tmp_path / "OPS" / "book.opf").write_text("<package version='2.0'/>", encoding="utf-8")
    (tmp_path / "META-INF" / "container.xml").write_text(
        """<?xml version="1.0"?>
<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
  <rootfiles>
    <rootfile full-path="OPS/book.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    plugin = EPUBInput(None)
    assert plugin.find_opf() == str(tmp_path / "OPS" / "book.opf")


def test_epub_output_condense_ncx_smoke(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_output import EPUBOutput

    ncx_path = tmp_path / "toc.ncx"
    ncx_path.write_bytes(
        b"""<?xml version="1.0"?>
<ncx>
  <navMap>
    <navPoint>
      <text>  Chapter 1  </text>
    </navPoint>
  </navMap>
</ncx>
"""
    )

    plugin = EPUBOutput(None)
    plugin.opts = types.SimpleNamespace(pretty_print=False)
    plugin.condense_ncx(str(ncx_path))

    condensed = ncx_path.read_text(encoding="utf-8")
    assert "<text>Chapter 1</text>" in condensed
    assert "\n  " not in condensed


def test_epub_add_page_map_runtime_smoke(tmp_path: Path) -> None:
    import types

    from LiuXin_alpha.file_formats.epub.pages import add_page_map

    opf_path = tmp_path / "metadata.opf"
    opf_path.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
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
  <spine toc="ncx"><itemref idref="chap"/></spine>
</package>
""",
        encoding="utf-8",
    )
    (tmp_path / "chapter.xhtml").write_text(
        '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Chapter One</title></head><body><h1>Page 1</h1><h2>Page 2</h2></body></html>',
        encoding="utf-8",
    )
    (tmp_path / "toc.ncx").write_text(
        """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>Smoke OEB</text></docTitle>
  <navMap><navPoint id="n1" playOrder="1"><navLabel><text>Chapter One</text></navLabel><content src="chapter.xhtml"/></navPoint></navMap>
</ncx>
""",
        encoding="utf-8",
    )

    opts = types.SimpleNamespace(page="//h:h1|//h:h2", page_names="string(.)", pretty_print=False)
    add_page_map(str(opf_path), opts)

    opf_text = opf_path.read_text(encoding="utf-8")
    assert 'page-map="' in opf_text
    assert "application/oebps-page-map+xml" in opf_text
