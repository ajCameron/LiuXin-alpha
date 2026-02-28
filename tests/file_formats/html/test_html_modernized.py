from __future__ import annotations

import importlib
import types

from pathlib import Path
from zipfile import ZipFile


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def debug(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))


def test_html_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.html")
    importlib.import_module("LiuXin_alpha.file_formats.html.input")
    importlib.import_module("LiuXin_alpha.file_formats.html.meta")
    importlib.import_module("LiuXin_alpha.file_formats.html.to_zip")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.html_output")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.htmlz_output")


def test_html_tostring_serializes_xml_and_strips_comments() -> None:
    from LiuXin_alpha.file_formats.html import tostring
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    root = etree.fromstring(b"<html><body>Smoke<!--comment--></body></html>")

    serialized = tostring(root, strip_comments=True, pretty_print=False)

    assert isinstance(serialized, bytes)
    text = serialized.decode("utf-8")
    assert text.startswith('<?xml version="1.0" encoding="utf-8" ?>')
    assert "<!--" not in text
    assert "Smoke" in text


def test_html_traverse_and_get_filelist_orders(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.html.input import get_filelist, traverse

    (tmp_path / "index.html").write_text(
        "<html><head><title>Index</title></head><body>"
        "<a href='chapter1.html'>One</a><a href='chapter2.html'>Two</a>"
        "</body></html>",
        encoding="utf-8",
    )
    (tmp_path / "chapter1.html").write_text(
        "<html><head><title>Chapter 1</title></head><body><a href='chapter3.html'>Next</a></body></html>",
        encoding="utf-8",
    )
    (tmp_path / "chapter2.html").write_text(
        "<html><head><title>Chapter 2</title></head><body></body></html>",
        encoding="utf-8",
    )
    (tmp_path / "chapter3.html").write_text(
        "<html><head><title>Chapter 3</title></head><body></body></html>",
        encoding="utf-8",
    )

    flat, depth = traverse(str(tmp_path / "index.html"), max_levels=5, verbose=0, encoding="utf-8")
    assert [Path(x.path).name for x in flat] == ["index.html", "chapter1.html", "chapter2.html", "chapter3.html"]
    assert [Path(x.path).name for x in depth] == ["index.html", "chapter1.html", "chapter3.html", "chapter2.html"]

    opts = types.SimpleNamespace(max_levels=5, verbose=0, input_encoding="utf-8", breadth_first=False)
    dfs_list = get_filelist(str(tmp_path / "index.html"), str(tmp_path), opts, _Log())
    assert [Path(x.path).name for x in dfs_list] == ["index.html", "chapter1.html", "chapter3.html", "chapter2.html"]

    opts.breadth_first = True
    bfs_list = get_filelist(str(tmp_path / "index.html"), str(tmp_path), opts, _Log())
    assert [Path(x.path).name for x in bfs_list] == ["index.html", "chapter1.html", "chapter2.html", "chapter3.html"]


def test_html_output_generate_html_toc_smoke(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.html_output import HTMLOutput

    class _Node:
        def __init__(self, href: str, title: str, nodes=None) -> None:
            self.href = href
            self.title = title
            self.nodes = nodes or []

    toc_root = _Node("", "", nodes=[_Node("chapter1.html", "Chapter One"), _Node("chapter2.html", "Chapter Two")])
    oeb_book = types.SimpleNamespace(toc=toc_root)

    plugin = HTMLOutput(None)
    out = plugin.generate_html_toc(oeb_book, str(tmp_path / "book.html"), str(tmp_path))

    assert isinstance(out, str)
    assert "Chapter One" in out
    assert "Chapter Two" in out


def test_liuxin_templite_basic_render() -> None:
    from LiuXin_alpha.utils.liuxin_templite import Templite

    t = Templite("Hello ${name}$")
    assert t.render(name="World") == "Hello World"


def test_html_output_convert_end_to_end_smoke(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.html_output import HTMLOutput
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    class _MetaItem:
        def __init__(self, term: str, value: str) -> None:
            self.term = term
            self.value = value

    class _Metadata:
        def __init__(self) -> None:
            dc = "http://purl.org/dc/elements/1.1/"
            self._data = {
                "title": [_MetaItem(f"{{{dc}}}title", "Smoke Book")],
                "creator": [_MetaItem(f"{{{dc}}}creator", "Smoke Author")],
            }
            self.items = list(self._data)

        def __getitem__(self, key: str):
            return self._data.get(key, [])

    class _ManifestItem:
        def __init__(self, href: str, spine_position, text: str = "", data=None) -> None:
            self.href = href
            self.spine_position = spine_position
            self._text = text
            self.data = data
            self.unloaded_to = []

        def __str__(self) -> str:
            return self._text

        def unload_data_from_memory(self, memory=None) -> None:
            self.unloaded_to.append(memory)

    class _TocNode:
        def __init__(self, href: str, title: str, nodes=None) -> None:
            self.href = href
            self.title = title
            self.nodes = nodes or []

        def count(self) -> int:
            return len(self.nodes)

    xhtml = etree.fromstring(
        b"""
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter One</title></head>
  <body><p>Hello smoke output.</p></body>
</html>
"""
    )

    spine_item = _ManifestItem("text/ch1.xhtml", 0, data=xhtml)
    css_item = _ManifestItem("styles/main.css", None, text="body { color: #333; }")

    oeb_book = types.SimpleNamespace(
        metadata=_Metadata(),
        toc=_TocNode("", "", nodes=[_TocNode("text/ch1.xhtml", "Chapter One")]),
        manifest=[spine_item, css_item],
        spine=[spine_item],
    )

    plugin = HTMLOutput(None)
    opts = types.SimpleNamespace(
        template_html_index=None,
        template_html=None,
        template_css=None,
        extract_to=None,
    )
    out_zip = tmp_path / "smoke_html_output.zip"

    plugin.convert(oeb_book, str(out_zip), None, opts, _Log())

    assert out_zip.exists()
    with ZipFile(out_zip) as zf:
        names = set(zf.namelist())
    assert "smoke_html_output.html" in names
    assert "smoke_html_output_files/calibreHtmlOutBasicCss.css" in names
    assert "smoke_html_output_files/text/ch1.xhtml" in names
    assert "smoke_html_output_files/styles/main.css" in names
