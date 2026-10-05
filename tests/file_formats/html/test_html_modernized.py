"""
Provide test html modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test html modernized through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import annotations

import importlib
import types
from pathlib import Path
from zipfile import ZipFile


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def info(self, *parts) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(" ".join(str(x) for x in parts))

    def debug(self, *parts) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(" ".join(str(x) for x in parts))


def test_html_modules_import_smoke() -> None:
    """
    Perform the test html modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test html modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.html")
    importlib.import_module("LiuXin_alpha.file_formats.html.input")
    importlib.import_module("LiuXin_alpha.file_formats.html.meta")
    importlib.import_module("LiuXin_alpha.file_formats.html.to_zip")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.html_output")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.htmlz_output")


def test_html_tostring_serializes_xml_and_strips_comments() -> None:
    """
    Perform the test html tostring serializes xml and strips comments operation under explicit file-format and conversion rules.

    Example:
        Exercise test html tostring serializes xml and strips comments through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test html traverse and get filelist orders operation under explicit file-format and conversion rules.

    Example:
        Exercise test html traverse and get filelist orders through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test html output generate html toc smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test html output generate html toc smoke through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.html_output import HTMLOutput

    class _Node:
        """
        Provide the node contract for validated ebook processing.

        Example:
            Exercise test html output generate html toc smoke. Node through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, href: str, title: str, nodes=None) -> None:
            """
            Initialize and validate the node state.

            Example:
                Exercise test html output generate html toc smoke. Node.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param href: Value supplied for href under the utility contract.
            :param title: Value supplied for title under the utility contract.
            :param nodes: Value supplied for nodes under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
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
    """
    Perform the test liuxin templite basic render operation under explicit file-format and conversion rules.

    Example:
        Exercise test liuxin templite basic render through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.libraries.liuxin_templite import Templite

    t = Templite("Hello ${name}$")
    assert t.render(name="World") == "Hello World"


def test_html_output_convert_end_to_end_smoke(tmp_path: Path) -> None:
    """
    Perform the test html output convert end to end smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test html output convert end to end smoke through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.html_output import HTMLOutput
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    class _MetaItem:
        """
        Provide the metaitem contract for validated ebook processing.

        Example:
            Exercise test html output convert end to end smoke. MetaItem through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, term: str, value: str) -> None:
            """
            Initialize and validate the metaitem state.

            Example:
                Exercise test html output convert end to end smoke. MetaItem.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param term: Value supplied for term under the utility contract.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; validated state is stored on the receiving object.
            """
            self.term = term
            self.value = value

    class _Metadata:
        """
        Provide the metadata contract for validated ebook processing.

        Example:
            Exercise test html output convert end to end smoke. Metadata through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self) -> None:
            """
            Initialize and validate the metadata state.

            Example:
                Exercise test html output convert end to end smoke. Metadata.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            dc = "http://purl.org/dc/elements/1.1/"
            self._data = {
                "title": [_MetaItem(f"{{{dc}}}title", "Smoke Book")],
                "creator": [_MetaItem(f"{{{dc}}}creator", "Smoke Author")],
            }
            self.items = list(self._data)

        def __getitem__(self, key: str):
            """
            Perform the getitem operation under explicit file-format and conversion rules.

            Example:
                Exercise test html output convert end to end smoke. Metadata.  getitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param key: Metadata, identifier or local-variable key.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._data.get(key, [])

    class _ManifestItem:
        """
        Provide the manifestitem contract for validated ebook processing.

        Example:
            Exercise test html output convert end to end smoke. ManifestItem through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, href: str, spine_position, text: str = "", data=None) -> None:
            """
            Initialize and validate the manifestitem state.

            Example:
                Exercise test html output convert end to end smoke. ManifestItem.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param href: Value supplied for href under the utility contract.
            :param spine_position: Value supplied for spine position under the utility contract.
            :param text: Text parsed, normalized or rendered.
            :param data: Value supplied for data under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.href = href
            self.spine_position = spine_position
            self._text = text
            self.data = data
            self.unloaded_to = []

        def __str__(self) -> str:
            """
            Perform the str operation under explicit file-format and conversion rules.

            Example:
                Exercise test html output convert end to end smoke. ManifestItem.  str   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._text

        def unload_data_from_memory(self, memory=None) -> None:
            """
            Perform the unload data from memory operation under explicit file-format and conversion rules.

            Example:
                Exercise test html output convert end to end smoke. ManifestItem.unload data from memory through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param memory: Value supplied for memory under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.unloaded_to.append(memory)

    class _TocNode:
        """
        Provide the tocnode contract for validated ebook processing.

        Example:
            Exercise test html output convert end to end smoke. TocNode through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        def __init__(self, href: str, title: str, nodes=None) -> None:
            """
            Initialize and validate the tocnode state.

            Example:
                Exercise test html output convert end to end smoke. TocNode.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :param href: Value supplied for href under the utility contract.
            :param title: Value supplied for title under the utility contract.
            :param nodes: Value supplied for nodes under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.href = href
            self.title = title
            self.nodes = nodes or []

        def count(self) -> int:
            """
            Perform the count operation under explicit file-format and conversion rules.

            Example:
                Exercise test html output convert end to end smoke. TocNode.count through a consuming regression::

                    python -m pytest -q tests/file_formats/html/test_html_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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


def test_html_zip_plugin_reports_unavailable_gui() -> None:
    """
    Keep the existing headless failure explicit without importing GUI stubs.

    Example:
        Exercise test html zip plugin reports unavailable gui through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import pytest

    from LiuXin_alpha.file_formats.html.to_zip import HTML2ZIP

    with pytest.raises(RuntimeError, match="GUI conversion is unavailable"):
        HTML2ZIP(None).run("book.html")
