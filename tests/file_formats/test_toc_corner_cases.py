"""
Provide test toc corner cases utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test toc corner cases through a consuming regression::

        python -m pytest -q tests/file_formats/test_toc_corner_cases.py
"""
from __future__ import annotations

from io import BytesIO

import pytest

from LiuXin_alpha.file_formats.toc import TOC
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _opf_reader_with_spine_toc(toc_name: str):
    """
    Perform the opf reader with spine toc operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf reader with spine toc through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param toc_name: Value supplied for toc name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Manifest(list):
        """
        Provide the manifest contract for validated ebook processing.

        Example:
            Exercise  opf reader with spine toc. Manifest through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def item(self, _name):  # pragma: no cover - explicit API stub
            """
            Perform the item operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with spine toc. Manifest.item through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param _name: Value supplied for name under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    class _Soup:
        """
        Provide the soup contract for validated ebook processing.

        Example:
            Exercise  opf reader with spine toc. Soup through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def find(self, name, **kwargs):
            """
            Perform the find operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with spine toc. Soup.find through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "spine" and kwargs.get("toc") is True:
                return {"toc": toc_name}
            return None

    class _Reader:
        """
        Parse reader data into normalized ebook structures.

        Example:
            Exercise  opf reader with spine toc. Reader through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def __init__(self):
            """
            Initialize and validate the reader state.

            Example:
                Exercise  opf reader with spine toc. Reader.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :return: None; validated state is stored on the receiving object.
            """
            self.soup = _Soup()
            self.manifest = _Manifest()

    return _Reader()


def _opf_reader_with_guide_toc(href: str):
    """
    Perform the opf reader with guide toc operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf reader with guide toc through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param href: Value supplied for href under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Reference:
        """
        Provide the reference contract for validated ebook processing.

        Example:
            Exercise  opf reader with guide toc. Reference through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def __getitem__(self, key):
            """
            Perform the getitem operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with guide toc. Reference.  getitem   through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param key: Metadata, identifier or local-variable key.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if key == "href":
                return href
            raise KeyError(key)

    class _Guide:
        """
        Provide the guide contract for validated ebook processing.

        Example:
            Exercise  opf reader with guide toc. Guide through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def find(self, name, attrs=None, **_kwargs):
            """
            Perform the find operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with guide toc. Guide.find through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param attrs: Value supplied for attrs under the utility contract.
            :param _kwargs: Value supplied for kwargs under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "reference" and attrs == {"type": "toc"}:
                return _Reference()
            return None

    class _Soup:
        """
        Provide the soup contract for validated ebook processing.

        Example:
            Exercise  opf reader with guide toc. Soup through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def find(self, name, **kwargs):
            """
            Perform the find operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with guide toc. Soup.find through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "spine" and kwargs.get("toc") is True:
                return None
            if name == "guide":
                return _Guide()
            return None

    class _Manifest(list):
        """
        Provide the manifest contract for validated ebook processing.

        Example:
            Exercise  opf reader with guide toc. Manifest through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        pass

    class _Reader:
        """
        Parse reader data into normalized ebook structures.

        Example:
            Exercise  opf reader with guide toc. Reader through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        soup = _Soup()
        manifest = _Manifest()

    return _Reader()


def _opf_reader_with_ncx_manifest(path):
    """
    Perform the opf reader with ncx manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise  opf reader with ncx manifest through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Manifest:
        """
        Provide the manifest contract for validated ebook processing.

        Example:
            Exercise  opf reader with ncx manifest. Manifest through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def item(self, name):
            """
            Perform the item operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with ncx manifest. Manifest.item through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "ncx":
                return type("_Item", (), {"path": str(path)})()
            return None

    class _Soup:
        """
        Provide the soup contract for validated ebook processing.

        Example:
            Exercise  opf reader with ncx manifest. Soup through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        def find(self, name, **kwargs):
            """
            Perform the find operation under explicit file-format and conversion rules.

            Example:
                Exercise  opf reader with ncx manifest. Soup.find through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "spine" and kwargs.get("toc") is True:
                return {"toc": "ncx"}
            return None

    class _Reader:
        """
        Parse reader data into normalized ebook structures.

        Example:
            Exercise  opf reader with ncx manifest. Reader through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py
        """
        soup = _Soup()
        manifest = _Manifest()

    return _Reader()


def test_read_ncx_toc_skips_missing_src_and_keeps_children(tmp_path) -> None:
    """
    Perform the test read ncx toc skips missing src and keeps children operation under explicit file-format and conversion rules.

    Example:
        Exercise test read ncx toc skips missing src and keeps children through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    xml = f"""\
    <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
      <navMap>
        <navPoint playOrder="9">
          <navLabel><text> Parent </text></navLabel>
          <content />
          <navPoint playOrder="2">
            <navLabel><text> Child   Title </text></navLabel>
            <content src="chapter.xhtml#sec%201" />
          </navPoint>
        </navPoint>
      </navMap>
    </ncx>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)

    assert len(toc) == 1
    assert toc[0].text == "Child Title"
    assert toc[0].href == "chapter.xhtml"
    assert toc[0].fragment == "sec 1"
    assert toc[0].play_order == 2


def test_read_ncx_toc_invalid_play_order_defaults_to_one(tmp_path) -> None:
    """
    Perform the test read ncx toc invalid play order defaults to one operation under explicit file-format and conversion rules.

    Example:
        Exercise test read ncx toc invalid play order defaults to one through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    xml = """\
    <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
      <navMap>
        <navPoint playOrder="not-an-int">
          <navLabel><text>One</text></navLabel>
          <content src="one.xhtml" />
        </navPoint>
      </navMap>
    </ncx>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)
    assert toc[0].play_order == 1


def test_read_ncx_toc_requires_navmap(tmp_path) -> None:
    """
    Perform the test read ncx toc requires navmap operation under explicit file-format and conversion rules.

    Example:
        Exercise test read ncx toc requires navmap through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    xml = """<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/"></ncx>"""
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    with pytest.raises(ValueError, match="navmap"):
        toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)


def test_toc_tree_helpers_count_purge_depth_flat_and_abspath(tmp_path) -> None:
    """
    Perform the test toc tree helpers count purge depth flat and abspath operation under explicit file-format and conversion rules.

    Example:
        Exercise test toc tree helpers count purge depth flat and abspath through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc = TOC(base_path=str(tmp_path))
    chapter = toc.add_item("chapters/one.xhtml", None, "Chapter", type="chapter")
    section = chapter.add_item("chapters/one.xhtml", "part", "Section", type="section")
    appendix = toc.add_item(str(tmp_path / "appendix.xhtml"), None, "Appendix", type="appendix")

    assert toc.depth() == 3
    assert toc.count("chapter") == 1
    assert toc.count("section") == 1
    assert list(toc.top_level_items()) == [chapter, appendix]
    assert [item.text for item in toc.flat()] == [None, "Chapter", "Section", "Appendix"]
    assert chapter.abspath == str(tmp_path / "chapters" / "one.xhtml")
    assert appendix.abspath == str(tmp_path / "appendix.xhtml")

    removed = toc.purge({"section"})
    assert removed == [section]
    assert section.parent is None
    assert toc.depth() == 2

    toc.remove(appendix)
    assert appendix.parent is None
    assert list(toc.top_level_items()) == [chapter]


def test_toc_purge_respects_keep_count() -> None:
    """
    Perform the test toc purge respects keep count operation under explicit file-format and conversion rules.

    Example:
        Exercise test toc purge respects keep count through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc = TOC()
    keep = toc.add_item("a.xhtml", None, "A", type="page")
    remove = toc.add_item("b.xhtml", None, "B", type="page")

    removed = toc.purge({"page"}, max=1)

    assert removed == [remove]
    assert list(toc) == [keep]


def test_read_html_toc_deduplicates_and_normalizes_text(tmp_path) -> None:
    """
    Perform the test read html toc deduplicates and normalizes text operation under explicit file-format and conversion rules.

    Example:
        Exercise test read html toc deduplicates and normalizes text through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc_html = tmp_path / "toc.html"
    toc_html.write_text(
        (
            "<html><body>"
            '<a href="chap.xhtml#one"> First <b>Chapter</b> </a>'
            '<a href="chap.xhtml#one">Duplicate</a>'
            '<a href="  ">Ignored empty href</a>'
            "<a>No href</a>"
            '<a href="#local"> Local   Link </a>'
            "</body></html>"
        ),
        encoding="utf-8",
    )

    toc = TOC()
    toc.read_html_toc(str(toc_html))

    assert [(x.href, x.fragment, x.text) for x in toc] == [
        ("chap.xhtml", "one", "First Chapter"),
        ("", "local", "Local Link"),
    ]


def test_read_from_opf_uses_baen_top_to_toc_fallback(tmp_path) -> None:
    """
    Perform the test read from opf uses baen top to toc fallback operation under explicit file-format and conversion rules.

    Example:
        Exercise test read from opf uses baen top to toc fallback through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    (tmp_path / "book_toc.htm").write_text(
        '<html><body><a href="chapter.xhtml#start">Chapter</a></body></html>',
        encoding="utf-8",
    )
    opf = _opf_reader_with_spine_toc("book_top.htm")
    toc = TOC(base_path=str(tmp_path))
    toc.read_from_opf(opf)

    assert len(toc) == 1
    assert toc[0].href == "chapter.xhtml"
    assert toc[0].fragment == "start"


def test_read_from_opf_uses_guide_toc_reference(tmp_path) -> None:
    """
    Perform the test read from opf uses guide toc reference operation under explicit file-format and conversion rules.

    Example:
        Exercise test read from opf uses guide toc reference through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    (tmp_path / "inline_toc.xhtml").write_text(
        '<html><body><a href="guide.xhtml#toc">Guide TOC</a></body></html>',
        encoding="utf-8",
    )
    toc = TOC(base_path=str(tmp_path))

    toc.read_from_opf(_opf_reader_with_guide_toc("inline_toc.xhtml"))

    assert [(item.href, item.fragment, item.text) for item in toc] == [("guide.xhtml", "toc", "Guide TOC")]


def test_read_from_opf_uses_ncx_manifest_item_path(tmp_path) -> None:
    """
    Perform the test read from opf uses ncx manifest item path operation under explicit file-format and conversion rules.

    Example:
        Exercise test read from opf uses ncx manifest item path through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ncx = tmp_path / "book.ncx"
    ncx.write_text(
        """\
        <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
          <navMap>
            <navPoint playOrder="3">
              <navLabel><text>NCX Chapter</text></navLabel>
              <content src="ncx.xhtml#start" />
            </navPoint>
          </navMap>
        </ncx>
        """,
        encoding="utf-8",
    )
    toc = TOC(base_path=str(tmp_path))

    toc.read_from_opf(_opf_reader_with_ncx_manifest(ncx))

    assert [(item.href, item.fragment, item.text, item.play_order) for item in toc] == [
        ("ncx.xhtml", "start", "NCX Chapter", 3)
    ]


def test_render_handles_none_href_without_literal_none_text() -> None:
    """
    Perform the test render handles none href without literal none text operation under explicit file-format and conversion rules.

    Example:
        Exercise test render handles none href without literal none text through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc = TOC()
    node = toc.add_item(None, None, "  T \n i  ")
    node.author = "A U"
    node.description = "D E"
    node.toc_thumbnail = "thumb.png"

    stream = BytesIO()
    toc.render(stream, uid="id-1")
    raw = stream.getvalue()

    assert b'src="None"' not in raw
    assert b"toc_thumbnail" in raw
    assert b"author" in raw
    assert b"description" in raw


def test_ebook_toc_module_is_compatibility_alias() -> None:
    """
    Perform the test ebook toc module is compatibility alias operation under explicit file-format and conversion rules.

    Example:
        Exercise test ebook toc module is compatibility alias through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.toc as ebook_toc
    from LiuXin_alpha.file_formats import toc

    assert ebook_toc.TOC is toc.TOC
    assert ebook_toc.NCX_NS == toc.NCX_NS


def test_toc_render_is_deterministic() -> None:
    """
    Perform the test toc render is deterministic operation under explicit file-format and conversion rules.

    Example:
        Exercise test toc render is deterministic through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc = TOC()
    a = toc.add_item("a.xhtml", "intro", "A")
    b = a.add_item("b.xhtml", "sec", "B")
    b.author = "Author"

    one = BytesIO()
    two = BytesIO()
    toc.render(one, uid="uid-1")
    toc.render(two, uid="uid-1")

    assert one.getvalue() == two.getvalue()
