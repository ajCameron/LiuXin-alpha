"""
Check calibre's LIT reader, ported.

calibre reads a LIT book's metadata from its OPF, which is LZX-compressed and, in
most books, sealed with DES. calibre's ``lzx`` and ``msdes`` extensions come from
LiuXin's utils plugins: the compiled modules where installed, else pure-Python
ports. The pure-Python DES gave the standard's answers, which are not LIT's (calibre
builds its DES with openclit's S-box tables), so no sealed book could be opened
without the compiled module; it now gives calibre's.

The data repository's LIT books are compared with LiuXin's own reader when it is
checked out. Their readings were also checked against calibre's compiled ``lzx``
and ``msdes``, built from calibre's source: identical.

Example:
    Run these checks::

        python -m pytest -q tests/file_formats/providers/test_ported_lit_reader.py
"""

from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.plugin_runtime import get_file_type_metadata
from LiuXin_alpha.file_formats.providers.ported_calibre import (
    adapt_ported_reader,
    run_ported_reader,
)
from LiuXin_alpha.metadata.upstream_calibre import _compat, _readers, lit_reader


def test_the_lit_port_is_registered_and_dispatched_inside_archives() -> None:
    """
    The LIT port is a provider, and calibre's ZIP and RAR readers hand LIT books to it.

    Example:
        >>> test_the_lit_port_is_registered_and_dispatched_inside_archives()


    :return: None after the registration is checked.
    """
    provider = adapt_ported_reader("Read LIT metadata")
    assert provider.calibre_read_formats == frozenset({"lit"})
    assert provider.provider_id == "calibre-port.read-lit-metadata"
    assert _readers.READER_MODULES["lit"] == "lit"


def test_lzx_comes_with_calibre_s_interface() -> None:
    """
    The stand-in for calibre's ``lzx`` extension takes calibre's calls, whichever plugin serves it.

    Example:
        >>> test_lzx_comes_with_calibre_s_interface()


    :return: None after the calls are checked.
    """
    _compat.lzx.init(17)
    assert _compat.lzx.reset() in (None, 0)
    assert issubclass(_compat.lzx.LZXError, Exception)
    with pytest.raises(_compat.lzx.LZXError):
        _compat.lzx.decompress(b"\xff" * 16, 1024)


def test_a_file_that_is_not_lit_is_refused(tmp_path: Path) -> None:
    """
    A file without LIT's signature is refused, as calibre refuses it.

    Example:
        >>> test_a_file_that_is_not_lit_is_refused(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to work in.
    :return: None after the refusal is checked.
    """
    path = tmp_path / "not.lit"
    path.write_bytes(b"ITSF" + bytes(60))
    with pytest.raises(Exception, match="Not a valid LIT file"):
        run_ported_reader("lit", path, "lit")


def test_real_lit_books_read_as_liuxin_reads_them(
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    """
    On the data repository's LIT books, the port and LiuXin's reader agree, cover included.

    Example:
        >>> test_real_lit_books_read_as_liuxin_reads_them({})  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :return: None after every book is compared.
    """
    books = md_test_files_by_ext.get("lit", [])
    if not books:
        pytest.skip("the data repository is not checked out")
    for path in books:
        ported = run_ported_reader("lit", path, "lit")
        ours = get_file_type_metadata(io.BytesIO(path.read_bytes()), "lit")
        assert ported.title == ours.title, path.name
        assert list(ported.authors) == [str(name) for name in ours.authors], path.name
        assert ported.publisher == ours.publisher, path.name
        our_cover = getattr(ours, "cover_data", None) or (None, None)
        if our_cover[1]:
            assert ported.cover_data[1] == our_cover[1], path.name


# --- CHM ---------------------------------------------------------------------------


class FakeChm:
    """
    The part of calibre's ``CHMReader`` its metadata function reads: no chmlib needed.

    Example:
        >>> FakeChm().GetEncoding()
        'cp1252'
    """

    title = b"Caf\xe9 Book"
    root = "book"

    def get_home(self) -> bytes:
        """
        The home page, with calibre's metadata shapes: spans, and a table of labels.

        Example:
            >>> FakeChm().get_home()[:6]
            b'<html>'


        :return: The page.
        """
        return (
            b"<html><body>"
            b'<span class="author">Ann Writer and Bob Writer</span>'
            b'<span class="imprint">Some Press</span>'
            b"<table><tr><td>ISBN</td><td>: 9780306406157</td></tr>"
            b"<tr><td>Pub Date:</td><td>2001 \xa9</td></tr>"
            b"<tr><td>Pages:</td><td>(321 pages)</td></tr></table>"
            b"</body></html>"
        )

    def GetEncoding(self) -> str:  # noqa: N802 - pychm's name
        """
        The book's encoding, from its language.

        Example:
            See the class.


        :return: The codec name.
        """
        return "cp1252"


def test_chm_metadata_is_read_from_the_home_page() -> None:
    """
    calibre takes a CHM's authors, publisher, ISBN and a date-and-pages comment from its home page.

    Example:
        >>> test_chm_metadata_is_read_from_the_home_page()  # doctest: +SKIP


    :return: None after the fields are checked.
    """
    pytest.importorskip("bs4")
    from LiuXin_alpha.metadata.upstream_calibre import chm

    mi = chm.get_metadata_from_reader(FakeChm())  # type: ignore[arg-type]
    assert mi.title == "Café Book"
    assert mi.authors == ["Ann Writer", "Bob Writer"]
    assert mi.publisher == "Some Press"
    assert mi.isbn == "9780306406157"
    assert mi.comments == "Published 2001, 321 pages."


def test_chm_author_spans_keep_their_entities_as_calibre_does() -> None:
    """
    calibre reads an author span's markup, not its text, so ``&amp;`` stays escaped and splits.

    Example:
        >>> test_chm_author_spans_keep_their_entities_as_calibre_does()  # doctest: +SKIP


    :return: None after the authors are checked.
    """
    pytest.importorskip("bs4")
    from LiuXin_alpha.metadata.upstream_calibre import chm

    class Ampersand(FakeChm):
        """
        A home page whose author span joins two names with an escaped ampersand.

        Example:
            >>> Ampersand().get_home()[:6]
            b'<html>'
        """

        def get_home(self) -> bytes:
            """
            The page.

            Example:
                See the class.


            :return: The page.
            """
            return (
                b'<html><body><span class="author">Ann &amp; Bob</span></body></html>'
            )

    assert chm.get_metadata_from_reader(Ampersand()).authors == ["Ann", "amp; Bob"]  # type: ignore[arg-type]


def test_the_chm_port_needs_pychm(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Without pychm the CHM provider reports itself unavailable, and opening a CHM is refused.

    Example:
        >>> test_the_chm_port_needs_pychm(None)  # doctest: +SKIP


    :param monkeypatch: Makes pychm missing.
    :return: None after the report is checked.
    """
    monkeypatch.setattr(_compat, "chmlib", None)
    availability = adapt_ported_reader("Read CHM metadata").availability()
    assert not availability.available
    assert "pychm" in str(availability.reason)
    with pytest.raises(ImportError, match="pychm"):
        _compat._NoCHMFile()


def test_the_partial_lit_port_keeps_the_type_aliases_it_uses() -> None:
    """
    Keep calibre's ``TYPE_CHECKING`` type aliases that the kept code names, and their imports.

    calibre's LIT reader declares ``LitMap``, ``Atoms`` and ``ParseFrame`` as ``type``
    statements for the type checker. The port keeps only part of the module, and once
    kept only the imports from that block, leaving the annotations naming nothing.

    Example:
        >>> test_the_partial_lit_port_keeps_the_type_aliases_it_uses()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    text = Path(lit_reader.__file__).read_text(encoding="utf-8")
    for alias in ("LitMap", "Atoms", "ParseFrame"):
        assert f"    type {alias} = " in text

    path = Path(__file__).resolve().parents[3] / "scripts" / "port_upstream_calibre.py"
    spec = importlib.util.spec_from_file_location("port_upstream_calibre", path)
    assert spec is not None and spec.loader is not None
    porter = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, porter)
    spec.loader.exec_module(porter)
    source = (
        "from typing import TYPE_CHECKING\n\n"
        "if TYPE_CHECKING:\n"
        "    from collections.abc import Mapping, Sequence\n"
        "    from os import PathLike\n\n"
        "    # names\n"
        "    type Names = Sequence[Inner]\n"
        "    type Inner = Mapping[int, str]\n"
        "    type Unused = PathLike[str]\n\n\n"
        "def f(names: Names) -> None:\n"
        "    return None\n\n\n"
        "def g() -> None:\n"
        "    return None\n"
    )
    kept = porter.keep_only(source, "x.py", ("f",))
    assert "    # names\n    type Names = Sequence[Inner]\n" in kept
    assert "    type Inner = Mapping[int, str]" in kept
    assert "    from collections.abc import Mapping, Sequence" in kept
    assert "Unused" not in kept and "PathLike" not in kept and "def g" not in kept
