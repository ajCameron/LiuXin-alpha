"""
Check the PDF reader ported from upstream calibre, with poppler and without it.

calibre's reader runs poppler's ``pdfinfo`` for the Info fields and the XMP
packet, and ``pdftoppm`` for a cover, then reads them with its own code
(``xmp.py`` included). The port runs poppler within limits, in its directory,
without forking a worker. Without poppler, a second provider runs the same
calibre code on fields LiuXin's own PDF object reader gives.

The fixes checked here:

- **No unbounded tools:** poppler runs with a timeout and with memory, CPU and
  output limits.
- **No process-wide directory change:** the tools run in their directory instead.
- **Covers capped in size:** a page too large for 150 dpi is drawn no more than
  3,000 pixels on its longest side; calibre drew a 172-inch page 25,834 pixels
  square.
- **No cubic declaration pattern:** the XMP declaration pattern no longer takes
  cubic time (20 KB took two minutes).
- **No deep XMP:** packets nested more than 32 deep are refused, since each
  lookup reads every match's descendants once per level.

Tests that run poppler are skipped where it is not installed.

Example:
    Run the tests::

        python -m pytest -q tests/file_formats/providers/test_ported_pdf_reader.py
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import shutil
import struct
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

import LiuXin_alpha.utils.plugins as plugin_package
from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.ported_calibre import (
    PortedPdfReader,
    PortedPdfReaderWithoutPoppler,
    adapt_ported_reader,
)
from LiuXin_alpha.metadata.upstream_calibre import _poppler, pdf, xmp

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
CASES = sorted(json.loads((FIXTURES / "cases.json").read_text(encoding="utf-8")))
HAVE_POPPLER = shutil.which("pdfinfo") is not None and shutil.which("pdftoppm") is not None
needs_poppler = pytest.mark.skipif(not HAVE_POPPLER, reason="poppler's pdfinfo and pdftoppm are not installed")
needs_posix = pytest.mark.skipif(os.name != "posix", reason="the fake tools are shell scripts")

INFO: dict[str, Any] = {
    "title": "Corpus Title — café",
    "authors": ("Ann Author", "Bob Writer"),
    "tags": ("Corpus subject", "alpha", "beta"),
    "identifiers": {"isbn": "9780306406157"},
    "book_producer": "Corpus Creator",
}
XMP: dict[str, Any] = {
    "title": "XMP Title — 東京",
    "authors": ("Carol XMP", "Дмитрий XMP"),
    "tags": ("xmp-tag", "タグ"),
    "identifiers": {"doi": "10.4242/corpus"},
    "book_producer": None,
}
NONE = {"tags": None, "identifiers": None, "book_producer": None}
#: What calibre's code makes of each layout, from the fields LiuXin's reader gives.
EXPECTED: dict[str, dict[str, Any]] = {
    **{
        name: INFO
        for name in (
            "plain",
            "object_streams",
            "linearized",
            "aes256_no_password",
            "aes128_no_password",
            "rc4_128_no_password",
            "rc4_40_no_password",
            "aes256_object_streams",
            "broken_xref",
            "huge_page",
            "leading_junk",
            "trailing_junk",
            "aes256_empty_owner",
        )
    },
    "incremental_update": {**INFO, "title": "Updated Title"},
    "incremental_object_streams": {**INFO, "title": "Updated Title"},
    "utf16_strings": {**INFO, "title": "書名 — Книга", "authors": ("金庸",)},
    "xmp_only": XMP,
    "wrong_stream_length": XMP,
    "xmp_and_info": {**XMP, "identifiers": {"doi": "10.4242/corpus", "isbn": "9780306406157"}},
    "aes128_cleartext_xmp": {**XMP, "identifiers": {"doi": "10.4242/corpus", "isbn": "9780306406157"}},
    "hybrid_xref": {"title": "Hybrid Title", "authors": ("Hybrid Author",), **NONE},
    "hybrid_xref_free_entry": {"title": "Hybrid Title", "authors": ("Hybrid Author",), **NONE},
    "indirect_info_values": {"title": "Indirect Title", "authors": ("Indirect Author",), **NONE},
    "utf8_without_bom": {"title": "Título — 東京", "authors": ("Zoë Writer",), **NONE},
}
#: Where poppler 24.02 reads less than LiuXin's reader (see dev-docs/file_formats/pdf).
POPPLER_DIFFERS = frozenset({"hybrid_xref_free_entry", "utf8_without_bom", "aes128_cleartext_xmp"})


@pytest.fixture(autouse=True)
def empty_cache() -> None:
    """
    Start every test with no remembered readings.

    Example:
        >>> "empty_cache" in globals()
        True


    :return: None.
    """
    fork_metadata._cache.clear()


def context(folder: Path) -> ProviderContext:
    """
    Make a provider context working in a folder.

    Example:
        >>> context(Path("/tmp")).options
        {}


    :param folder: The work folder.
    :return: The context.
    """
    return ProviderContext(
        work_dir=folder, options={}, log=logging.getLogger("tests.ported-pdf"), report=ConversionReport()
    )


def fields_of(provider: Any, book: Path) -> dict[str, Any]:
    """
    Read the fields the tests compare, with a provider.

    Example:
        >>> fields_of(PortedPdfReaderWithoutPoppler(), FIXTURES / "plain.pdf")["title"]  # doctest: +SKIP
        'Corpus Title — café'


    :param provider: The provider.
    :param book: The PDF.
    :return: Title, authors, tags, identifiers and book producer.
    """
    values = dict(provider.read_calibre_metadata(book, "pdf", context(book.parent)).values)
    identifiers = values.get("identifiers")
    return {
        "title": values.get("title"),
        "authors": values.get("authors"),
        "tags": values.get("tags"),
        "identifiers": dict(identifiers) if identifiers is not None else None,
        "book_producer": values.get("book_producer"),
    }


def jpeg_size(data: bytes) -> tuple[int, int]:
    """
    Read a JPEG's width and height from its start-of-frame marker.

    Example:
        >>> jpeg_size(b"\\xff\\xd8\\xff\\xc0\\x00\\x11\\x08\\x00\\x02\\x00\\x03")
        (3, 2)


    :param data: The JPEG.
    :return: Width and height.
    """
    at = 2
    while at + 4 <= len(data):
        marker, length = data[at + 1], struct.unpack(">H", data[at + 2 : at + 4])[0]
        if marker in (0xC0, 0xC1, 0xC2):
            height, width = struct.unpack(">HH", data[at + 5 : at + 9])
            return width, height
        at += 2 + length
    raise ValueError("no start-of-frame marker")


@pytest.mark.parametrize("name", CASES)
def test_without_poppler_calibre_reads_every_layout(name: str) -> None:
    """
    Read each corpus file with calibre's code on the fields LiuXin's reader gives.

    Example:
        >>> test_without_poppler_calibre_reads_every_layout("plain")  # doctest: +SKIP


    :param name: The corpus case.
    :return: None; the assertions are the check.
    """
    provider = PortedPdfReaderWithoutPoppler()
    book = FIXTURES / f"{name}.pdf"
    if name == "aes256_user_password":
        with pytest.raises(ValueError, match="Could not read info dict"):
            provider.read_calibre_metadata(book, "pdf", context(book.parent))
        return
    expected = EXPECTED[name]
    module, _ = plugin_package.plugins["aes"]
    if name == "aes128_cleartext_xmp" and getattr(module, "SOURCE", None) == "poppler":
        # Without cryptography the aes plugin hands AES files to pdfinfo, which garbles this
        # packet (see dev-docs/file_formats/pdf), so calibre's code keeps the Info fields.
        expected = INFO
    assert fields_of(provider, book) == expected


@needs_poppler
@pytest.mark.parametrize("name", CASES)
def test_with_poppler_the_port_agrees_and_draws_a_cover(name: str) -> None:
    """
    Read each corpus file with poppler, agreeing with the reading without it where poppler reads as much.

    Example:
        >>> test_with_poppler_the_port_agrees_and_draws_a_cover("plain")  # doctest: +SKIP


    :param name: The corpus case.
    :return: None; the assertions are the check.
    """
    provider = adapt_ported_reader("Read PDF metadata")()
    book = FIXTURES / f"{name}.pdf"
    if name == "aes256_user_password":
        with pytest.raises(ValueError, match="Could not read info dict"):
            provider.read_calibre_metadata(book, "pdf", context(book.parent))
        return
    if name not in POPPLER_DIFFERS:
        assert fields_of(provider, book) == EXPECTED[name]
    cover = provider.extract_cover(book, "pdf", context(book.parent))
    assert cover is not None and cover.image_format == "jpeg"
    width, height = jpeg_size(cover.data)
    assert max(width, height) <= _poppler.COVER_PIXELS
    if name == "huge_page":
        assert (width, height) == (3000, 3000)
    else:
        assert (width, height) == (625, 834)  # 300 by 400 points at 150 dpi, rounded up


@needs_poppler
def test_poppler_runs_in_its_directory_without_changing_ours(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Read a PDF without calling ``os.chdir``, which calibre's worker does and LiuXin's threads would share.

    Example:
        >>> test_poppler_runs_in_its_directory_without_changing_ours  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to record calls to ``os.chdir``.
    :return: None; the assertions are the check.
    """
    before = Path.cwd()
    calls: list[object] = []
    monkeypatch.setattr(os, "chdir", calls.append)
    with (FIXTURES / "plain.pdf").open("rb") as stream:
        metadata = pdf.get_metadata(stream)
    assert metadata.title == "Corpus Title — café"
    assert metadata.cover_data[0] == "jpeg"
    assert calls == []
    assert Path.cwd() == before


def fake_tools(folder: Path, script: str) -> None:
    """
    Put shell scripts named ``pdfinfo`` and ``pdftoppm`` in a folder.

    Example:
        >>> fake_tools(Path("/tmp/x"), "exit 1")  # doctest: +SKIP


    :param folder: The folder to put on the PATH.
    :param script: The scripts' body.
    :return: None.
    """
    for name in ("pdfinfo", "pdftoppm"):
        tool = folder / name
        tool.write_text(f"#!/bin/sh\n{script}\n", encoding="utf-8")
        tool.chmod(0o755)


@needs_posix
@pytest.mark.parametrize(
    ("script", "limits"),
    [
        ("sleep 60", {"timeout": 1.0}),
        ("while :; do echo Title: endless; done", {"output_bytes": 1 << 20}),
        ("kill -SEGV $$", {}),
    ],
    ids=["hangs", "prints-forever", "crashes"],
)
def test_tools_that_hang_print_forever_or_crash_are_stopped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, script: str, limits: dict[str, Any]
) -> None:
    """
    Stop a tool that hangs, prints without end or crashes; calibre then reports no Info dictionary.

    Example:
        >>> test_tools_that_hang_print_forever_or_crash_are_stopped  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to put the fake tools first on the PATH.
    :param tmp_path: A folder for the fake tools.
    :param script: What the fake tools do.
    :param limits: The limits to use instead of the defaults, to keep the test short.
    :return: None; the assertions are the check.
    """
    fake_tools(tmp_path, script)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(_poppler, "PDFINFO_LIMITS", _poppler.ToolLimits(**{**_poppler.PDFINFO_LIMITS.__dict__, **limits}))
    started = time.monotonic()
    with (FIXTURES / "plain.pdf").open("rb") as stream, pytest.raises(ValueError, match="Could not read info dict"):
        pdf.get_metadata(stream)
    assert time.monotonic() - started < 15


def test_availability_follows_poppler_on_the_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Offer the poppler port only where ``pdfinfo`` is on the PATH; the other is always ready.

    Example:
        >>> test_availability_follows_poppler_on_the_path  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to change the PATH.
    :param tmp_path: An empty folder.
    :return: None; the assertions are the check.
    """
    monkeypatch.setenv("PATH", str(tmp_path))
    port = adapt_ported_reader("Read PDF metadata")
    assert issubclass(port, PortedPdfReader)
    missing = port.availability()
    assert not missing.available and "pdfinfo" in str(missing.reason)
    assert PortedPdfReaderWithoutPoppler.availability().available
    if os.name == "posix":
        fake_tools(tmp_path, "exit 1")
        assert port.availability().available


def test_xmp_declarations_and_nesting_are_bounded() -> None:
    """
    Strip a crafted declaration quickly, and refuse deep nesting, keeping the Info fields.

    Example:
        >>> test_xmp_declarations_and_nesting_are_bounded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    head = (
        '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?><x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/">'
    )
    tail = "</rdf:Description></rdf:RDF></x:xmpmeta>"
    title = '<dc:title><rdf:Alt><rdf:li xml:lang="x-default">Packet title</rdf:li></rdf:Alt></dc:title>'
    declarations = head.replace("?><x:", "?><?xml " + 'encoding="' * 100_000 + "?><x:", 1) + title + tail
    started = time.monotonic()
    assert xmp.metadata_from_xmp_packet(declarations.encode()).title == "Packet title"
    assert time.monotonic() - started < 5
    nested = head + "<dc:title>" * 250 + "<rdf:li></rdf:li>" * 50_000 + "</dc:title>" * 250 + tail
    started = time.monotonic()
    with pytest.raises(ValueError, match="nested too deeply"):
        xmp.metadata_from_xmp_packet(nested.encode())
    info_mi = xmp.Metadata("Info title", ["Info Author"])
    assert xmp.consolidate_metadata(info_mi, {"xmp_metadata": nested.encode()}).title == "Info title"
    assert time.monotonic() - started < 5
    deep_enough = head + "<dc:title>" * 27 + title.replace("<dc:title>", "").replace("</dc:title>", "") + "</dc:title>" * 27 + tail
    assert xmp.metadata_from_xmp_packet(deep_enough.encode()).title == "Packet title"


def test_fields_without_poppler_look_like_pdfinfo(tmp_path: Path) -> None:
    """
    Give the Info fields pdfinfo prints, on one line, with its ISO dates, and skip the rest.

    Example:
        >>> test_fields_without_poppler_look_like_pdfinfo(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder for ``src.pdf``.
    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.file_formats.pdf.objects import minimal_pdf

    (tmp_path / "src.pdf").write_bytes(
        minimal_pdf(
            b"<< /Title (Real Title\\nAuthor: Mallory) /Keywords [(a) (b)] /Custom (x)"
            b" /CreationDate (D:20190601120000+05'30') /ModDate (not a date) >>"
        )
    )
    assert _poppler.read_info_without_poppler(str(tmp_path), True) == {
        "Title": "Real Title Author: Mallory",
        "CreationDate": "2019-06-01T12:00:00+05:30",
        "ModDate": "not a date",
    }
    (tmp_path / "src.pdf").write_bytes(b"not a pdf")
    assert _poppler.read_info_without_poppler(str(tmp_path), True) is None


def load_porter() -> ModuleType:
    """
    Load the port script as a module.

    Example:
        >>> load_porter().PKG
        'LiuXin_alpha.metadata.upstream_calibre'


    :return: The module.
    """
    path = Path(__file__).resolve().parents[3] / "scripts" / "port_upstream_calibre.py"
    spec = importlib.util.spec_from_file_location("port_upstream_calibre_for_pdf_tests", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_the_pdf_fixes_are_in_the_port_and_guarded() -> None:
    """
    Keep the ported fixes, and stop a re-port whose calibre code has moved.

    Example:
        >>> test_the_pdf_fixes_are_in_the_port_and_guarded()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    ported_pdf = Path(pdf.__file__).read_text(encoding="utf-8")
    for fix in (
        "_poppler.check_output(list(a), creationflags=creationflags)",
        "check_output, check_call = _poppler.in_directory(outputdir)",
        "*_poppler.cover_scale(ans)",
        "res = _poppler.run_here(read_info, pdfpath, bool(cover))",
    ):
        assert fix in ported_pdf
    assert "os.chdir" not in ported_pdf.replace("rather than changing directory", "")
    ported_xmp = Path(xmp.__file__).read_text(encoding="utf-8")
    assert "_linear_time.linear_pattern(_xml_declaration)" in ported_xmp
    assert "_linear_time.nested_deeper_than(root, 32)" in ported_xmp
    porter = load_porter()
    for source in ("ebooks/metadata/pdf.py", "ebooks/metadata/xmp.py"):
        with pytest.raises(SystemExit, match="no longer matches"):
            porter.apply_patches("x = 1\n", source)
