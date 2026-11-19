"""
Verify LiuXin's PDF metadata source on every storage layout in the PDF metadata corpus.

The corpus (``tests/fixtures/pdf_metadata``) stores the same metadata in object
streams, incremental updates, hybrid files, encrypted files and more. The old reader
found nothing in object streams and returned ciphertext for encrypted files; these
tests pin what the metadata source maps from each layout now.

Example:
    Run the tests::

        python -m pytest -q tests/metadata/file_sources/test_pdf_storage_layouts.py
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

import LiuXin_alpha.utils.plugins as plugin_package
from LiuXin_alpha.metadata.file_sources import pdf

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"

INFO = ("Corpus Title — café", ["Ann Author", "Bob Writer"], "9780306406157")
XMP = ("XMP Title — 東京", ["Carol XMP", "Дмитрий XMP"], None)
#: Title, authors and ISBN expected from each case; XMP wins over Info where both exist.
EXPECTED: dict[str, tuple[str, list[str], str | None]] = {
    "plain": INFO,
    "object_streams": INFO,
    "incremental_update": ("Updated Title", INFO[1], INFO[2]),
    "incremental_object_streams": ("Updated Title", INFO[1], INFO[2]),
    "linearized": INFO,
    "aes256_no_password": INFO,
    "aes128_no_password": INFO,
    "rc4_128_no_password": INFO,
    "rc4_40_no_password": INFO,
    "aes256_object_streams": INFO,
    "utf16_strings": ("書名 — Книга", ["金庸"], INFO[2]),
    "xmp_only": XMP,
    "xmp_and_info": (XMP[0], XMP[1], INFO[2]),
    "broken_xref": INFO,
    "huge_page": INFO,
    "hybrid_xref": ("Hybrid Title", ["Hybrid Author"], None),
    "hybrid_xref_free_entry": ("Hybrid Title", ["Hybrid Author"], None),
    "indirect_info_values": ("Indirect Title", ["Indirect Author"], None),
    "utf8_without_bom": ("Título — 東京", ["Zoë Writer"], None),
    "leading_junk": INFO,
    "trailing_junk": INFO,
    "wrong_stream_length": XMP,
    "aes128_cleartext_xmp": (XMP[0], XMP[1], INFO[2]),
    "aes256_empty_owner": INFO,
}


def _authors(md) -> list[str]:
    """
    List a metadata record's authors in order.

    Example:
        Exercise authors through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_storage_layouts.py


    :param md: The metadata record.
    :return: The authors.
    """
    raw = md.authors
    return [raw] if isinstance(raw, str) else list(raw or [])


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_metadata_source_reads_each_storage_layout(name: str) -> None:
    """
    Read a corpus file through the metadata source, by path and by stream.

    Example:
        Exercise test metadata source reads each storage layout through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_storage_layouts.py


    :param name: The corpus case.
    :return: None; the function records state or raises through its assertions.
    """
    path = FIXTURES / f"{name}.pdf"
    title, authors, isbn = EXPECTED[name]
    module, _ = plugin_package.plugins["aes"]
    if name == "aes128_cleartext_xmp" and getattr(module, "SOURCE", None) == "poppler":
        # Without cryptography the aes plugin hands AES files to pdfinfo, which garbles this
        # packet (see dev-docs/file_formats/pdf), so the Info fields stand.
        title, authors, isbn = INFO
    md = pdf.get_metadata_inplace(path)
    assert md.title == title
    assert _authors(md) == authors
    assert list(md.isbn or {}) == ([isbn] if isbn else [])
    # A stream is read from its start, as before, and its position is restored.
    stream = io.BytesIO(path.read_bytes())
    stream.seek(10)
    assert pdf.get_metadata(stream).title == title
    assert stream.tell() == 10


def test_password_protected_pdf_is_a_parse_error_not_ciphertext() -> None:
    """
    Check that a file needing a password raises, or falls back to its file name when asked.

    Example:
        Exercise test password protected pdf is a parse error not ciphertext through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_pdf_storage_layouts.py


    :return: None; the function records state or raises through its assertions.
    """
    path = FIXTURES / "aes256_user_password.pdf"
    with pytest.raises(pdf.PdfParseError, match="needs a password"):
        pdf.get_metadata_inplace(path)
    md = pdf.get_metadata_inplace(path, fallback_on_parse_error=True)
    assert md.title == "aes256_user_password"
    assert _authors(md) == ["Unknown Author"]
