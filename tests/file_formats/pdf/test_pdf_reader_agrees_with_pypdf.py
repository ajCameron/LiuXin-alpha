"""
Check LiuXin's PDF object reader against pypdf, a second implementation, on the corpus.

pypdf comes with the ``pdf`` extra; without it these checks are skipped. The corpus is
also checked against poppler's ``pdfinfo`` (in ``test_pdf_object_reader.py``); pypdf
gives a second opinion from a reader that shares no code with either.

Example:
    Run these checks::

        python -m pytest -q tests/file_formats/pdf/test_pdf_reader_agrees_with_pypdf.py
"""

from __future__ import annotations

import io
import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.pdf.errors import PdfEncryptedError
from LiuXin_alpha.file_formats.pdf.objects import read_metadata

pypdf = pytest.importorskip("pypdf")

CORPUS = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
CASES: dict[str, Any] = json.loads((CORPUS / "cases.json").read_text(encoding="utf-8"))

#: Where pypdf reads differently, and why LiuXin's reading is kept.
DIVERGES = {
    "utf8_without_bom": "pypdf decodes strings without a byte-order mark as PDFDocEncoding,"
    " so raw UTF-8 comes out garbled; LiuXin reads valid UTF-8 as UTF-8",
    "aes128_cleartext_xmp": "pypdf decrypts the XMP stream that /EncryptMetadata false"
    " leaves in clear text, garbling it, as poppler does",
}


def _text(value: Any) -> Any:
    """
    Turn a value from either reader into plain text, keeping arrays as lists.

    Example:
        >>> _text(["a", 1])
        ['a', '1']


    :param value: The value.
    :return: Text, or a list of texts.
    """
    if hasattr(value, "get_object"):
        value = value.get_object()
    if isinstance(value, list):
        return [_text(item) for item in value]
    return str(value)


def _pypdf_reading(data: bytes) -> tuple[dict[str, Any], bytes | None]:
    """
    Read the Info dictionary and XMP packet with pypdf, opening the file with an empty password.

    Example:
        Read one corpus file::

            info, xmp = _pypdf_reading((CORPUS / "plain.pdf").read_bytes())


    :param data: The file.
    :return: The Info entries, by name, and the packet's bytes.
    :raises PdfEncryptedError: If pypdf cannot open the file without a password.
    """
    with warnings.catch_warnings():
        # pypdf warns about the damage the corpus holds on purpose.
        warnings.simplefilter("ignore")
        reader = pypdf.PdfReader(io.BytesIO(data), strict=False)
        if reader.is_encrypted and not reader.decrypt(""):
            raise PdfEncryptedError("pypdf needs a password")
        info = {
            str(name).lstrip("/"): _text(value)
            for name, value in (reader.metadata or {}).items()
        }
        packet = reader.trailer["/Root"].get_object().get("/Metadata")
        return info, packet.get_object().get_data() if packet is not None else None


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_object_reader_agrees_with_pypdf(name: str) -> None:
    """
    Both readers find the same Info entries and XMP packet, or both refuse the file.

    Example:
        >>> test_the_object_reader_agrees_with_pypdf("plain")


    :param name: The corpus case.
    :return: None after the readings are compared.
    """
    data = (CORPUS / f"{name}.pdf").read_bytes()
    try:
        theirs = _pypdf_reading(data)
    except pypdf.errors.DependencyError as error:
        pytest.skip(f"pypdf cannot decrypt here: {error}")
    except PdfEncryptedError:
        with pytest.raises(PdfEncryptedError):
            read_metadata(data)
        return
    ours = read_metadata(data)
    agrees = (
        {key: _text(value) for key, value in ours.info.items()},
        ours.xmp,
    ) == theirs
    assert agrees != (name in DIVERGES), DIVERGES.get(
        name, f"{name} reads differently in pypdf"
    )


def test_every_divergence_is_a_corpus_case() -> None:
    """
    The divergences listed name real cases, so a renamed case cannot hide one.

    Example:
        >>> test_every_divergence_is_a_corpus_case()


    :return: None after the names are checked.
    """
    assert set(DIVERGES) <= set(CASES)
