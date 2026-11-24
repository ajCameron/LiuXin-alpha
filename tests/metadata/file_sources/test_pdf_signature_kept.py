"""
Check that writing metadata into a signed PDF leaves its digital signature valid.

LiuXin's PDF writer appends an incremental update, so the bytes a signature covers
are not touched. This signs a PDF as signing tools do (``adbe.pkcs7.detached``,
with a self-signed certificate), writes new metadata into it, and checks the
signature still verifies: with the signer's public key, and with poppler's
``pdfsig`` where it is installed.

Example:
    Run the checks::

        python -m pytest -q tests/metadata/file_sources/test_pdf_signature_kept.py
"""

from __future__ import annotations

import datetime
import io
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

pypdf = pytest.importorskip("pypdf")
pytest.importorskip("cryptography")

from cryptography import (
    x509,  # noqa: E402 - only once cryptography is known to be there
)
from cryptography.exceptions import InvalidSignature  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import padding, rsa  # noqa: E402
from cryptography.hazmat.primitives.serialization import pkcs7  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402

from LiuXin_alpha.metadata.book.base import calibreMetadata  # noqa: E402
from LiuXin_alpha.metadata.file_sources import pdf  # noqa: E402

SIGNATURE_BYTES = 4096
_PLACEHOLDER = 1_000_000_000


def signer() -> tuple[Any, x509.Certificate]:
    """
    Make an RSA key and a self-signed certificate for it.

    Example:
        >>> key, certificate = signer()
        >>> key.key_size
        2048


    :return: The private key and the certificate.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "LiuXin test signer")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def unsigned_pdf() -> bytes:
    """
    Write a one-page PDF with a signature field whose signature is a placeholder.

    Example:
        >>> b"/adbe.pkcs7.detached" in unsigned_pdf()
        True


    :return: The PDF's bytes.
    """
    from pypdf.generic import (
        ArrayObject,
        ByteStringObject,
        DictionaryObject,
        NameObject,
        NumberObject,
        TextStringObject,
    )

    writer = pypdf.PdfWriter()
    page = writer.add_blank_page(612, 792)
    writer.add_metadata({"/Title": "Signed"})
    signature = writer._add_object(  # noqa: SLF001 - pypdf's way to add an indirect object
        DictionaryObject(
            {
                NameObject("/Type"): NameObject("/Sig"),
                NameObject("/Filter"): NameObject("/Adobe.PPKLite"),
                NameObject("/SubFilter"): NameObject("/adbe.pkcs7.detached"),
                NameObject("/ByteRange"): ArrayObject([NumberObject(0)] + [NumberObject(_PLACEHOLDER)] * 3),
                NameObject("/Contents"): ByteStringObject(b"\0" * SIGNATURE_BYTES),
            }
        )
    )
    field = writer._add_object(  # noqa: SLF001
        DictionaryObject(
            {
                NameObject("/Type"): NameObject("/Annot"),
                NameObject("/Subtype"): NameObject("/Widget"),
                NameObject("/FT"): NameObject("/Sig"),
                NameObject("/T"): TextStringObject("Signature1"),
                NameObject("/Rect"): ArrayObject([NumberObject(0)] * 4),
                NameObject("/F"): NumberObject(132),
                NameObject("/P"): page.indirect_reference,
                NameObject("/V"): signature,
            }
        )
    )
    page[NameObject("/Annots")] = ArrayObject([field])
    writer._root_object[NameObject("/AcroForm")] = DictionaryObject(  # noqa: SLF001
        {NameObject("/Fields"): ArrayObject([field]), NameObject("/SigFlags"): NumberObject(3)}
    )
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def sign(data: bytes, key: Any, certificate: x509.Certificate) -> bytes:
    """
    Fill in a placeholder signature: the byte range, then a detached CMS signature over it.

    The CMS has no signed attributes, so its last 256 bytes are the RSA signature of
    the covered bytes, which :func:`verify` checks.

    Example:
        Sign a placeholder PDF::

            signed = sign(unsigned_pdf(), *signer())


    :param data: The PDF with a placeholder.
    :param key: The signer's private key.
    :param certificate: The signer's certificate.
    :return: The signed PDF.
    """
    hex_start = data.index(b"<" + b"00" * SIGNATURE_BYTES + b">")
    hex_end = hex_start + 2 + 2 * SIGNATURE_BYTES
    placeholder = re.search(rb"/ByteRange\s*\[([^\]]*)\]", data)
    assert placeholder is not None
    ranges = b"0 %d %d %d" % (hex_start, hex_end, len(data) - hex_end)
    width = placeholder.end(1) - placeholder.start(1)
    data = data[: placeholder.start(1)] + ranges.ljust(width) + data[placeholder.end(1) :]
    covered = data[:hex_start] + data[hex_end:]
    cms = (
        pkcs7.PKCS7SignatureBuilder()
        .set_data(covered)
        .add_signer(certificate, key, hashes.SHA256())
        .sign(
            serialization.Encoding.DER,
            # Binary: sign the bytes as they are, not as S/MIME text with CRLF line ends.
            [pkcs7.PKCS7Options.DetachedSignature, pkcs7.PKCS7Options.NoAttributes, pkcs7.PKCS7Options.Binary],
        )
    )
    contents = cms.hex().encode().ljust(2 * SIGNATURE_BYTES, b"0")
    return data[: hex_start + 1] + contents + data[hex_end - 1 :]


def verify(data: bytes, certificate: x509.Certificate) -> None:
    """
    Check the first signature in a PDF against its byte range, with the signer's key.

    Example:
        Check a signed PDF::

            verify(signed, certificate)


    :param data: The PDF.
    :param certificate: The signer's certificate.
    :return: None.
    :raises InvalidSignature: The covered bytes are not what was signed.
    """
    found = re.search(rb"/ByteRange\s*\[\s*0\s+(\d+)\s+(\d+)\s+(\d+)\s*\]", data)
    assert found is not None
    start, end, rest = (int(value) for value in found.groups())
    raw = bytes.fromhex(data[start + 1 : end - 1].decode())
    # The DER is padded with zeros: its length is in its header (0x30 0x82 and two bytes).
    cms = raw[: 4 + int.from_bytes(raw[2:4], "big")] if raw[1] == 0x82 else raw
    covered = data[:start] + data[end : end + rest]
    certificate.public_key().verify(cms[-256:], covered, padding.PKCS1v15(), hashes.SHA256())


def test_writing_metadata_keeps_the_signature_valid(tmp_path: Path) -> None:
    """
    Write new metadata into a signed PDF, and check the signed revision is untouched and still verifies.

    Example:
        >>> test_writing_metadata_keeps_the_signature_valid(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Pytest-managed temporary directory.
    :return: None; the assertions are the check.
    """
    key, certificate = signer()
    signed = sign(unsigned_pdf(), key, certificate)
    verify(signed, certificate)

    record = calibreMetadata("A new title", ["Ann Lee"])
    record.comments = "Written after signing."
    stream = io.BytesIO(signed)
    report = pdf.set_metadata(stream, record)
    written = stream.getvalue()

    assert not report.rewritten
    assert written.startswith(signed) and len(written) > len(signed)
    verify(written, certificate)
    assert pdf.get_metadata(io.BytesIO(written)).title == "A new title"

    tampered = bytearray(written)
    tampered[20] ^= 1
    with pytest.raises(InvalidSignature):
        verify(bytes(tampered), certificate)

    pdfsig = shutil.which("pdfsig")
    if pdfsig:
        path = tmp_path / "signed.pdf"
        path.write_bytes(written)
        result = subprocess.run([pdfsig, "-nocert", str(path)], capture_output=True, text=True, timeout=60)
        assert "Signature is Valid" in result.stdout, result.stdout + result.stderr
