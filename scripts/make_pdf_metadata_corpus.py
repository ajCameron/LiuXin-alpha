"""
Write the PDF metadata corpus in ``tests/fixtures/pdf_metadata``.

Each case is a small PDF that stores the same kind of metadata in a different
way: in a plain Info dictionary, in an object stream, after an incremental
update, encrypted (RC4 and AES, with and without a user password), as UTF-16 or
PDFDocEncoding strings, in XMP alone, behind a broken cross-reference table, and
on a page far larger than paper. ``cases.json`` records how each was made and
what poppler's ``pdfinfo`` reads from it, so the readers can be compared
without either tool.

The script needs pypdf and qpdf, which tests do not: the PDFs are committed.
Re-running it changes the files (PDF IDs and dates differ), so run it only to
add or change cases, and commit what it writes.

Example:
    Write the corpus::

        python scripts/make_pdf_metadata_corpus.py
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "fixtures" / "pdf_metadata"

INFO = {
    "/Title": "Corpus Title — café",
    "/Author": "Ann Author & Bob Writer",
    "/Subject": "Corpus subject",
    "/Keywords": "alpha, beta, 9780306406157",
    "/Creator": "Corpus Creator",
    "/Producer": "Corpus Producer",
}

XMP = """<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
 <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/"
     xmlns:xmp="http://ns.adobe.com/xap/1.0/" xmlns:prism="http://prismstandard.org/namespaces/basic/2.0/">
   <dc:title><rdf:Alt><rdf:li xml:lang="x-default">XMP Title — 東京</rdf:li></rdf:Alt></dc:title>
   <dc:creator><rdf:Seq><rdf:li>Carol XMP</rdf:li><rdf:li>Дмитрий XMP</rdf:li></rdf:Seq></dc:creator>
   <dc:subject><rdf:Bag><rdf:li>xmp-tag</rdf:li><rdf:li>タグ</rdf:li></rdf:Bag></dc:subject>
   <dc:description><rdf:Alt><rdf:li xml:lang="x-default">XMP description</rdf:li></rdf:Alt></dc:description>
   <dc:publisher><rdf:Bag><rdf:li>XMP Publisher</rdf:li></rdf:Bag></dc:publisher>
   <prism:doi>10.4242/corpus</prism:doi>
   <xmp:CreateDate>2019-06-01T12:00:00Z</xmp:CreateDate>
  </rdf:Description>
 </rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>"""


def writer(info: dict[str, str] | None = INFO, *, width: float = 300, height: float = 400, xmp: str | None = None) -> bytes:
    """
    Write a one-page PDF with pypdf, with an Info dictionary and, optionally, XMP.

    Example:
        >>> writer()[:5]  # doctest: +SKIP
        b'%PDF-'


    :param info: The Info dictionary, or ``None`` for none.
    :param width: The page's width in points.
    :param height: The page's height in points.
    :param xmp: An XMP packet to store as the catalog's metadata stream.
    :return: The PDF's bytes.
    """
    from pypdf import PdfWriter
    from pypdf.generic import DecodedStreamObject, NameObject

    pdf = PdfWriter()
    pdf.add_blank_page(width=width, height=height)
    if info is None:
        pdf.metadata = None
    else:
        pdf.add_metadata(info)
    if xmp is not None:
        stream = DecodedStreamObject()
        stream.set_data(xmp.encode("utf-8"))
        stream.update({NameObject("/Type"): NameObject("/Metadata"), NameObject("/Subtype"): NameObject("/XML")})
        pdf._root_object[NameObject("/Metadata")] = pdf._add_object(stream)
    out = io.BytesIO()
    pdf.write(out)
    return out.getvalue()


def qpdf(raw: bytes, *args: str) -> bytes:
    """
    Run qpdf on a PDF and give its output.

    Example:
        >>> qpdf(writer(), "--object-streams=generate")[:5]  # doctest: +SKIP
        b'%PDF-'


    :param raw: The PDF.
    :param args: qpdf's options.
    :return: The rewritten PDF.
    """
    with tempfile.TemporaryDirectory() as folder:
        source, target = Path(folder, "in.pdf"), Path(folder, "out.pdf")
        source.write_bytes(raw)
        # qpdf's options and the output go before "--" ends an --encrypt group, so the files come last.
        result = subprocess.run(["qpdf", str(source), *args, str(target)], capture_output=True, check=False)
        if result.returncode not in (0, 3):  # 3: written, with warnings
            raise SystemExit(f"qpdf {' '.join(args)} failed: {result.stderr.decode(errors='replace')}")
        return target.read_bytes()


def incremental(raw: bytes) -> bytes:
    """
    Append an incremental update that changes the title, as an editor saving in place does.

    Example:
        >>> incremental(writer())[:5]  # doctest: +SKIP
        b'%PDF-'


    :param raw: The original PDF.
    :return: The original bytes followed by the update.
    """
    from pypdf import PdfWriter

    pdf = PdfWriter(io.BytesIO(raw), incremental=True)
    pdf.add_metadata({"/Title": "Updated Title"})
    out = io.BytesIO()
    pdf.write(out)
    return out.getvalue()


def broken_xref(raw: bytes) -> bytes:
    """
    Point ``startxref`` at nothing, so a reader must rebuild the table from the objects.

    Example:
        >>> broken_xref(writer())[-30:]  # doctest: +SKIP


    :param raw: The PDF.
    :return: The PDF with a wrong ``startxref``.
    """
    head, _, tail = raw.rpartition(b"startxref")
    return head + b"startxref\n12345\n%%EOF\n"


def handmade(objects: dict[int, bytes], trailer: bytes, *, omit: frozenset[int] = frozenset(), version: str = "1.4") -> bytes:
    """
    Write a PDF by hand, with a classic cross-reference table, for layouts pypdf and qpdf do not make.

    Example:
        >>> handmade({1: b"<< /Type /Catalog >>"}, b"/Root 1 0 R")[:8]
        b'%PDF-1.4'


    :param objects: The objects' bodies, by object number, written in that order.
    :param trailer: The trailer dictionary's entries, after ``/Size``.
    :param omit: Object numbers to leave out of the table though they are written, as a
        hybrid-reference file leaves out what only its cross-reference stream lists.
    :param version: The header's PDF version.
    :return: The PDF's bytes.
    """
    out = bytearray(f"%PDF-{version}\n%\xe2\xe3\xcf\xd3\n".encode("latin-1"))
    offsets = {}
    for number, body in objects.items():
        offsets[number] = len(out)
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    size = max(objects) + 1
    table = len(out)
    out += b"xref\n"
    listed = [number for number in range(size) if number not in omit]
    runs: list[list[int]] = []
    for number in listed:
        if runs and runs[-1][-1] == number - 1:
            runs[-1].append(number)
        else:
            runs.append([number])
    for run in runs:
        out += f"{run[0]} {len(run)}\n".encode()
        for number in run:
            if number in offsets:
                out += f"{offsets[number]:010d} 00000 n\r\n".encode()
            else:
                out += b"0000000000 65535 f\r\n"
    out += f"trailer\n<< /Size {size} ".encode() + trailer + f" >>\nstartxref\n{table}\n%%EOF\n".encode()
    return bytes(out)


PAGES = {
    1: b"<< /Type /Catalog /Pages 2 0 R >>",
    2: b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    3: b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 400] >>",
}


def hybrid(*, listed_free: bool = False) -> bytes:
    """
    Write a hybrid-reference file, as Word's "Save as PDF" does: a classic table plus ``/XRefStm``.

    The Info dictionary is in an object stream that only the cross-reference stream lists; the
    table leaves it out, as the specification's own example does.

    Example:
        >>> b"/XRefStm" in hybrid()  # doctest: +SKIP
        True


    :param listed_free: List the compressed object as free in the table instead of leaving it out.
    :return: The PDF's bytes.
    """
    info = b"<< /Title (Hybrid Title) /Author (Hybrid Author) /Producer (Corpus Producer) >>"
    header = b"5 0\n"
    objstm = b"<< /Type /ObjStm /N 1 /First %d /Length %d >>\nstream\n" % (len(header), len(header + info))
    objstm += header + info + b"\nendstream"
    xref_data = bytes([2, 0, 4, 0])
    xref = b"<< /Type /XRef /Size 7 /Index [5 1] /W [1 2 1] /Length 4 >>\nstream\n" + xref_data + b"\nendstream"
    objects = {**PAGES, 4: objstm, 6: xref}
    omit = frozenset() if listed_free else frozenset({5})
    raw = handmade(objects, b"/Root 1 0 R /Info 5 0 R /XRefStm XREFSTM", omit=omit, version="1.5")
    offset = raw.index(b"6 0 obj")
    return raw.replace(b"/XRefStm XREFSTM", b"/XRefStm %d" % offset)


def indirect_values() -> bytes:
    """
    Write an Info dictionary whose title is an indirect reference to a string object.

    Example:
        >>> b"/Title 5 0 R" in indirect_values()
        True


    :return: The PDF's bytes.
    """
    objects = {**PAGES, 4: b"<< /Title 5 0 R /Author (Indirect Author) >>", 5: b"(Indirect Title)"}
    return handmade(objects, b"/Root 1 0 R /Info 4 0 R")


def utf8_without_bom() -> bytes:
    """
    Write Info strings as raw UTF-8, which the PDF specification does not allow but many tools produce.

    Example:
        >>> "東京".encode() in utf8_without_bom()
        True


    :return: The PDF's bytes.
    """
    title = "Título — 東京".encode()
    objects = {**PAGES, 4: b"<< /Title (" + title + b") /Author (Zo\xc3\xab Writer) >>"}
    return handmade(objects, b"/Root 1 0 R /Info 4 0 R")


def wrong_stream_length() -> bytes:
    """
    Write an XMP stream whose ``/Length`` is too short, so a reader must find ``endstream`` itself.

    Example:
        >>> b"/Length 5" in wrong_stream_length()
        True


    :return: The PDF's bytes.
    """
    xmp = XMP.encode()
    metadata = b"<< /Type /Metadata /Subtype /XML /Length 5 >>\nstream\n" + xmp + b"\nendstream"
    catalog = b"<< /Type /Catalog /Pages 2 0 R /Metadata 4 0 R >>"
    return handmade({**PAGES, 1: catalog, 4: metadata}, b"/Root 1 0 R")


def pdfinfo(path: Path) -> dict[str, str]:
    """
    Read what poppler's pdfinfo reads, as calibre runs it.

    Example:
        >>> pdfinfo(OUT / "plain.pdf")["Title"]  # doctest: +SKIP
        'Corpus Title — café'


    :param path: The PDF.
    :return: Its fields, or ``{"error": ...}`` when pdfinfo fails.
    """
    result = subprocess.run(["pdfinfo", "-enc", "UTF-8", "-isodates", str(path)], capture_output=True, check=False)
    if result.returncode:
        return {"error": result.stderr.decode("utf-8", "replace").strip()}
    fields = {}
    for line in result.stdout.decode("utf-8").splitlines():
        name, _, value = line.partition(":")
        if name in {"Title", "Author", "Subject", "Keywords", "Creator", "Producer", "Encrypted", "Page size"}:
            fields[name] = value.strip()
    meta = subprocess.run(["pdfinfo", "-meta", str(path)], capture_output=True, check=False).stdout
    fields["xmp"] = b"XMP Title" in meta
    return fields


CASES: dict[str, tuple[str, Callable[[], bytes]]] = {
    "plain": ("Info dictionary as a plain object, classic xref table", lambda: writer()),
    "object_streams": (
        "Info dictionary inside a compressed object stream, with an xref stream (PDF 1.5)",
        lambda: qpdf(writer(), "--object-streams=generate"),
    ),
    "incremental_update": (
        "An incremental update after the original changes the title; the newer Info wins",
        lambda: incremental(writer()),
    ),
    "incremental_object_streams": (
        "An incremental update to a PDF with object streams",
        lambda: incremental(qpdf(writer(), "--object-streams=generate")),
    ),
    "linearized": ("Linearized with object streams", lambda: qpdf(writer(), "--linearize", "--object-streams=generate")),
    "aes256_no_password": (
        "AES-256 (R6) with an empty user password: anyone may open it",
        lambda: qpdf(writer(), "--encrypt", "", "owner", "256", "--"),
    ),
    "aes128_no_password": (
        "AES-128 (R4) with an empty user password",
        lambda: qpdf(writer(), "--encrypt", "", "owner", "128", "--use-aes=y", "--"),
    ),
    "rc4_128_no_password": (
        "RC4 128-bit (R3) with an empty user password",
        lambda: qpdf(writer(), "--allow-weak-crypto", "--encrypt", "", "owner", "128", "--use-aes=n", "--"),
    ),
    "rc4_40_no_password": (
        "RC4 40-bit (R2) with an empty user password",
        lambda: qpdf(writer(), "--allow-weak-crypto", "--encrypt", "", "owner", "40", "--"),
    ),
    "aes256_user_password": (
        "AES-256 with a user password: the metadata cannot be read without it",
        lambda: qpdf(writer(), "--encrypt", "secret", "owner", "256", "--"),
    ),
    "aes256_object_streams": (
        "AES-256 with object streams: the Info dictionary is inside an encrypted stream",
        lambda: qpdf(writer(), "--object-streams=generate", "--encrypt", "", "owner", "256", "--"),
    ),
    "utf16_strings": (
        "Info strings that PDFDocEncoding cannot hold, stored as UTF-16BE",
        lambda: writer({**INFO, "/Title": "書名 — Книга", "/Author": "金庸"}),
    ),
    "xmp_only": ("XMP metadata and no Info dictionary", lambda: writer(None, xmp=XMP)),
    "xmp_and_info": ("Both XMP and an Info dictionary", lambda: writer(xmp=XMP)),
    "broken_xref": ("startxref points at nothing; readers must rebuild", lambda: broken_xref(writer())),
    "huge_page": ("A 200 by 200 inch page, which calibre renders at 150 dpi", lambda: writer(width=14400, height=14400)),
    "hybrid_xref": ("A classic table plus /XRefStm, with the Info dictionary in an object stream", hybrid),
    "hybrid_xref_free_entry": (
        "A hybrid file whose table lists the compressed Info object as free; poppler 24.02 does not then "
        "consult /XRefStm, qpdf and pypdf do",
        lambda: hybrid(listed_free=True),
    ),
    "indirect_info_values": ("An Info value that is an indirect reference to a string object", indirect_values),
    "utf8_without_bom": ("Info strings in raw UTF-8 without a byte order mark (not allowed, but common)", utf8_without_bom),
    "leading_junk": (
        "300 bytes before %PDF-, as a MacBinary header leaves: every xref offset is off by 300",
        lambda: b"\0" * 300 + writer(),
    ),
    "trailing_junk": ("4 KiB after %%EOF", lambda: writer() + b"\0" * 4096),
    "wrong_stream_length": ("The XMP stream's /Length is too short", wrong_stream_length),
    "aes128_cleartext_xmp": (
        "AES-128 with an empty user password and /EncryptMetadata false: the XMP stream is plain",
        lambda: qpdf(writer(xmp=XMP), "--encrypt", "", "owner", "128", "--use-aes=y", "--cleartext-metadata", "--"),
    ),
    "aes256_empty_owner": (
        "AES-256 with a user password and an empty owner password: the empty owner password opens it",
        lambda: qpdf(writer(), "--encrypt", "secret", "", "256", "--allow-insecure", "--"),
    ),
}


def main() -> int:
    """
    Write every case, its SHA-256 and pdfinfo's reading, to the corpus folder.

    Example:
        >>> main()  # doctest: +SKIP
        0


    :return: The exit status.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, (description, make) in CASES.items():
        path = OUT / f"{name}.pdf"
        data = make()
        path.write_bytes(data)
        manifest[name] = {
            "description": description,
            "sha256": hashlib.sha256(data).hexdigest(),
            "pdfinfo": pdfinfo(path),
        }
        print(name, len(data), manifest[name]["pdfinfo"].get("Title", manifest[name]["pdfinfo"].get("error")))
    (OUT / "cases.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
