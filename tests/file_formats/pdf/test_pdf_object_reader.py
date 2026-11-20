"""
Verify the bounded PDF object reader against poppler's readings and against hostile files.

``tests/fixtures/pdf_metadata`` holds one small PDF per storage layout, written by
``scripts/make_pdf_metadata_corpus.py``, with what poppler's ``pdfinfo`` 24.02 read
from each. The reader must agree with poppler field for field, except where it reads
more than poppler does, which the divergence tables below record. The hostile tests
build files that once made the old regex reader quadratic, or that would make an
unbounded reader exhaust time, memory or the stack, and check each finishes quickly.

Example:
    Run the tests::

        python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py
"""

from __future__ import annotations

import doctest
import hashlib
import importlib
import importlib.util
import io
import json
import shutil
import time
import zlib
from pathlib import Path
from typing import Any

import pytest

import LiuXin_alpha.utils.plugins as plugin_package
from LiuXin_alpha.file_formats.pdf.objects import (
    PdfDocument,
    PdfEncryptedError,
    PdfLimits,
    PdfReadError,
    minimal_pdf,
    read_metadata,
)
from LiuXin_alpha.file_formats.pdf.security import StandardSecurityHandler
from LiuXin_alpha.file_formats.pdf.syntax import (
    Lexer,
    NeedMore,
    PdfName,
    PdfRef,
    parse_value,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
CASES: dict[str, dict] = json.loads((FIXTURES / "cases.json").read_text(encoding="utf-8"))
FIELDS = ("Title", "Author", "Subject", "Keywords", "Creator", "Producer")
HAVE_CRYPTOGRAPHY = importlib.util.find_spec("cryptography") is not None
#: Cases encrypted with AES, which the ``aes`` plugin decrypts (or hands to poppler).
AES_CASES = frozenset(name for name in CASES if name.startswith("aes"))
#: Where LiuXin reads Info fields that poppler 24.02 does not.
INFO_DIVERGENCES = {
    # poppler does not consult /XRefStm for an object the table lists as free; qpdf and pypdf do.
    "hybrid_xref_free_entry": {"Title": "Hybrid Title", "Author": "Hybrid Author", "Producer": "Corpus Producer"},
    # Raw UTF-8 is not allowed in PDF strings, but common; poppler reads it as PDFDocEncoding.
    "utf8_without_bom": {"Title": "Título — 東京", "Author": "Zoë Writer"},
}
#: Where LiuXin reads an XMP packet that poppler 24.02 does not.
XMP_DIVERGENCES = {
    # /EncryptMetadata false leaves the packet in clear text; poppler decrypts it anyway.
    "aes128_cleartext_xmp": True,
}


def _bounded(seconds: float):
    """
    Return a context manager that fails when its body takes longer than ``seconds``.

    Example:
        Use it around a hostile read::

            with _bounded(2.0):
                read_metadata(payload)


    :param seconds: The time allowed.
    :return: The context manager.
    """

    class _Timer:
        """
        Time a block and fail when it runs too long.

        Example:
            Exercise _Timer through its owning regression module::

                python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py
        """

        def __enter__(self):
            """
            Start the clock.

            Example:
                Exercise _Timer.__enter__ through its owning regression module::

                    python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


            :return: The timer.
            """
            self.start = time.perf_counter()
            return self

        def __exit__(self, *exc_info):
            """
            Stop the clock and check it.

            Example:
                Exercise _Timer.__exit__ through its owning regression module::

                    python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


            :param exc_info: The exception, if the block raised.
            :return: False, so exceptions propagate.
            """
            elapsed = time.perf_counter() - self.start
            assert elapsed < seconds, f"took {elapsed:.2f}s, allowed {seconds}s"
            return False

    return _Timer()


@pytest.mark.parametrize("module", ["errors", "security", "syntax", "filters", "objects", "poppler"])
def test_reader_module_doctests_pass(module: str) -> None:
    """
    Run each reader module's examples.

    Example:
        Exercise test reader module doctests pass through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :param module: The module's name in ``LiuXin_alpha.file_formats.pdf``.
    :return: None; the function records state or raises through its assertions.
    """
    result = doctest.testmod(importlib.import_module(f"LiuXin_alpha.file_formats.pdf.{module}"), report=False)
    assert result.attempted > 0
    assert result.failed == 0


@pytest.mark.parametrize("name", sorted(CASES))
def test_reader_agrees_with_pdfinfo(name: str) -> None:
    """
    Read each corpus file and compare every Info field with what pdfinfo read.

    Example:
        Exercise test reader agrees with pdfinfo through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :param name: The corpus case.
    :return: None; the function records state or raises through its assertions.
    """
    case = CASES[name]
    path = FIXTURES / f"{name}.pdf"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == case["sha256"]
    expected = case["pdfinfo"]
    with path.open("rb") as stream:
        if "error" in expected:
            with pytest.raises(PdfEncryptedError, match="needs a password"):
                read_metadata(stream)
            return
        metadata = read_metadata(stream)
    wanted = INFO_DIVERGENCES.get(name) or {key: expected[key] for key in FIELDS if key in expected}
    assert {key: metadata.info[key] for key in FIELDS if key in metadata.info} == wanted
    # Without cryptography, AES files may go to poppler (the aes plugin's second layer), which reads as pdfinfo does.
    by_poppler = metadata.read_by == "poppler's pdfinfo"
    assert by_poppler == (name in AES_CASES and aes_layer() == "poppler")
    xmp_expected = expected["xmp"] if by_poppler else XMP_DIVERGENCES.get(name, expected["xmp"])
    assert bool(metadata.xmp and b"XMP Title" in metadata.xmp) == xmp_expected
    assert metadata.encrypted == expected["Encrypted"].startswith("yes")
    assert metadata.problems == ()


def test_file_and_bytes_read_the_same_and_xmp_is_whole() -> None:
    """
    Check that reading ranges from a file gives what reading bytes gives, and that a wrong /Length costs nothing.

    Example:
        Exercise test file and bytes read the same and xmp is whole through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    for name in ("object_streams", "xmp_and_info", "wrong_stream_length", "linearized"):
        data = (FIXTURES / f"{name}.pdf").read_bytes()
        with (FIXTURES / f"{name}.pdf").open("rb") as stream:
            assert read_metadata(stream) == read_metadata(data)
    packet = read_metadata((FIXTURES / "wrong_stream_length.pdf").read_bytes()).xmp
    assert packet is not None
    assert packet.startswith(b"<?xpacket begin=") and packet.endswith(b'<?xpacket end="w"?>')


def aes_layer() -> str | None:
    """
    Name the layer of the ``aes`` plugin that serves in this process.

    Example:
        >>> aes_layer() in ("cryptography", "poppler", "pure-python", None)
        True


    :return: ``cryptography``, ``poppler`` or ``pure-python``, or ``None`` if none loads.
    """
    module, _ = plugin_package.plugins["aes"]
    return getattr(module, "SOURCE", None)


def use_aes_layer(monkeypatch: pytest.MonkeyPatch, module: object) -> None:
    """
    Make the PDF reader take AES from one layer of the plugin, or from none.

    Example:
        >>> use_aes_layer  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to replace the plugin table.
    :param module: The layer's module, or ``None``.
    :return: None.
    """
    error = None if module is not None else "no layer loads"
    monkeypatch.setattr(plugin_package, "plugins", {"aes": (module, error)})


def layer_modules() -> list[Any]:
    """
    List the layers this machine can run, for parametrizing.

    Example:
        >>> [param.id for param in layer_modules()][-1]
        'pure-python'


    :return: ``pytest.param`` entries, skipped where a layer cannot run here.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import aes as pure
    from LiuXin_alpha.utils.plugins.fallbacks.fallback_beta import aes as beta

    alpha: Any = None
    if HAVE_CRYPTOGRAPHY:
        from LiuXin_alpha.utils.plugins.fallbacks.fallback_alpha import aes as alpha
    return [
        pytest.param(alpha, id="cryptography", marks=pytest.mark.skipif(alpha is None, reason="no cryptography")),
        pytest.param(beta, id="poppler", marks=pytest.mark.skipif(shutil.which("pdfinfo") is None, reason="no poppler")),
        pytest.param(pure, id="pure-python"),
    ]


@pytest.mark.parametrize("layer", layer_modules())
def test_every_aes_layer_reads_the_aes_corpus(monkeypatch: pytest.MonkeyPatch, layer: Any) -> None:
    """
    Read every AES file with each layer of the ``aes`` plugin, as pdfinfo reads it.

    Example:
        >>> test_every_aes_layer_reads_the_aes_corpus  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to choose the layer.
    :param layer: The layer's module.
    :return: None; the assertions are the check.
    """
    use_aes_layer(monkeypatch, layer)
    for name in sorted(AES_CASES):
        expected = CASES[name]["pdfinfo"]
        data = (FIXTURES / f"{name}.pdf").read_bytes()
        if "error" in expected:
            with pytest.raises(PdfEncryptedError, match="needs a password"):
                read_metadata(data)
            continue
        with _bounded(10.0):
            metadata = read_metadata(data)
        assert {key: metadata.info[key] for key in FIELDS if key in metadata.info} == {
            key: expected[key] for key in FIELDS if key in expected
        }
        assert metadata.encrypted
        assert metadata.read_by == ("poppler's pdfinfo" if layer.SOURCE == "poppler" else "LiuXin's object reader")


def test_with_no_aes_layer_aes_files_are_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Refuse AES files, rather than return ciphertext, when no layer of the plugin loads; RC4 still reads.

    Example:
        >>> test_with_no_aes_layer_aes_files_are_refused  # doctest: +SKIP


    :param monkeypatch: Pytest fixture used to empty the plugin.
    :return: None; the assertions are the check.
    """
    use_aes_layer(monkeypatch, None)
    for name in ("aes128_no_password", "aes256_no_password", "aes256_object_streams"):
        with pytest.raises(PdfEncryptedError, match="no AES implementation loads"):
            read_metadata((FIXTURES / f"{name}.pdf").read_bytes())
    for name in ("rc4_40_no_password", "rc4_128_no_password"):
        assert read_metadata((FIXTURES / f"{name}.pdf").read_bytes()).info["Title"] == "Corpus Title — café"


def _encrypted_pdf(*, xmp: bytes = b"<x:xmpmeta/>", title: bytes = b"Secret Title") -> bytes:
    """
    Build an RC4 (revision 2) file by hand, with an encrypted title and an encrypted XMP stream.

    Example:
        Exercise encrypted pdf through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :param xmp: The XMP packet to encrypt into object 4.
    :param title: The title to encrypt into the Info dictionary, object 2.
    :return: The PDF.
    """
    handler = StandardSecurityHandler.for_test_vector()
    encrypt = b"<< /Filter /Standard /V 1 /R 2 /P -4 /O <%s> /U <%s> >>" % (
        handler.owner.hex().encode(),
        handler.user.hex().encode(),
    )
    data = handler.encrypt_stream(xmp, 4, 0)
    stream = b"<< /Type /Metadata /Length %d >>\nstream\n" % len(data) + data + b"\nendstream"
    file_id = handler.file_id.hex().encode()
    return minimal_pdf(
        b"<< /Title <%s> >>" % handler.encrypt_string(title, 2, 0).hex().encode(),
        extra={3: encrypt, 4: stream},
        catalog=b"<< /Type /Catalog /Metadata 4 0 R >>",
        trailer=b"/Encrypt 3 0 R /ID [<%s> <%s>] " % (file_id, file_id),
    )


def test_rc4_strings_and_streams_decrypt_within_limits() -> None:
    """
    Check RC4 decryption of strings and streams, and the limit on pure-Python RC4.

    Example:
        Exercise test rc4 strings and streams decrypt within limits through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = read_metadata(_encrypted_pdf())
    assert metadata.info == {"Title": "Secret Title"}
    assert metadata.xmp == b"<x:xmpmeta/>"
    assert metadata.encrypted
    big = _encrypted_pdf(xmp=b"x" * 5000)
    limited = read_metadata(big, limits=PdfLimits(rc4_stream_bytes=4096))
    assert limited.info == {"Title": "Secret Title"}
    assert limited.xmp is None
    assert limited.problems == ("XMP packet: an RC4-encrypted stream is larger than the reading limit",)


def test_lexer_tokens_escapes_and_incomplete_windows() -> None:
    """
    Check the lexer's escapes, comments and names, and that incomplete windows ask for more.

    Example:
        Exercise test lexer tokens escapes and incomplete windows through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    lexer = Lexer(b" %comment\r\n" + rb"""(line\n tab\t paren\( nested(inner) octal\101\7 cont\
done) /A#20Name#ZZ <4142F> <not-hex> {""")
    assert lexer.token() == ("string", b"line\n tab\t paren( nested(inner) octalA\x07 contdone")
    assert lexer.token() == ("name", "A Name#ZZ")
    assert lexer.token() == ("string", b"AB\xf0")
    assert lexer.token() == ("string", b"\xe0")
    assert lexer.token() == ("keyword", b"{")
    assert lexer.token() == ("eof", None)
    assert Lexer(rb"(dangling\)").token() == ("string", b"dangling)")
    for window in (b"(unterminated", b"<41", b"/Name", b"123", b"<", b"(ends with\\"):
        with pytest.raises(NeedMore):
            Lexer(window, complete=False).token()
    lexer = Lexer(b"<< /K 1 0 R /L [1 0] /M 2 0 >>")
    assert parse_value(lexer, *lexer.token(), 0, 64) == {"K": PdfRef(1, 0), "L": [1, 0], "M": 2}
    lexer = Lexer(b"<< (not a key) /K /V >>")
    assert parse_value(lexer, *lexer.token(), 0, 64) == {"K": PdfName("V")}
    for broken in (b"<< /K 1", b"[1 2", b"endobj", b"]"):
        lexer = Lexer(broken)
        with pytest.raises(PdfReadError):
            parse_value(lexer, *lexer.token(), 0, 64)


# --- hostile files --------------------------------------------------------------------------------


def test_unterminated_objects_scan_in_linear_time() -> None:
    """
    Check the input that made the old ``N G obj .*? endobj`` regex quadratic.

    200,000 unterminated objects in 1.8 MB: the old reader needed about 4 s for 72 KB
    and quadrupled with each doubling.

    Example:
        Exercise test unterminated objects scan in linear time through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    payload = b"%PDF-1.4\n" + b"1 0 obj (" * 200_000
    with _bounded(5.0):
        assert read_metadata(payload).info == {}
    nested = b"%PDF-1.4\n1 0 obj << /Title " + b"[" * 200_000 + b"\ntrailer << /Info 1 0 R >>\n"
    with _bounded(5.0):
        assert read_metadata(nested).problems == ("Info dictionary: objects nest too deeply",)


def test_oversized_and_deeply_nested_info_is_refused() -> None:
    """
    Check that an unterminated string stops at the object limit and deep nesting at the depth limit.

    Example:
        Exercise test oversized and deeply nested info is refused through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    with _bounded(5.0):
        metadata = read_metadata(minimal_pdf(b"<< /Title (" + b"(" * (2 << 20) + b" >>"))
    assert metadata.info == {}
    assert metadata.problems == ("Info dictionary: an object is larger than the reading limit",)
    with _bounded(5.0):
        metadata = read_metadata(minimal_pdf(b"<< /Title " + b"[" * 100_000 + b" >>"))
    assert metadata.problems == ("Info dictionary: objects nest too deeply",)
    padded = minimal_pdf(b"<< /Title (T) >>").replace(b"2 0 obj\n", b"2 0 obj\n" + b" " * (3 << 20))
    with _bounded(5.0):
        metadata = read_metadata(padded)
    assert metadata.info == {}
    assert metadata.problems == ("Info dictionary: an object is larger than the reading limit",)


def test_flate_bombs_cost_only_their_stream() -> None:
    """
    Check that a metadata stream inflating past the limit costs the XMP packet, not the Info dictionary.

    Example:
        Exercise test flate bombs cost only their stream through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    compressor = zlib.compressobj(9)
    zeros = bytes(1 << 20)
    bomb = b"".join(compressor.compress(zeros) for _ in range(64)) + compressor.flush()
    stream = b"<< /Type /Metadata /Filter /FlateDecode /Length %d >>\nstream\n" % len(bomb) + bomb + b"\nendstream"
    payload = minimal_pdf(b"<< /Title (T) >>", extra={3: stream}, catalog=b"<< /Type /Catalog /Metadata 3 0 R >>")
    with _bounded(5.0):
        metadata = read_metadata(payload)
    assert metadata.info == {"Title": "T"}
    assert metadata.problems == ("XMP packet: a stream decodes to more than the reading limit",)
    small = read_metadata(payload, limits=PdfLimits(decoded_bytes=1 << 40, total_decoded_bytes=1 << 20))
    assert small.problems == ("XMP packet: a stream decodes to more than the reading limit",)


def test_loops_and_impossible_counts_terminate() -> None:
    """
    Check /Prev loops, reference loops, impossible table counts and object stream headers.

    Example:
        Exercise test loops and impossible counts terminate through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    plain = minimal_pdf(b"<< /Title (T) >>")
    table = plain.index(b"xref\n0")
    prev_loop = plain.replace(b"/Info 2 0 R ", b"/Info 2 0 R /Prev %d " % table)
    with _bounded(2.0):
        assert read_metadata(prev_loop).info == {"Title": "T"}
    huge_count = plain.replace(b"xref\n0 3\n", b"xref\n0 4000000000\n")
    with _bounded(2.0):
        assert read_metadata(huge_count).info == {"Title": "T"}
    past_end = plain.replace(b"startxref\n%d" % table, b"startxref\n999999999")
    with _bounded(2.0):
        assert read_metadata(past_end).info == {"Title": "T"}
    references = minimal_pdf(b"3 0 R", extra={3: b"2 0 R"})
    with _bounded(2.0):
        assert read_metadata(references).problems == (
            "Info dictionary: a chain of references is longer than the reading limit",
        )
    member = b"<< /Title (Compressed) >>"
    header = b"5 0 "
    objstm = (
        b"<< /Type /ObjStm /N 1000000000 /First %d /Length %d >>\nstream\n" % (len(header), len(header + member))
        + header
        + member
        + b"\nendstream"
    )
    # Object 5 is only in the object stream; the scan finds it there because the table does not list it.
    compressed = minimal_pdf(b"<< >>", extra={4: objstm}).replace(b"/Info 2 0 R", b"/Info 5 0 R")
    with _bounded(2.0):
        assert read_metadata(compressed).info == {"Title": "Compressed"}


def test_broken_cross_reference_streams_fall_back_to_a_scan() -> None:
    """
    Check that a malformed cross-reference stream sends the reader to a scan that finds object streams.

    Example:
        Exercise test broken cross reference streams fall back to a scan through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    data = (FIXTURES / "object_streams.pdf").read_bytes()
    assert data.count(b"/W [ 1 2 1 ]") == 1
    broken = data.replace(b"/W [ 1 2 1 ]", b"/W [ 9 2 1 ]")
    metadata = read_metadata(broken)
    assert metadata.info["Title"] == "Corpus Title — café"
    document = PdfDocument(broken)
    assert document.sections == []


def test_encryption_that_cannot_be_opened_is_refused() -> None:
    """
    Check foreign handlers, malformed dictionaries and files that look encrypted without a trailer.

    Example:
        Exercise test encryption that cannot be opened is refused through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    for encrypt, message in (
        (b"<< /Filter /Adobe.PubSec /V 4 >>", "Adobe.PubSec"),
        (b"<< /Filter /Standard /V 5 /R 6 /O <00> /U <00> >>", "malformed"),
        (b"<< /Filter /Standard /V 4 /R 4 /CF << >> /StmF /Missing /O <00> /U <00> >>", "does not define"),
        (b"<< /Filter /Standard /V 3 /R 3 >>", "version 3"),
    ):
        payload = minimal_pdf(b"<< /Title (x) >>", extra={3: encrypt}, trailer=b"/Encrypt 3 0 R ")
        with pytest.raises(PdfEncryptedError, match=message):
            read_metadata(payload)
    trailerless = b"%PDF-1.4\n1 0 obj << /Type /Catalog >> endobj\n2 0 obj << /Title (x) >> endobj\n3 0 obj << /Encrypt 4 0 R >> endobj\n"
    with pytest.raises(PdfEncryptedError, match="trailer is missing"):
        read_metadata(trailerless)


def test_not_a_pdf_and_empty_files() -> None:
    """
    Check files without a header, and with a header but nothing else.

    Example:
        Exercise test not a pdf and empty files through its owning regression module::

            python -m pytest -q tests/file_formats/pdf/test_pdf_object_reader.py


    :return: None; the function records state or raises through its assertions.
    """
    for payload in (b"", b"plain text", b"\0" * 2048 + b"%PDF-1.4"):
        with pytest.raises(PdfReadError, match="no %PDF- header"):
            read_metadata(payload)
    with pytest.raises(PdfReadError, match="no objects"):
        read_metadata(b"%PDF-1.7\n%%EOF\n")
    with pytest.raises(PdfReadError, match="no objects"):
        read_metadata(io.BytesIO(b"%PDF-1.7\nstartxref\n0\n%%EOF\n"))
