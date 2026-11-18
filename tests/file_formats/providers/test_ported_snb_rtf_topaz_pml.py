"""
Check the SNB, RTF, Topaz and PML readers ported from upstream calibre.

The samples are written here. SNB and RTF samples are made with the ported
modules' own writers, which are calibre's; Topaz and PML samples are built
byte by byte, following the layout calibre's readers parse. Two upstream
readers keep reading at the end of a damaged file, for minutes or forever;
the port stops them, and the tests here would fail rather than hang if that
regressed.
"""

from __future__ import annotations

import ast
import doctest
import io
import logging
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import _compat, pml, rtf, snbfile, topaz

JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 60 + b"\xff\xd9"
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 60


class EndOfFileGuard(io.BytesIO):
    """
    A stream that fails a test which keeps reading after its end.

    Example:
        >>> guard = EndOfFileGuard(b"ab")
        >>> guard.read(), guard.read()
        (b'ab', b'')
    """

    allowed_empty_reads = 64

    def __init__(self, data: bytes) -> None:
        """
        Hold the data and start counting reads at the end.

        Example:
            >>> EndOfFileGuard(b"").empty_reads
            0


        :param data: The stream's content.
        :return: None.
        """
        super().__init__(data)
        self.empty_reads = 0

    def read(self, size: int | None = -1, /) -> bytes:
        """
        Read as a stream does, failing after many reads at the end.

        Example:
            >>> EndOfFileGuard(b"x").read(1)
            b'x'


        :param size: How many bytes to read; all if negative.
        :return: The bytes read.
        :raises AssertionError: When the reader keeps reading at the end.
        """
        data = super().read(size)
        if not data and size != 0:
            self.empty_reads += 1
            if self.empty_reads > self.allowed_empty_reads:
                raise AssertionError("the reader kept reading after the end")
        return data


def snb_file(tmp_path: Path, head: str, cover: bytes | None = None) -> bytes:
    """
    Write an SNB book with calibre's own SNB writer.

    Example:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as folder:
        ...     snb_file(Path(folder), "<name>T</name>")[:8]
        b'SNBP000B'


    :param tmp_path: A folder to build the book in.
    :param head: The elements inside ``book.snbf``'s ``head``.
    :param cover: A JPEG to store as ``cover.jpg``, if any.
    :return: The book's bytes.
    """
    source = tmp_path / "snb-source"
    (source / "snbf").mkdir(parents=True)
    (source / "snbc" / "images").mkdir(parents=True)
    (source / "snbf" / "book.snbf").write_text(
        f'<book-snbf version="1.0"><head>{head}</head></book-snbf>',
        encoding="utf-8",
    )
    (source / "snbc" / "chapter.snbc").write_text(
        "<snbc><head><title>One</title></head><body><text>x</text></body></snbc>",
        encoding="utf-8",
    )
    if cover is not None:
        (source / "snbc" / "images" / "cover.jpg").write_bytes(cover)
    book = snbfile.SNBFile()
    book.FromDir(str(source))
    out = io.BytesIO()
    book.Output(out)
    return out.getvalue()


def vwi(value: int) -> bytes:
    """
    Encode a number as a Topaz variable-width integer.

    Example:
        >>> vwi(5), vwi(200)
        (b'\\x05', b'\\x81H')


    :param value: The number.
    :return: Seven bits a byte, high bit set on all but the last.
    """
    groups = [value & 0x7F]
    value >>= 7
    while value:
        groups.append((value & 0x7F) | 0x80)
        value >>= 7
    return bytes(reversed(groups))


def topaz_file(records: dict[bytes, bytes]) -> bytes:
    """
    Build a Topaz file whose only section is its metadata record.

    Example:
        >>> topaz_file({b"Title": b"T"})[:4]
        b'TPZ0'


    :param records: The metadata record's tags and values.
    :return: The file's bytes.
    """
    body = b"".join(
        vwi(len(tag)) + tag + vwi(len(value)) + value for tag, value in records.items()
    )
    metadata = vwi(8) + b"metadata" + b"\x00" + bytes((len(records),)) + body
    # calibre counts the record's length without the tag length, tag and flags.
    header = b"c" + vwi(8) + b"metadata" + vwi(1) + vwi(0)
    header += vwi(len(metadata) - 10) + vwi(0)
    return b"TPZ0" + vwi(1) + header + b"d" + metadata


def pml_text(**fields: str) -> bytes:
    """
    Build PML text whose comment block holds the given header fields.

    Example:
        >>> pml_text(TITLE="T")
        b'\\\\v TITLE="T" \\\\v\\nText.\\n'


    :param fields: Header names and values, such as ``TITLE``.
    :return: The text, encoded as cp1252 as PML is.
    """
    header = " ".join(f'{name}="{value}"' for name, value in fields.items())
    return f"\\v {header} \\v\nText.\n".encode("cp1252")


def context(work_dir: Path) -> ProviderContext:
    """
    Build a provider context with a fresh report.

    Example:
        >>> context(Path.cwd()).options
        {}


    :param work_dir: The provider's work folder.
    :return: The context.
    """
    return ProviderContext(
        work_dir=work_dir,
        options={},
        log=logging.getLogger("tests.ported-calibre-batch-two"),
        report=ConversionReport(),
    )


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


def read(path: Path, plugin: str) -> tuple[dict[str, Any], Any]:
    """
    Read one file with a ported reader's provider.

    Example:
        >>> read(Path("book.rtf"), "Read RTF metadata")  # doctest: +SKIP
        ({'title': ...}, None)


    :param path: The file.
    :param plugin: The calibre reader's name.
    :return: The fields and the cover.
    """
    provider = adapt_ported_reader(plugin)()
    file_format = path.suffix[1:]
    values = dict(
        provider.read_calibre_metadata(path, file_format, context(path.parent)).values
    )
    cover = provider.extract_cover(path, file_format, context(path.parent))
    return values, cover


def test_snb_reads_the_book_head_and_cover(tmp_path: Path) -> None:
    """
    Read an SNB book's name, author, language, publisher and cover.

    Example:
        >>> test_snb_reads_the_book_head_and_cover(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "book.snb"
    path.write_bytes(
        snb_file(
            tmp_path,
            "<name>Shanda Title</name><author>Li Fanxi</author>"
            "<language>ZH_CN</language><publisher>Pub Co</publisher>"
            "<cover>cover.jpg</cover>",
            cover=JPEG,
        )
    )

    values, cover = read(path, "Read SNB metadata")

    assert values["title"] == "Shanda Title"
    assert values["authors"] == ("Li Fanxi",)
    assert values["languages"] == ("zh-cn",)
    assert values["publisher"] == "Pub Co"
    assert cover is not None
    assert (cover.image_format, cover.data) == ("jpg", JPEG)


def test_snb_without_a_complete_head_reads_as_unknown(tmp_path: Path) -> None:
    """
    Fall back to Unknown when the head lacks an element, as calibre does.

    Example:
        >>> test_snb_without_a_complete_head_reads_as_unknown(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "partial.snb"
    path.write_bytes(snb_file(tmp_path, "<name>Only a Name</name>"))

    values, cover = read(path, "Read SNB metadata")

    # calibre sets the title, then stops at the missing author; authors left
    # as Unknown count as unset, so they are not serialised.
    assert values["title"] == "Only a Name"
    assert "authors" not in values
    assert cover is None


def test_rtf_reads_the_info_group(tmp_path: Path) -> None:
    """
    Read what calibre's RTF writer stores, non-ASCII text included.

    Example:
        >>> test_rtf_reads_the_info_group(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    book = _compat.Metadata("Test ø̄title", ["Author One", "Author БTwo"])
    book.tags = ["tag1", "見tag2"]
    book.comments = "<p>some ⊹comments</p>"
    book.publisher = "publiSher"
    stream = io.BytesIO(rb"{\rtf1\ansi\ansicpg1252}")
    rtf.set_metadata(stream, book)
    path = tmp_path / "book.rtf"
    path.write_bytes(stream.getvalue())

    values, cover = read(path, "Read RTF metadata")

    assert values["title"] == "Test ø̄title"
    assert values["authors"] == ("Author One", "Author БTwo")
    assert values["tags"] == ("tag1", "見tag2")
    assert values["comments"] == "<p>some ⊹comments</p>"
    assert values["publisher"] == "publiSher"
    assert cover is None


def test_rtf_decodes_code_page_escapes(tmp_path: Path) -> None:
    """
    Decode ``\\'xx`` escapes with the file's ``\\ansicpg`` code page.

    Example:
        >>> test_rtf_decodes_code_page_escapes(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "cyrillic.rtf"
    path.write_bytes(
        rb"{\rtf1\ansi\ansicpg1251{\info{\title \'cc\'e8\'f0}"
        rb"{\author Ann, Bob}}Text.}"
    )

    values, _ = read(path, "Read RTF metadata")

    assert values["title"] == "Мир"
    assert values["authors"] == ("Ann", "Bob")


def test_rtf_with_an_unclosed_info_group_stops_at_the_end() -> None:
    """
    Stop at the end of a file whose info group is cut off.

    Upstream reads on forever here, adding to its block on every read.

    Example:
        >>> test_rtf_with_an_unclosed_info_group_stops_at_the_end()


    :return: None; the assertions are the check.
    """
    stream = EndOfFileGuard(rb"{\rtf1\ansi{\info{\title Cut off")

    book = rtf.get_metadata(stream)

    assert book.title == "Unknown"


@pytest.mark.parametrize(
    "block",
    [
        rb"{\info{\title A}{\author B}}",
        rb"{\info{\title A \} still A}}",
        rb"{\info{\title A \\}}",
        rb"{\info{\titlepg}{\title Real}}",
        rb"{\title Outside}{\info{\title Inside}}",
        rb"{\info{\author A}{\title never closed",
        rb"{\info{\info{\title X}",
        b"",
    ],
)
def test_rtf_field_stand_ins_match_calibre_patterns(block: bytes) -> None:
    """
    Find and replace the same fields calibre's RTF patterns do.

    Example:
        >>> test_rtf_field_stand_ins_match_calibre_patterns(rb"{\\info{\\title A}}")


    :param block: An info block, well formed or not.
    :return: None; the assertions are the check.
    """
    text = block.decode("ascii")
    for name in ("title", "author", "subject", "category", "manager"):
        pattern = re.compile(
            rb"\{\\info.*?\{\\" + name.encode() + rb"(.*?)(?<!\\)\}", re.DOTALL
        )
        expected = pattern.search(block)
        found = _compat.InfoFieldPattern(name.encode()).search(block)
        assert (None if found is None else found.group(1)) == (
            None if expected is None else expected.group(1)
        )
        template = r"{\\" + name + " " + r"a\\b\\u1234?" + "}"
        writer = re.compile(r"\{\\" + name + r"(.*?)(?<!\\)\}", re.DOTALL)
        assert _compat.FieldPattern(name).subn(template, text) == writer.subn(
            template, text
        )


def test_rtf_many_unclosed_info_groups_are_read_in_linear_time() -> None:
    """
    Read and rewrite repeated, never-closed info groups quickly.

    calibre's patterns backtrack in cubic time here: 15 kB took nearly a minute
    to read. The 1.5 MB file runs in a child process with a time limit, so a
    regression fails rather than hanging the suite.

    Example:
        >>> test_rtf_many_unclosed_info_groups_are_read_in_linear_time()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    script = (
        "import io\n"
        "from LiuXin_alpha.metadata.upstream_calibre import _compat, rtf\n"
        "data = b'{\\\\rtf1' + b'{\\\\info{\\\\title x' * 100000\n"
        "assert rtf.get_metadata(io.BytesIO(data)).title == 'Unknown'\n"
        "try:\n"
        "    rtf.set_metadata(io.BytesIO(data), _compat.Metadata('New', ['A']))\n"
        "except ValueError:\n"
        "    pass\n"
        "print('done')\n"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "done"


def test_topaz_reads_title_and_authors(tmp_path: Path) -> None:
    """
    Read a Topaz file's title and its semicolon-separated authors.

    Example:
        >>> test_topaz_reads_title_and_authors(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "book.tpz"
    path.write_bytes(
        topaz_file(
            {
                b"Title": "Topaz Títle".encode(),
                b"Authors": b"Ann Smith;Bob Jones",
                b"bookLength": b"1234",
            }
        )
    )

    values, cover = read(path, "Read Topaz metadata")

    assert values["title"] == "Topaz Títle"
    assert values["authors"] == ("Ann Smith", "Bob Jones")
    assert cover is None


def test_topaz_writer_round_trips() -> None:
    """
    Read back what calibre's Topaz writer stores.

    Example:
        >>> test_topaz_writer_round_trips()


    :return: None; the assertions are the check.
    """
    stream = io.BytesIO(topaz_file({b"Title": b"Old", b"Authors": b"Ann"}))
    topaz.set_metadata(stream, _compat.Metadata("New Title", ["Cy", "Di"]))

    book = topaz.get_metadata(stream)

    assert book.title == "New Title"
    assert book.authors == ["Cy", " Di"]


@pytest.mark.parametrize(
    "data",
    [
        b"TPZ0" + vwi(2**28 - 1) + b"c",
        b"TPZ0" + vwi(1) + b"c" + vwi(8) + b"metadata" + vwi(2**28 - 1),
    ],
    ids=["header-count", "block-count"],
)
def test_topaz_with_counts_past_the_end_is_refused(
    data: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Refuse a Topaz file whose counts run past its end.

    Upstream reads on through every counted record: a 9-byte file kept it
    busy for minutes, using gigabytes. Reads past the end touch no stream, so
    the test counts decoded numbers instead, and fails fast if that returns.

    Example:
        Run with pytest, which supplies ``monkeypatch``::

            python -m pytest -q tests/file_formats/providers/test_ported_snb_rtf_topaz_pml.py


    :param data: A damaged file.
    :param monkeypatch: Wraps the number decoder with a counter.
    :return: None; the assertions are the check.
    """
    decode = topaz.MetadataUpdater.decode_vwi
    calls = 0

    def counted(self: Any, byts: bytes) -> tuple[int, int]:
        """
        Decode as calibre does, failing once far more numbers were read.

        Example:
            Replaces ``MetadataUpdater.decode_vwi`` during the test.


        :param self: The updater.
        :param byts: The encoded number.
        :return: The number and the bytes it took.
        :raises AssertionError: When the reader keeps decoding.
        """
        nonlocal calls
        calls += 1
        if calls > 1000:
            raise AssertionError("the reader kept reading after the end")
        return decode(self, byts)

    monkeypatch.setattr(topaz.MetadataUpdater, "decode_vwi", counted)

    with pytest.raises(ValueError, match="Truncated Topaz header"):
        topaz.get_metadata(io.BytesIO(data))


def test_topaz_rejects_other_files(tmp_path: Path) -> None:
    """
    Raise for a file without the Topaz signature, as calibre's reader does.

    Example:
        >>> test_topaz_rejects_other_files(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "other.tpz"
    path.write_bytes(b"PK\x03\x04" + b"\x00" * 30)

    with pytest.raises(ValueError, match="Not a Topaz file"):
        read(path, "Read Topaz metadata")


def test_pml_reads_the_header_comment(tmp_path: Path) -> None:
    """
    Read PML's title, authors, publisher, rights and ISBN, and a nearby cover.

    Example:
        >>> test_pml_reads_the_header_comment(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "book.pml"
    path.write_bytes(
        pml_text(
            TITLE="Palm Títle",
            AUTHOR="Ann Smith",
            PUBLISHER="Pub Co",
            COPYRIGHT="2001",
            ISBN="9780000000002",
        )
        + pml_text(AUTHOR="Bob Jones")
    )
    (tmp_path / "book_img").mkdir()
    (tmp_path / "book_img" / "cover.png").write_bytes(PNG)

    values, cover = read(path, "Read PML metadata")

    assert values["title"] == "Palm Títle"
    assert values["authors"] == ("Ann Smith", "Bob Jones")
    assert values["publisher"] == "Pub Co"
    assert values["rights"] == "2001"
    assert values["identifiers"] == {"isbn": "9780000000002"}
    assert cover is not None
    assert (cover.image_format, cover.data) == ("png", PNG)


def test_pml_escapes_header_text_for_xml_as_calibre_does(tmp_path: Path) -> None:
    """
    Keep calibre's XML escaping of header values, after decoding entities.

    Example:
        >>> test_pml_escapes_header_text_for_xml_as_calibre_does(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "escaped.pml"
    path.write_bytes(pml_text(TITLE="Tom &amp; Jerry <2> &copy; &#x2122;"))

    values, _ = read(path, "Read PML metadata")

    assert values["title"] == "Tom &amp; Jerry &lt;2&gt; © ™"


def test_pmlz_reads_the_archive_and_its_cover(tmp_path: Path) -> None:
    """
    Read the PML files inside a PMLZ archive, and its top-level cover.

    Example:
        >>> test_pmlz_reads_the_archive_and_its_cover(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    path = tmp_path / "book.pmlz"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("book.pml", pml_text(TITLE="Zipped", AUTHOR="Ann"))
        archive.writestr("cover.png", PNG)

    values, cover = read(path, "Read PML metadata")

    assert values["title"] == "Zipped"
    assert values["authors"] == ("Ann",)
    assert cover is not None
    assert cover.data == PNG


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Tom &amp; Jerry", "Tom & Jerry"),
        ("&copy &copy; &COPY;", "&copy © ©"),
        ("&#65;&#x42;&#X43;", "ABC"),
        ("&#128;&#x81;&#0;&#xD800;&#99999999;", "€\x81\ufffd\ufffd\ufffd"),
        ("&&amp; &#; &#x; &# 65;", "&&amp; &#; &#x; &# 65;"),
        ("&squot;&hellips;", "'…"),
        ("&" + "a" * 70 + ";", "&" + "a" * 70 + ";"),
    ],
)
def test_entities_are_replaced_as_calibre_replaces_them(
    raw: str, expected: str
) -> None:
    """
    Replace entities by the rules of calibre's C ``replace_all_entities``.

    Example:
        >>> test_entities_are_replaced_as_calibre_replaces_them("&lt;", "<")


    :param raw: Text with references.
    :param expected: What calibre makes of it.
    :return: None; the assertion is the check.
    """
    assert _compat.replace_entities(raw) == expected


def test_force_unicode_falls_back_as_calibre_does() -> None:
    """
    Decode bytes with the asked encoding, then the fallbacks.

    Example:
        >>> test_force_unicode_falls_back_as_calibre_does()


    :return: None; the assertions are the check.
    """
    assert _compat.force_unicode("é".encode(), "utf-8") == "é"
    assert _compat.force_unicode("é".encode("latin-1"), "ascii") in {
        "é",
        repr("é".encode("latin-1")),
    }
    assert _compat.force_unicode(7) == 7


@pytest.mark.parametrize("module", [pml, rtf, snbfile, topaz])
def test_ported_modules_import_nothing_from_calibre(module: Any) -> None:
    """
    Keep the ported modules free of imports from calibre itself.

    Example:
        >>> test_ported_modules_import_nothing_from_calibre(pml)


    :param module: A ported module.
    :return: None; the assertion is the check.
    """
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported = {
        name
        for node in ast.walk(tree)
        for name in (
            [node.module or ""]
            if isinstance(node, ast.ImportFrom)
            else [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else []
        )
    }
    roots = {name.partition(".")[0] for name in imported}
    assert not roots & {"calibre", "polyglot", "calibre_extensions", "compression"}


def test_examples_in_the_helpers_run() -> None:
    """
    Run the docstring examples of the ported helpers.

    Example:
        >>> test_examples_in_the_helpers_run()


    :return: None; the assertions are the check.
    """
    result = doctest.testmod(_compat, report=False)
    assert result.attempted > 0
    assert result.failed == 0
