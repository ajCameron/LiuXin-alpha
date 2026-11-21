"""
Check that what LiuXin's metadata writers put in a book reads back the same in calibre's reader, and that nothing else changes.

Found by writing every book in ``LiuXin_alpha_data`` with each writer and reading
the result with both LiuXin's readers and calibre's ported ones:

- DOCX tags had their spaces written as ``_-_``, which Word and calibre show as written.
- DOCX languages were written as ISO 639-2 codes (``fre``) where Word expects ``fr``.
- LRF authors had their ampersand written as LiuXin's private token ``,0420,``.
- RTF authors were joined with ``& `` (no space before it), so calibre read one author.

The last test repeats that audit on every book in ``LiuXin_alpha_data`` when it is
checked out, and is skipped otherwise.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_metadata_writer_interchange.py
"""

from __future__ import annotations

import io
import re
import struct
import zipfile
from pathlib import Path
from typing import Any

import pytest
from lxml import etree

from LiuXin_alpha.file_formats.lrf.meta import LRFMetaFile
from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata,
    set_file_type_metadata,
)
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from LiuXin_alpha.metadata.book.base import calibreMetadata
from tests.support.file_format_docx import build_unicode_docx
from tests.support.file_format_lrf import lrf_book

PORTED = {"docx": "docx", "epub": "epub", "fb2": "fb2", "htmlz": "extz", "txtz": "extz", "lrf": "lrf",
          "mobi": "mobi", "azw": "mobi", "azw3": "mobi", "prc": "mobi", "pdb": "pdb", "rtf": "rtf"}  # fmt: skip


def _values(raw: Any) -> list[str]:
    """
    List a field's values, whichever container holds them.

    Example:
        >>> _values({"a": 1}), _values("b"), _values(None)
        (['a'], ['b'], [])


    :param raw: The field.
    :return: Its values as text.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(value) for value in raw]


def _write(
    data: bytes, ftype: str, errors: list[str] | None = None, **fields: Any
) -> bytes:
    """
    Write fields into a book through the writer plugin, as the application does.

    Example:
        >>> _write(b"", "rtf", title="T")  # doctest: +SKIP


    :param data: The book.
    :param ftype: Its format.
    :param errors: Collects the errors the writer reports; without it they are only logged.
    :param fields: The fields; ``title`` and ``authors`` are required.
    :return: The rewritten book.
    """
    mi = calibreMetadata(fields.pop("title"), fields.pop("authors"))
    for name, value in fields.items():
        setattr(mi, name, value)
    stream = io.BytesIO(data)
    report = (
        None
        if errors is None
        else (lambda _mi, _ftype, trace: errors.append(str(trace)))
    )
    set_file_type_metadata(stream, mi, ftype, report_error=report)
    return stream.getvalue()


def _both(data: bytes, ftype: str) -> tuple[Any, Any]:
    """
    Read a book with LiuXin's reader and with calibre's.

    Example:
        >>> _both(b"", "rtf")  # doctest: +SKIP


    :param data: The book.
    :param ftype: Its format.
    :return: LiuXin's metadata and calibre's.
    """
    return get_file_type_metadata(io.BytesIO(data), ftype), ported_reader(
        PORTED[ftype]
    )(io.BytesIO(data))


def _docx(tmp_path: Path, keywords: str | None = None) -> bytes:
    """
    Make a DOCX without a subject (which both readers take as a tag), optionally with the given keywords.

    Example:
        >>> _docx(Path("/tmp"))[:2]  # doctest: +SKIP
        b'PK'


    :param tmp_path: A folder to build in.
    :param keywords: The ``cp:keywords`` text, written straight into the core properties;
        ``None`` for none.
    :return: The book.
    """
    path = tmp_path / "book.docx"
    build_unicode_docx(path, include_image=False)
    replacement = (
        f"<cp:keywords>{keywords}</cp:keywords>".encode()
        if keywords is not None
        else b""
    )
    out = io.BytesIO()
    with zipfile.ZipFile(path) as source, zipfile.ZipFile(out, "w") as target:
        for item in source.infolist():
            member = source.read(item)
            if item.filename == "docProps/core.xml":
                member = re.sub(
                    rb"<dc:subject>.*?</dc:subject>", b"", member, flags=re.S
                )
                member = re.sub(
                    rb"<cp:keywords>.*?</cp:keywords>", replacement, member, flags=re.S
                )
            target.writestr(item, member)
    return out.getvalue()


def test_docx_tags_and_language_are_written_as_word_and_calibre_expect(
    tmp_path: Path,
) -> None:
    """
    Tags are plain comma-separated keywords, the language a two-letter code; both readers agree.

    Example:
        >>> test_docx_tags_and_language_are_written_as_word_and_calibre_expect(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :return: None after the file and both readings are checked.
    """
    out = _write(
        _docx(tmp_path),
        "docx",
        title="T",
        authors=["A"],
        tags=["alpha", "βeta"],
        languages=["fra"],
    )
    core = zipfile.ZipFile(io.BytesIO(out)).read("docProps/core.xml").decode("utf-8")
    assert "<cp:keywords>alpha, βeta</cp:keywords>" in core
    assert "<dc:language>fr</dc:language>" in core
    ours, calibre = _both(out, "docx")
    assert sorted(_values(ours.tags)) == sorted(calibre.tags) == ["alpha", "βeta"]
    assert calibre.languages == ["fra"]


@pytest.mark.parametrize(
    ("keywords", "tags"),
    [
        ("alpha,_-_βeta", ["alpha", "βeta"]),
        ("science_-_fiction", ["science fiction"]),
        ("science fiction, cats", ["science fiction", "cats"]),
        ("cats; dogs", ["cats", "dogs"]),
        ("cats dogs", ["cats", "dogs"]),
    ],
)
def test_docx_keywords_are_split_on_commas_when_there_are_any(
    tmp_path: Path, keywords: str, tags: list[str]
) -> None:
    """
    Commas or semicolons separate tags that may hold spaces; without them, spaces separate; ``_-_`` is a space.

    Example:
        >>> test_docx_keywords_are_split_on_commas_when_there_are_any(Path("/tmp"), "a b", ["a", "b"])  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :param keywords: The keywords as a file holds them; the first two as LiuXin once wrote them.
    :param tags: The tags LiuXin's reader should find.
    :return: None after the tags are checked.
    """
    md = get_file_type_metadata(io.BytesIO(_docx(tmp_path, keywords)), "docx")
    assert _values(md.tags) == tags


def test_lrf_authors_are_written_with_a_plain_ampersand() -> None:
    """
    Authors are joined with `` & `` (an ampersand in a name doubled), which both readers split.

    LiuXin's metadata record still splits a name holding ``&`` when it is set (a known
    container issue), so only calibre's reading is checked for that name.

    Example:
        >>> test_lrf_authors_are_written_with_a_plain_ampersand()


    :return: None after the field and both readings are checked.
    """
    out = _write(lrf_book(), "lrf", title="T", authors=["Ann Writer", "Bob Writer"])
    assert LRFMetaFile(io.BytesIO(out)).author == "Ann Writer & Bob Writer"
    ours, calibre = _both(out, "lrf")
    assert _values(ours.authors) == calibre.authors == ["Ann Writer", "Bob Writer"]
    out = _write(lrf_book(), "lrf", title="T", authors=["Ann Writer", "Bob & Co"])
    assert LRFMetaFile(io.BytesIO(out)).author == "Ann Writer & Bob && Co"
    assert ported_reader("lrf")(io.BytesIO(out)).authors == ["Ann Writer", "Bob & Co"]


def test_lrf_authors_written_with_the_old_token_still_read() -> None:
    """
    Books LiuXin wrote with ``,0420,`` for the ampersand still read as two authors.

    Example:
        >>> test_lrf_authors_written_with_the_old_token_still_read()


    :return: None after the authors are checked.
    """
    stream = io.BytesIO(lrf_book())
    LRFMetaFile(stream).author = "Ann Writer ,0420, Bob Writer"
    assert _values(
        get_file_type_metadata(io.BytesIO(stream.getvalue()), "lrf").authors
    ) == ["Ann Writer", "Bob Writer"]


def test_rtf_authors_are_joined_with_commas() -> None:
    """
    Authors are joined with commas, as calibre writes and reads them; both readers find each.

    Example:
        >>> test_rtf_authors_are_joined_with_commas()


    :return: None after the field and both readings are checked.
    """
    original = rb"{\rtf1\ansi{\info{\title Old}{\author Someone}}Hello.}"
    out = _write(original, "rtf", title="T", authors=["Ann Writer", "Bob Writer"])
    assert rb"{\author Ann Writer, Bob Writer}" in out
    assert out.endswith(rb"Hello.}")
    ours, calibre = _both(out, "rtf")
    assert _values(ours.authors) == calibre.authors == ["Ann Writer", "Bob Writer"]


# --- the audit, on real books -------------------------------------------------------------------


def _outside_metadata(ftype: str, data: bytes) -> Any:
    """
    Take what a book holds outside its metadata, so a write can be checked to have left it alone.

    Example:
        >>> _outside_metadata("rtf", rb"{\\rtf1{\\info{\\title T}}x}")
        b'{\\\\rtf1x}'


    :param ftype: The format.
    :param data: The book.
    :return: Something equal before and after a write that changed only the metadata.
    """
    if ftype in ("docx", "epub", "htmlz", "txtz"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = [item.filename for item in archive.infolist()]
            kept = {
                name: archive.read(name)
                for name in names
                if not name.endswith(".opf") and not name.startswith("docProps/")
            }
        return names, kept
    if ftype in ("mobi", "azw", "azw3", "prc", "pdb"):
        count = struct.unpack(">H", data[76:78])[0]
        offsets = [
            struct.unpack(">I", data[78 + 8 * i : 82 + 8 * i])[0] for i in range(count)
        ] + [len(data)]
        return data[60:68], [data[offsets[i] : offsets[i + 1]] for i in range(1, count)]
    if ftype == "fb2":
        # The writer serialises the XML afresh, as calibre's does: compare it canonically.
        root = etree.fromstring(
            data,
            etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True),
        )
        for child in list(root):
            if isinstance(child.tag, str) and child.tag.endswith("description"):
                root.remove(child)
        return etree.tostring(root, method="c14n")
    if ftype == "rtf":
        start = data.find(rb"{\info")
        depth, index = 0, start
        while start >= 0 and index < len(data):
            char = data[index : index + 1]
            if char == b"\\":
                index += 2
                continue
            depth += {b"{": 1, b"}": -1}.get(char, 0)
            if depth == 0:
                return data[:start] + data[index + 1 :]
            index += 1
        return data
    if ftype == "lrf":
        # Objects move when the metadata block changes size, so compare them by ID.
        count, index = struct.unpack("<QQ", data[0x10:0x20])
        entries = [
            struct.unpack("<IIII", data[index + 16 * i : index + 16 * i + 16])
            for i in range(count)
        ]
        return {
            obj_id: data[offset : offset + size]
            for obj_id, offset, size, _reserved in entries
        }
    raise AssertionError(f"no check for {ftype}")


def test_every_real_book_keeps_its_contents_and_reads_back_in_both_readers(
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    """
    Write each book in ``LiuXin_alpha_data`` twice; nothing outside the metadata changes, and both readers agree.

    Only text every format can hold is written, so the PDB and MOBI code-page limits do
    not show. PDB books keep only a title, in their header.

    Example:
        >>> test_every_real_book_keeps_its_contents_and_reads_back_in_both_readers({})  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :return: None after every book is checked.
    """
    checked = 0
    for ftype in PORTED:
        for path in md_test_files_by_ext.get(ftype, []):
            original = path.read_bytes()
            data = original
            for title, authors in (
                ("Audit Title One", ["Ann Writer", "Bob Writer"]),
                ("Audit Title Two", ["Cy Writer"]),
            ):
                errors: list[str] = []
                result = _write(
                    data,
                    ftype,
                    errors,
                    title=title,
                    authors=authors,
                    tags=["alpha"],
                    publisher="Audit Press",
                )
                if errors:
                    # A writer may refuse a book (an HTMLZ without an OPF, as calibre's does), but not damage it.
                    assert result == data, path.name
                    break
                data = result
                assert _outside_metadata(ftype, data) == _outside_metadata(
                    ftype, original
                ), path.name
                ours, calibre = _both(data, ftype)
                assert ours.title == title, path.name
                if ftype != "pdb":
                    assert calibre.title == title, path.name
                    assert _values(ours.authors) == calibre.authors == authors, (
                        path.name
                    )
            else:
                checked += 1
    assert checked >= 10
