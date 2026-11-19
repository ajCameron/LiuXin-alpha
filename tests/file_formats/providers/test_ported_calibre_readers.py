"""
Check the readers ported from upstream calibre and their providers.

The samples are built here byte by byte, following each format's layout as
calibre's reader parses it. The same files are also read by the fork's
calibre path, so the pool can be compared.
"""

from __future__ import annotations

import doctest
import logging
import struct
import zlib
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata, ported_calibre
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.ported_calibre import (
    PORTED_PRIORITY,
    PORTED_READERS,
    adapt_ported_reader,
    ported_reader,
    register_ported_calibre_readers,
)
from LiuXin_alpha.file_formats.providers.registry import ProviderRegistry
from LiuXin_alpha.file_formats.providers.upstream_calibre import (
    UPSTREAM_PRIORITY,
    register_upstream_calibre_readers,
)
from LiuXin_alpha.metadata.upstream_calibre import _compat, metadata_base


def imp_file(title: str, author: str, category: str) -> bytes:
    """
    Build an IMP header as calibre's IMP reader expects it.

    Example:
        >>> imp_file("T", "A", "C")[:10]
        b'\\x00\\x01BOOKDOUG'


    :param title: The book's title.
    :param author: The author string.
    :param category: The category.
    :return: The file's bytes.
    """

    def text(value: str) -> bytes:
        """
        Encode one NUL-terminated string.

        Example:
            >>> text("a")
            b'a\\x00'


        :param value: The string.
        :return: Its bytes with a terminating NUL.
        """
        return value.encode("utf-8") + b"\x00"

    return (
        b"\x00\x01BOOKDOUG"
        + b"\x00" * 38
        + text("ignored")
        + text(category)
        + text("skipped")
        + text(title)
        + text("skipped")
        + text("skipped")
        + text(author)
    )


def rb_file(info: str) -> bytes:
    """
    Build a Rocket eBook file whose INFO record holds ``info``.

    Example:
        >>> rb_file("TITLE=T")[:14]
        b'\\xb0\\x0c\\xb0\\x0c\\x02\\x00NUVO\\x00\\x00\\x00\\x00'


    :param info: The INFO record's text.
    :return: The file's bytes.
    """
    record = info.encode("utf-8")
    toc_at = 28
    info_at = toc_at + 4 + 44
    head = b"\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00" + b"\x00" * 10
    toc = struct.pack("<I", 1) + b"INFO".ljust(32, b"\x00")
    toc += struct.pack("<III", len(record), info_at, 2)
    body = head + struct.pack("<I", toc_at) + toc
    assert len(body) == info_at
    return body + record


def lrx_file(title: str, author: str, publisher: str, language: str) -> bytes:
    """
    Build an LRX container holding compressed book information.

    Example:
        >>> lrx_file("T", "A", "P", "en")[4:12]
        b'ftypLRX2'


    :param title: The book's title.
    :param author: The author string.
    :param publisher: The publisher.
    :param language: The language code.
    :return: The file's bytes.
    """
    xml = (
        "<Info><BookInfo>"
        f'<Title reading="{title.upper()}">{title}</Title>'
        f"<Author>{author}</Author><Publisher>{publisher}</Publisher>"
        "<Category>Fiction</Category>"
        f"</BookInfo><DocInfo><Language>{language}</Language></DocInfo></Info>"
    ).encode()
    packed = zlib.compress(xml)
    box = struct.pack(">L", 12) + b"ftypLRX2"
    lrf = bytearray(b"\x00" * 0x4C)
    lrf[0:8] = "LRF\x00".encode("utf-16-le")
    lrf[8:12] = struct.pack("<L", 999)
    tail = struct.pack("<H", len(packed) + 4) + b"\x00" * 6
    tail += struct.pack("<L", len(xml)) + packed
    return box + struct.pack(">L", 8) + b"bbeb" + bytes(lrf) + tail


def context(work_dir: Path) -> ProviderContext:
    """
    Build a provider context with a fresh report.

    Example:
        >>> context(Path.cwd()).options
        {}


    :param work_dir: The call's work folder.
    :return: The context.
    """
    return ProviderContext(
        work_dir=work_dir,
        options={},
        log=logging.getLogger("tests.ported-calibre"),
        report=ConversionReport(),
    )


@pytest.fixture(autouse=True)
def empty_cache() -> None:
    """
    Start every test with no remembered readings.

    Example:
        >>> "empty_cache" in globals()
        True


    :return: None after the cache is emptied.
    """
    fork_metadata._cache.clear()


def read(tmp_path: Path, plugin: str, name: str, data: bytes) -> dict[str, Any]:
    """
    Read one sample with a ported reader's provider.

    Example:
        >>> read(Path("/tmp"), "Read RB metadata", "a.rb", rb_file("TITLE=T"))  # doctest: +SKIP
        {'title': 'T', ...}


    :param tmp_path: Where to write the sample.
    :param plugin: The calibre plugin name.
    :param name: The sample's file name.
    :param data: The sample's bytes.
    :return: The fields read, as a plain dictionary.
    """
    path = tmp_path / name
    path.write_bytes(data)
    file_format = name.rpartition(".")[2]
    provider = adapt_ported_reader(plugin)
    fields = provider().read_calibre_metadata(path, file_format, context(tmp_path))
    return dict(fields.values)


def test_every_port_registers_above_the_installed_calibre() -> None:
    """
    Register each port with a priority above running an installed calibre.

    Example:
        >>> test_every_port_registers_above_the_installed_calibre()


    :return: None after the registry's order is checked.
    """
    registry = ProviderRegistry()
    register_upstream_calibre_readers(registry)
    made = register_ported_calibre_readers(registry)
    assert len(made) == len(PORTED_READERS) == 18
    assert PORTED_PRIORITY > UPSTREAM_PRIORITY
    for provider in made:
        file_format = next(iter(provider.calibre_read_formats))
        found = registry.calibre_metadata_readers(file_format)
        assert found[0] is provider
        assert found[1].provider_id == provider.provider_id.replace(
            "calibre-port.", "calibre."
        )


def test_ported_txt_reads_title_and_authors(tmp_path: Path) -> None:
    """
    Read the title and authors from a text file's first lines.

    Example:
        >>> test_ported_txt_reads_title_and_authors(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the fields are checked.
    """
    values = read(tmp_path, "Read TXT metadata", "book.txt", b"A Title\n\n\nAnn, Bob\n")
    assert values["title"] == "A Title"
    assert values["authors"] == ("Ann", " Bob")


def test_ported_imp_reads_title_author_and_category(tmp_path: Path) -> None:
    """
    Read an IMP header's title and authors, split as calibre splits them.

    Example:
        >>> test_ported_imp_reads_title_author_and_category(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the fields are checked.
    """
    values = read(
        tmp_path,
        "Read IMP metadata",
        "book.imp",
        imp_file("Imp Title", "Ann Smith and Bob Jones", "Novels"),
    )
    assert values["title"] == "Imp Title"
    assert values["authors"] == ("Ann Smith", "Bob Jones")


def test_ported_rb_reads_the_info_record(tmp_path: Path) -> None:
    """
    Read a Rocket eBook's INFO record.

    Example:
        >>> test_ported_rb_reads_the_info_record(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the fields are checked.
    """
    values = read(
        tmp_path,
        "Read RB metadata",
        "book.rb",
        rb_file("TITLE=Rocket Title\nAUTHOR=Ann & Bob\n"),
    )
    assert values["title"] == "Rocket Title"
    assert values["authors"] == ("Ann", "Bob")


def test_ported_lrx_reads_the_compressed_book_info(tmp_path: Path) -> None:
    """
    Read an LRX file's title, sort, author, publisher, tags and language.

    Example:
        >>> test_ported_lrx_reads_the_compressed_book_info(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the fields are checked.
    """
    values = read(
        tmp_path,
        "Read LRX metadata",
        "book.lrx",
        lrx_file("Sony Title", "Ann Smith", "Pub Co", "en"),
    )
    assert values["title"] == "Sony Title"
    assert values["title_sort"] == "SONY TITLE"
    assert values["authors"] == ("Ann Smith",)
    assert values["publisher"] == "Pub Co"
    assert values["tags"] == ("Fiction",)
    assert values["languages"] == ("en",)


def test_ported_readers_report_bad_files_as_upstream_does(tmp_path: Path) -> None:
    """
    Raise for a malformed LRX file, as calibre's reader does.

    Example:
        >>> test_ported_readers_report_bad_files_as_upstream_does(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the error is checked.
    """
    with pytest.raises(ValueError, match="Not a LRX file"):
        read(tmp_path, "Read LRX metadata", "bad.lrx", b"\x00" * 16)


def test_unknown_ports_are_refused() -> None:
    """
    Refuse a reader that has not been ported.

    Example:
        >>> test_unknown_ports_are_refused()


    :return: None after both lookups are refused.
    """
    with pytest.raises(KeyError):
        ported_reader("pdf")
    with pytest.raises(KeyError):
        adapt_ported_reader("Read PDF metadata")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ann Smith and Bob Jones", ["Ann Smith", "Bob Jones"]),
        ("Ann, with Bob", ["Ann", "Bob"]),
        ("Tom && Jerry & Spike", ["Tom & Jerry", "Spike"]),
        ("", []),
        (None, []),
    ],
)
def test_string_to_authors_splits_as_calibre_does(
    raw: str | None, expected: list[str]
) -> None:
    """
    Split author strings with calibre's default rule.

    Example:
        >>> test_string_to_authors_splits_as_calibre_does("A and B", ["A", "B"])


    :param raw: The author string.
    :param expected: The names calibre gives.
    :return: None after the split is compared.
    """
    assert metadata_base.string_to_authors(raw) == expected


def test_safe_xml_parsing_ignores_external_entities() -> None:
    """
    Parse XML without reading external entities, as calibre's parser does.

    Example:
        >>> test_safe_xml_parsing_ignores_external_entities()


    :return: None after the entity is checked.
    """
    root = _compat.safe_xml_fromstring(
        '<!DOCTYPE a [<!ENTITY x SYSTEM "file:///etc/passwd">]><a>&x;</a>'
    )
    assert "root:" not in (root.text or "")


@pytest.mark.parametrize("module", [ported_calibre, _compat])
def test_examples_in_the_modules_run(module: Any) -> None:
    """
    Run the docstring examples of the port's modules.

    Example:
        >>> test_examples_in_the_modules_run(_compat)


    :param module: The module to check.
    :return: None after every example passes.
    """
    result = doctest.testmod(module, report=False)
    assert result.attempted > 0
    assert result.failed == 0
