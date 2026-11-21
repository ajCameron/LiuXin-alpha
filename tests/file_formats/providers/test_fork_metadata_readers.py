"""
Check the providers made from the project's own metadata readers.

Two sets come from the same readers: fork providers take the fork's calibre
path and return calibre fields, and LiuXin providers run each reader as the
application does and place the result by WEMI level. The tests read small
sample files written here and the PDF golden fixtures.
"""

from __future__ import annotations

import doctest
import logging
import zipfile
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata, wemi_mapping
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.fork_metadata import (
    FORK_CALIBRE_READERS,
    ForkCalibreReader,
    LiuXinReader,
    MetadataModelError,
    adapt_fork_calibre_reader,
    adapt_liuxin_reader,
    register_fork_metadata_readers,
)
from LiuXin_alpha.file_formats.providers.legacy_conversion import LEGACY_PRIORITY
from LiuXin_alpha.file_formats.providers.metadata import (
    CalibreMetadataReader,
    CoverExtractor,
    WEMIMetadataReader,
)
from LiuXin_alpha.file_formats.providers.registry import (
    ProviderRegistry,
    capabilities_of,
)
from LiuXin_alpha.file_formats.providers.upstream_calibre import (
    register_upstream_calibre_readers,
)
from LiuXin_alpha.file_formats.providers.wemi_mapping import wemi_from_calibre
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.file_sources.builtin_readers import (
    get_metadata_reader_plugins,
)
from tests.support.file_format_epub import build_unicode_epub

PDF_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "pdf_golden"
    / "fake_realworld_001_info.pdf"
)

OPF = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Pool Test</dc:title>
  <dc:creator opf:role="aut">Jane Doe</dc:creator>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:language>en</dc:language>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:subject>Fiction</dc:subject>
 </metadata>
 <manifest/>
 <spine/>
</package>
"""

FB2 = """<?xml version="1.0" encoding="utf-8"?>
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
 <description>
  <title-info>
   <genre>fiction</genre>
   <author><first-name>Jane</first-name><last-name>Doe</last-name></author>
   <book-title>Pool Test</book-title>
   <lang>en</lang>
  </title-info>
  <publish-info><publisher>Pub Co</publisher><isbn>9780000000002</isbn></publish-info>
 </description>
 <body><section><p>Text.</p></section></body>
</FictionBook>
"""


def sample(folder: Path, file_format: str) -> Path:
    """
    Write a small file of one format with known metadata.

    Example:
        >>> sample(Path("/tmp"), "txt").name  # doctest: +SKIP
        'pool.txt'


    :param folder: Where to write it.
    :param file_format: ``txt``, ``opf``, ``fb2``, ``htmlz`` or ``epub``.
    :return: The file's path.
    """
    path = folder / f"pool.{file_format}"
    if file_format == "txt":
        path.write_text("Pool Test\n\nSome text.\n", encoding="utf-8")
    elif file_format == "opf":
        path.write_text(OPF, encoding="utf-8")
    elif file_format == "fb2":
        path.write_text(FB2, encoding="utf-8")
    elif file_format == "htmlz":
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("index.html", "<p>Text.</p>")
            archive.writestr("metadata.opf", OPF)
    elif file_format == "epub":
        build_unicode_epub(path)
    return path


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
        log=logging.getLogger("tests.fork-metadata"),
        report=ConversionReport(),
    )


def plugin_for(file_format: str) -> type[Any]:
    """
    Find the fork's reader plugin for a format.

    Example:
        >>> plugin_for("txt").name
        'Read TXT metadata'


    :param file_format: The format.
    :return: The plugin class.
    """
    return next(
        plugin
        for plugin in get_metadata_reader_plugins()
        if file_format in plugin.file_types
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


def test_both_sets_register_from_the_fork_readers() -> None:
    """
    Make fork providers for readers with a calibre path and LiuXin ones for all.

    Example:
        >>> test_both_sets_register_from_the_fork_readers()


    :return: None after every provider is checked.
    """
    registry = ProviderRegistry()
    made = register_fork_metadata_readers(registry)
    fork = [provider for provider in made if issubclass(provider, ForkCalibreReader)]
    liuxin = [provider for provider in made if issubclass(provider, LiuXinReader)]
    plugins = get_metadata_reader_plugins()

    assert len(fork) == len(FORK_CALIBRE_READERS) == 12
    # 25 formats' readers, and three PDF readers that each fix one merge rule.
    assert len(liuxin) == len(plugins) == 28
    assert {provider.fork_plugin.name for provider in fork} == set(FORK_CALIBRE_READERS)
    assert all(provider.provider_id.startswith("fork.") for provider in fork)
    assert all(provider.provider_id.startswith("liuxin.") for provider in liuxin)
    assert {provider.priority for provider in made} == {LEGACY_PRIORITY}
    assert capabilities_of(fork[0]) == (CalibreMetadataReader, CoverExtractor)
    assert capabilities_of(liuxin[0]) == (WEMIMetadataReader, CoverExtractor)


def test_the_pool_lists_wemi_readers_first() -> None:
    """
    Order a format's readers: LiuXin (WEMI), then upstream, then the fork.

    WEMI readings outrank calibre ones; upstream calibre has a higher
    priority than the fork's calibre path.

    Example:
        >>> test_the_pool_lists_wemi_readers_first()


    :return: None after the EPUB readers are listed.
    """
    registry = ProviderRegistry()
    register_fork_metadata_readers(registry)
    register_upstream_calibre_readers(registry)
    found = [provider.provider_id for provider in registry.metadata_readers("epub")]
    assert found == [
        "liuxin.read-epub-metadata",
        "calibre.read-epub-metadata",
        "fork.read-epub-metadata",
    ]


@pytest.mark.parametrize("file_format", ["txt", "opf", "epub"])
def test_fork_providers_return_calibre_fields(tmp_path: Path, file_format: str) -> None:
    """
    Read calibre fields through the fork's calibre path.

    Example:
        >>> test_fork_providers_return_calibre_fields(Path.cwd(), "txt")  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param file_format: The sample's format.
    :return: None after the title and authors are checked.
    """
    path = sample(tmp_path, file_format)
    provider = adapt_fork_calibre_reader(plugin_for(file_format))
    fields = provider().read_calibre_metadata(path, file_format, context(tmp_path))
    title = fields.values["title"]
    assert title in ("Pool Test", "EPUB Καλημέρα 世界")
    if file_format != "txt":
        assert fields.values["authors"]
    if file_format == "opf":
        assert fields.values["publisher"] == "Pub Co"
        assert dict(fields.values["identifiers"]) == {"isbn": "9780000000002"}


def test_a_fork_provider_refuses_liuxin_metadata(tmp_path: Path) -> None:
    """
    Raise rather than mislabel when a calibre path returns the LiuXin object.

    Example:
        >>> test_a_fork_provider_refuses_liuxin_metadata(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the error is checked.
    """

    class Plugin:
        """
        Stand in for a reader that returns the half-converted object.

        Example:
            >>> Plugin.name
            'Read RB metadata'
        """

        name = "Read RB metadata"
        file_types = frozenset({"rb"})

        def __init__(self, path: object) -> None:
            """
            Accept the plugin path argument plugins take.

            Example:
                >>> Plugin(None).quick
                True


            :param path: Unused.
            :return: None.
            """
            self.quick = True

        def get_metadata(self, stream: object, file_format: str) -> object:
            """
            Return the half-converted object.

            Example:
                >>> type(Plugin(None).get_metadata(None, "rb")).__name__
                'CalibreLikeLiuXinBookMetaData'


            :param stream: Unused.
            :param file_format: Unused.
            :return: A half-converted object.
            """
            return CalibreLikeLiuXinBookMetaData("Pool Test", ["Jane Doe"])

    book = tmp_path / "book.rb"
    book.write_bytes(b"rb")
    with pytest.raises(MetadataModelError, match="expected calibre metadata"):
        adapt_fork_calibre_reader(Plugin)().read_calibre_metadata(
            book, "rb", context(tmp_path)
        )


def test_liuxin_providers_place_the_hybrid_by_level(tmp_path: Path) -> None:
    """
    Place an FB2 reading's title, creator, language, publisher and ISBN.

    Example:
        >>> test_liuxin_providers_place_the_hybrid_by_level(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after each level is checked.
    """
    path = sample(tmp_path, "fb2")
    fields = adapt_liuxin_reader(plugin_for("fb2"))().read_wemi_metadata(
        path, "fb2", context(tmp_path)
    )
    work, expression, manifestation = (
        fields.levels["work"],
        fields.levels["expression"],
        fields.levels["manifestation"],
    )
    assert work["titles"] == ("Pool Test",)
    assert work["creators"] == ({"name": "Jane Doe", "role": "authors"},)
    assert expression["languages"] == ("en",)
    assert manifestation["publishers"] == ("Pub Co",)
    assert dict(manifestation["identifiers"]) == {"isbn": ("9780000000002",)}


def test_liuxin_providers_keep_what_calibre_conversion_loses(tmp_path: Path) -> None:
    """
    Keep creator roles and identifiers the hybrid's calibre conversion drops.

    The PDF fixture names producers, and the HTMLZ sample's ISBN is only in
    the half-converted object's identifiers.

    Example:
        >>> test_liuxin_providers_keep_what_calibre_conversion_loses(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after both readings are checked.
    """
    pdf = adapt_liuxin_reader(plugin_for("pdf"))().read_wemi_metadata(
        PDF_FIXTURE, "pdf", context(tmp_path)
    )
    producers = [
        person["name"]
        for person in pdf.levels["manifestation"]["creators"]
        if person["role"] == "producers"
    ]
    assert producers
    assert dict(pdf.levels["manifestation"]["identifiers"])["doi"] == ("10.1000/182",)

    htmlz = adapt_liuxin_reader(plugin_for("htmlz"))().read_wemi_metadata(
        sample(tmp_path, "htmlz"), "htmlz", context(tmp_path)
    )
    assert dict(htmlz.levels["manifestation"]["identifiers"]) == {
        "isbn": ("9780000000002",)
    }


def test_liuxin_providers_place_calibre_results_too(tmp_path: Path) -> None:
    """
    Place a reader's result by level when the plugin returns calibre metadata.

    Example:
        >>> test_liuxin_providers_place_calibre_results_too(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the OPF reading is checked.
    """
    fields = adapt_liuxin_reader(plugin_for("opf"))().read_wemi_metadata(
        sample(tmp_path, "opf"), "opf", context(tmp_path)
    )
    assert fields.levels["work"]["tags"] == ("Fiction",)
    assert "subjects" not in fields.levels["work"]
    assert fields.levels["manifestation"]["publishers"] == ("Pub Co",)


def test_one_read_answers_metadata_and_cover(tmp_path: Path) -> None:
    """
    Run the reader once for the metadata and the cover of an unchanged file.

    Example:
        >>> test_one_read_answers_metadata_and_cover(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after the read count is checked.
    """
    calls: list[str] = []

    class Plugin:
        """
        Count reads and return calibre metadata with a cover.

        Example:
            >>> Plugin.name
            'Read TXT metadata'
        """

        name = "Read TXT metadata"
        file_types = frozenset({"txt"})

        def __init__(self, path: object) -> None:
            """
            Accept the plugin path argument plugins take.

            Example:
                >>> Plugin(None).quick
                True


            :param path: Unused.
            :return: None.
            """
            self.quick = True

        def get_metadata(self, stream: object, file_format: str) -> object:
            """
            Record the read and return metadata with a cover.

            Example:
                >>> Plugin(None).get_metadata(None, "txt").title
                'Counted'


            :param stream: Unused.
            :param file_format: The format asked for.
            :return: calibre metadata.
            """
            calls.append(file_format)
            metadata = calibreMetadata("Counted", ["Ann"])
            metadata.cover_data = ("png", b"\x89PNG")
            return metadata

    book = tmp_path / "book.txt"
    book.write_text("x")
    for adapter in (adapt_fork_calibre_reader, adapt_liuxin_reader):
        calls.clear()
        provider = adapter(Plugin)()
        call = context(tmp_path)
        if isinstance(provider, ForkCalibreReader):
            provider.read_calibre_metadata(book, "txt", call)
        else:
            provider.read_wemi_metadata(book, "txt", call)
        cover = provider.extract_cover(book, "txt", call)
        assert cover is not None and cover.data == b"\x89PNG"
        assert cover.image_format == "png"
        assert calls == ["txt"]


def test_the_mapping_places_each_field() -> None:
    """
    Place calibre fields by level, keeping unknown fields as unplaced.

    Example:
        >>> test_the_mapping_places_each_field()


    :return: None after each level is checked.
    """
    fields = wemi_from_calibre(
        {
            "title": "A Book",
            "title_sort": "Book, A",
            "authors": ["Ann", "Bob"],
            "author_sort_map": {"Ann": "A, Ann"},
            "author_link_map": {"Bob": "https://example.org/bob"},
            "book_producer": "Maker",
            "series": "Saga",
            "series_index": 2.0,
            "tags": ["Fiction"],
            "languages": ["en"],
            "comments": "<p>Blurb</p>",
            "publisher": "Pub Co",
            "pubdate": "2026-11-17T00:00:00+00:00",
            "identifiers": {"isbn": "978"},
            "rights": "CC-BY",
            "timestamp": "2026-11-17T00:00:00+00:00",
            "rating": 8,
            "uuid": "",
        },
        identifiers={"isbn": ["978", "979"], "doi": ["10.1/x"]},
    )
    work = fields.levels["work"]
    assert work["titles"] == ("A Book",)
    assert work["title_sort"] == "Book, A"
    assert work["series"] == ({"name": "Saga", "index": 2.0},)
    assert work["tags"] == ("Fiction",)
    assert "subjects" not in work
    assert work["creators"] == (
        {"name": "Ann", "role": "authors", "sort": "A, Ann"},
        {"name": "Bob", "role": "authors", "link": "https://example.org/bob"},
    )
    assert fields.levels["expression"] == {
        "languages": ("en",),
        "description": "<p>Blurb</p>",
    }
    manifestation = fields.levels["manifestation"]
    assert manifestation["publishers"] == ("Pub Co",)
    assert manifestation["publication_date"] == "2026-11-17T00:00:00+00:00"
    assert dict(manifestation["identifiers"]) == {
        "isbn": ("978", "979"),
        "doi": ("10.1/x",),
    }
    assert manifestation["creators"] == ({"name": "Maker", "role": "book_producer"},)
    assert dict(manifestation["unplaced"]) == {
        "timestamp": "2026-11-17T00:00:00+00:00",
        "rating": 8,
    }
    assert "item" not in fields.levels


def test_the_mapping_places_creators_by_role() -> None:
    """
    Put each creator role at its level, and an unknown role at the Expression.

    Example:
        >>> test_the_mapping_places_creators_by_role()


    :return: None after each role is checked.
    """
    fields = wemi_from_calibre(
        {"authors": ["Ignored"]},
        creators={
            "authors": ["Ann"],
            "translators": ["Tom"],
            "cover_artists": ["Cat"],
            "narrators": ["Ned"],
            "editors": [],
        },
    )
    roles = {
        level: [(person["name"], person["role"]) for person in found["creators"]]
        for level, found in fields.levels.items()
    }
    assert roles == {
        "work": [("Ann", "authors")],
        "expression": [("Tom", "translators"), ("Ned", "narrators")],
        "manifestation": [("Cat", "cover_artists")],
    }


@pytest.mark.parametrize("module", [fork_metadata, wemi_mapping])
def test_examples_in_the_modules_run(module: Any) -> None:
    """
    Run the docstring examples of the adapter and mapping modules.

    Example:
        >>> test_examples_in_the_modules_run(wemi_mapping)


    :param module: The module to check.
    :return: None after every example passes.
    """
    result = doctest.testmod(module, report=False)
    assert result.attempted > 0
    assert result.failed == 0
