"""
Check the OPF reader ported from upstream calibre, and the helpers it brings.

calibre's own OPF 2, OPF 3 and author-sort tests run on the port. The Python
stand-ins for calibre's C functions are checked against answers taken from
calibre's compiled C, and the language, country and media-type tables against
calibre's.
"""

from __future__ import annotations

import datetime
import doctest
import importlib
import logging
import mimetypes
import time
import unittest
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import fork_metadata
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.ported_calibre import adapt_ported_reader
from LiuXin_alpha.metadata.upstream_calibre import _compat, _speedups, config_base, meta

OPF2 = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Field Test</dc:title>
  <dc:creator opf:role="aut" opf:file-as="Doe, Jane">Jane Doe</dc:creator>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:language>fr</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:subject>Fiction</dc:subject>
  <dc:description>A blurb.</dc:description>
  <dc:date>2001-02-03</dc:date>
  <dc:rights>CC-BY</dc:rights>
  <meta name="calibre:series" content="Saga"/>
  <meta name="calibre:series_index" content="2"/>
  <meta name="calibre:rating" content="8"/>
 </metadata>
 <manifest/>
 <spine/>
</package>
"""

OPF3 = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id"
         prefix="calibre: https://calibre-ebook.com">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
  <dc:title>Three Test</dc:title>
  <dc:creator id="c1">Jane Doe</dc:creator>
  <meta refines="#c1" property="role" scheme="marc:relators">aut</meta>
  <meta refines="#c1" property="file-as">Doe, Jane</meta>
  <dc:creator id="c2">Tom Translator</dc:creator>
  <meta refines="#c2" property="role" scheme="marc:relators">trl</meta>
  <dc:identifier id="id">urn:isbn:9780000000002</dc:identifier>
  <dc:language>de</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:subject>Fiction</dc:subject>
  <dc:date>2001-02-03</dc:date>
  <meta property="belongs-to-collection" id="s">Saga</meta>
  <meta refines="#s" property="collection-type">series</meta>
  <meta refines="#s" property="group-position">3</meta>
  <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
 </metadata>
 <manifest/>
 <spine/>
</package>
"""


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
        log=logging.getLogger("tests.ported-opf"),
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


def read_opf(tmp_path: Path, text: str) -> dict[str, Any]:
    """
    Read OPF text with the ported reader's provider.

    Example:
        >>> read_opf(Path("/tmp"), OPF2)["title"]  # doctest: +SKIP
        'Field Test'


    :param tmp_path: Folder for the file.
    :param text: The OPF.
    :return: The fields, in calibre's names and shapes.
    """
    path = tmp_path / "book.opf"
    path.write_text(text, encoding="utf-8")
    provider = adapt_ported_reader("Read OPF metadata")()
    return dict(provider.read_calibre_metadata(path, "opf", context(tmp_path)).values)


@pytest.mark.parametrize(
    ("module", "suite"),
    [
        ("opf2", "suite"),
        ("opf3_test", "suite"),
        ("author_sort_test", "find_tests"),
    ],
)
def test_calibres_own_tests_pass_on_the_port(module: str, suite: str) -> None:
    """
    Run calibre's OPF 2, OPF 3 and author-sort tests against the ported code.

    Example:
        >>> test_calibres_own_tests_pass_on_the_port("opf2", "suite")  # doctest: +SKIP


    :param module: The ported module holding the tests.
    :param suite: The function that builds its suite.
    :return: None; the assertions are the check.
    """
    tests = getattr(
        importlib.import_module(f"LiuXin_alpha.metadata.upstream_calibre.{module}"),
        suite,
    )()
    result = unittest.TestResult()
    tests.run(result)

    problems = [
        f"{test.id()}: {trace}" for test, trace in result.failures + result.errors
    ]
    assert result.testsRun > 0
    assert not problems, "\n".join(problems)


def test_opf2_is_read_as_calibre_reads_it(tmp_path: Path) -> None:
    """
    Read every field of an OPF 2 package, languages as ISO 639-3 codes.

    Example:
        >>> test_opf2_is_read_as_calibre_reads_it(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    values = read_opf(tmp_path, OPF2)

    assert values["title"] == "Field Test"
    assert values["authors"] == ("Jane Doe",)
    assert values["author_sort"] == "Doe, Jane"
    assert values["identifiers"] == {"isbn": "9780000000002"}
    assert values["languages"] == ("fra",)
    assert values["publisher"] == "Pub Co"
    assert values["tags"] == ("Fiction",)
    assert values["comments"] == "A blurb."
    assert values["rights"] == "CC-BY"
    assert (values["series"], values["series_index"]) == ("Saga", 2.0)
    assert values["rating"] == 8.0
    assert values["pubdate"].startswith("2001-02-03")


def test_opf3_refinements_are_read(tmp_path: Path) -> None:
    """
    Apply OPF 3 refinements: roles, file-as and series collections.

    Example:
        >>> test_opf3_refinements_are_read(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    values = read_opf(tmp_path, OPF3)

    # calibre keeps only authors; the translator is not an author.
    assert values["authors"] == ("Jane Doe",)
    assert values["author_sort"] == "Doe, Jane"
    assert values["languages"] == ("deu",)
    assert (values["series"], values["series_index"]) == ("Saga", 3.0)
    assert values["identifiers"] == {"isbn": "9780000000002"}


def test_external_entities_are_not_resolved(tmp_path: Path) -> None:
    """
    Leave an external entity unresolved, so an OPF cannot read local files.

    Example:
        >>> test_external_entities_are_not_resolved(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the sample and the secret.
    :return: None; the assertions are the check.
    """
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET", encoding="utf-8")
    text = OPF2.replace(
        "<?xml version='1.0' encoding='utf-8'?>",
        f"<?xml version='1.0' encoding='utf-8'?>\n"
        f'<!DOCTYPE package [<!ENTITY s SYSTEM "file://{secret.as_posix()}">]>',
    ).replace("A blurb.", "&s;")

    values = read_opf(tmp_path, text)

    assert values["title"] == "Field Test"
    assert "comments" not in values
    assert "TOP SECRET" not in repr(values)


def test_entity_expansion_is_bounded(tmp_path: Path) -> None:
    """
    Read an OPF with nested entity definitions quickly, leaving them unexpanded.

    Example:
        >>> test_entity_expansion_is_bounded(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: Folder for the sample.
    :return: None; the assertions are the check.
    """
    entities = ['<!ENTITY a0 "lol">'] + [
        f'<!ENTITY a{i} "{f"&a{i - 1};" * 10}">' for i in range(1, 10)
    ]
    text = OPF2.replace(
        "<?xml version='1.0' encoding='utf-8'?>",
        "<?xml version='1.0' encoding='utf-8'?>\n<!DOCTYPE package ["
        + "".join(entities)
        + "]>",
    ).replace("A blurb.", "&a9;")

    start = time.monotonic()
    values = read_opf(tmp_path, text)

    assert time.monotonic() - start < 10
    assert values["title"] == "Field Test"
    assert "comments" not in values


# Answers from calibre 9.18's compiled speedup.parse_iso8601.
ISO8601_FROM_CALIBRE_C: list[tuple[str, tuple[datetime.datetime, bool, int] | None]] = [
    ("2001", (datetime.datetime(2001, 1, 1), False, 0)),
    ("2001-02", (datetime.datetime(2001, 2, 1), False, 0)),
    ("20010203", (datetime.datetime(2001, 2, 3), False, 0)),
    ("2001-02-03T04", (datetime.datetime(2001, 2, 3, 4), False, 0)),
    ("2001-02-03 04:05:06", (datetime.datetime(2001, 2, 3, 4, 5, 6), False, 0)),
    (
        "2001-02-03T04:05:06.7",
        (datetime.datetime(2001, 2, 3, 4, 5, 6, 700000), False, 0),
    ),
    (
        "2001-02-03T04:05:06,123456789",
        (datetime.datetime(2001, 2, 3, 4, 5, 6, 123456), False, 0),
    ),
    ("2001-02-03T04:05:06Z", (datetime.datetime(2001, 2, 3, 4, 5, 6), True, 0)),
    (
        "2001-02-03T04:05:06+05:30",
        (datetime.datetime(2001, 2, 3, 4, 5, 6), True, 19800),
    ),
    (
        "2001-02-03T04:05:06-0800",
        (datetime.datetime(2001, 2, 3, 4, 5, 6), True, -28800),
    ),
    ("2001-00-05", (datetime.datetime(2001, 1, 1), True, -18000)),
    (" \t2001-02-03", (datetime.datetime(2001, 2, 3), False, 0)),
    ("2001-2-3", (datetime.datetime(2001, 2, 3), False, 0)),
    ("2001-02-03+01", (datetime.datetime(2001, 2, 3), True, 3600)),
    ("2001-02-03T04:05Zjunk", (datetime.datetime(2001, 2, 3, 4, 5), True, 0)),
    ("2001-02-03Tx", None),
    ("2001-13-01", None),
    ("99", None),
    ("", None),
    ("2001-02-30", None),
    ("2001-02-03T25:00", None),
    ("0000-01-01", None),
]


@pytest.mark.parametrize(("raw", "expected"), ISO8601_FROM_CALIBRE_C)
def test_parse_iso8601_matches_calibres_c(
    raw: str, expected: tuple[datetime.datetime, bool, int] | None
) -> None:
    """
    Parse each date as calibre's compiled C does, quirks included.

    Example:
        >>> test_parse_iso8601_matches_calibres_c("2001", (datetime.datetime(2001, 1, 1), False, 0))


    :param raw: The date string.
    :param expected: calibre's answer, or ``None`` where it raises ValueError.
    :return: None; the assertions are the check.
    """
    if expected is None:
        with pytest.raises(ValueError):
            _speedups.parse_iso8601(raw)
    else:
        assert _speedups.parse_iso8601(raw) == expected


def test_clean_xml_chars_keeps_only_what_xml_allows() -> None:
    """
    Drop control characters, surrogates and non-characters, as calibre's C does.

    Example:
        >>> test_clean_xml_chars_keeps_only_what_xml_allows()


    :return: None; the assertions are the check.
    """
    raw = "a\x00\x08\t\n\x0b\r\x1f \x7e\x7f\x84\x85\x86\xa0\ud800﷐ﷰ￾\U0001f600"
    assert _speedups.clean_xml_chars(raw) == "a\t\n\r \x7e\x85\xa0ﷰ\U0001f600"
    with pytest.raises(TypeError):
        _speedups.clean_xml_chars(b"x")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("raw", "keep", "expected"),
    [
        (b"caf&eacute; &amp; &#x3C;", False, b"caf\xc3\xa9 & <"),
        (b"caf&eacute; &amp; &#x3C;", True, b"caf\xc3\xa9 &amp; &#x3C;"),
        (b"\xff&copy;\xfe", False, b"\xff\xc2\xa9\xfe"),
        ("&quot;&#39;&apos;", True, "&quot;&#39;&apos;"),
        ("&nGt;", False, "≫⃒"),
    ],
)
def test_replace_all_entities_matches_calibres_c(
    raw: Any, keep: bool, expected: Any
) -> None:
    """
    Replace entities in bytes and text, keeping XML's own when asked, as the C does.

    Example:
        >>> test_replace_all_entities_matches_calibres_c("&amp;", False, "&")


    :param raw: The input.
    :param keep: Whether to keep references to ``< > & " '``.
    :param expected: calibre's answer.
    :return: None; the assertion is the check.
    """
    assert _compat.replace_all_entities(raw, keep) == expected


def test_language_and_country_codes_follow_calibres_rules() -> None:
    """
    Turn codes and English names into ISO 639-3, and tags into language and country.

    Example:
        >>> test_language_and_country_codes_follow_calibres_rules()


    :return: None; the assertions are the check.
    """
    assert _compat.canonicalize_lang("en-GB") == "eng"
    assert _compat.canonicalize_lang("Greek, Ancient (to 1453)") == "grc"
    assert _compat.canonicalize_lang("und") == "und"
    assert _compat.canonicalize_lang("zz") is None
    assert _compat.lang_as_iso639_1("deu") == "de"
    assert tuple(_compat.parse_lang_code("pt-BRA")) == ("por", "BR")
    with pytest.raises(ValueError):
        _compat.parse_lang_code("zz")


def test_media_types_come_from_calibres_table_privately() -> None:
    """
    Guess media types from calibre's table without changing Python's global one.

    Example:
        >>> test_media_types_come_from_calibres_table_privately()


    :return: None; the assertions are the check.
    """
    before = dict(mimetypes.types_map)

    assert _compat.guess_type("toc.ncx")[0] == "application/x-dtbncx+xml"
    assert _compat.guess_type("book.azw3")[0] == "application/x-mobi8-ebook"
    assert _compat.guess_type("cover.avif")[0] == "image/avif"
    assert dict(mimetypes.types_map) == before


def test_tweaks_are_calibres_defaults() -> None:
    """
    Read calibre's default tweaks file.

    Example:
        >>> test_tweaks_are_calibres_defaults()


    :return: None; the assertions are the check.
    """
    assert config_base.tweaks["author_sort_copy_method"] == "comma"
    assert config_base.tweaks["title_series_sorting"] == "library_order"
    assert config_base.tweaks["authors_split_regex"] == r"(?i),?\s+(and|with)\s+"


def test_examples_in_the_helpers_run() -> None:
    """
    Run the docstring examples of the modules written for the port.

    Example:
        >>> test_examples_in_the_helpers_run()


    :return: None; the assertions are the check.
    """
    for module in (_compat, _speedups, config_base, meta):
        result = doctest.testmod(module, report=False)
        assert result.attempted > 0
        assert result.failed == 0, module.__name__


PACKAGE_DIR = Path(_compat.__file__).parent


@pytest.mark.parametrize(
    "source", sorted(PACKAGE_DIR.glob("*.py")), ids=lambda path: path.name
)
def test_no_ported_module_imports_calibre(source: Path) -> None:
    """
    Keep every module in the port free of imports from calibre itself.

    Only Qt and html5-parser imports remain, inside functions the readers do
    not call.

    Example:
        >>> test_no_ported_module_imports_calibre(PACKAGE_DIR / "opf.py")


    :param source: A module of the package.
    :return: None; the assertion is the check.
    """
    import ast

    tree = ast.parse(source.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            roots.add((node.module or "").partition(".")[0])
        elif isinstance(node, ast.Import):
            roots.update(alias.name.partition(".")[0] for alias in node.names)
    assert not roots & {
        "calibre",
        "calibre_extensions",
        "polyglot",
        "tzlocal",
        "compression",
    }
