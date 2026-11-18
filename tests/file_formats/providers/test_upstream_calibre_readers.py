"""
Check the providers that read metadata with an installed calibre.

calibre is not installed where these tests run. A stand-in ``calibre-debug``
runs the real probe script against a small fake ``calibre`` package, so the
whole path is exercised: finding the program, copying the probe to an empty
folder, the request and result files, calibre's serialisation, and turning
the result into fields and a cover.
"""

from __future__ import annotations

import doctest
import importlib
import logging
import sys
import textwrap
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.conversion.report import ConversionReport
from LiuXin_alpha.file_formats.providers import upstream_calibre
from LiuXin_alpha.file_formats.providers.base import ProviderContext
from LiuXin_alpha.file_formats.providers.metadata import (
    CalibreMetadataReader,
    CoverExtractor,
)
from LiuXin_alpha.file_formats.providers.registry import (
    ProviderRegistry,
    capabilities_of,
)
from LiuXin_alpha.file_formats.providers.upstream_calibre import (
    CALIBRE_DEBUG_VARIABLE,
    UPSTREAM_PRIORITY,
    UPSTREAM_READERS,
    CalibreRunError,
    UpstreamCalibreReader,
    adapt_upstream_reader,
    find_calibre_debug,
    register_upstream_calibre_readers,
)
from tests.support.format_providers import declare

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="the stand-in calibre-debug is a POSIX script"
)

COVER_BYTES = b"\xff\xd8\xff\xe0fake jpeg"

FAKE_MODULES: dict[str, str] = {
    "calibre/__init__.py": "",
    "calibre/constants.py": '__version__ = "9.18.0-test"\n',
    "calibre/customize/__init__.py": "",
    "calibre/customize/ui.py": """
        import os

        KNOWN = {"Read EPUB metadata", "Read RTF metadata"}

        class FakeMetadata:
            def __init__(self, title, plugin, quick, size):
                self.title = title
                self.plugin = plugin
                self.quick = quick
                self.size = size

        class FakePlugin:
            quick = True

            def __init__(self, name):
                self.name = name

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def get_metadata(self, stream, ftype):
                counter = os.environ.get("FAKE_CALIBRE_COUNTER")
                if counter:
                    with open(counter, "a") as handle:
                        handle.write("run\\n")
                data = stream.read()
                if data.startswith(b"FAIL"):
                    raise ValueError("bad file for " + ftype)
                if data.startswith(b"NONE"):
                    return None
                return FakeMetadata(data.decode("utf-8").strip(), self.name, self.quick, len(data))

        def find_plugin(name):
            return FakePlugin(name) if name in KNOWN else None
    """,
    "calibre/ebooks/__init__.py": "",
    "calibre/ebooks/metadata/__init__.py": "",
    "calibre/ebooks/metadata/book/__init__.py": "",
    "calibre/ebooks/metadata/book/serialize.py": f"""
        import base64
        import datetime

        def metadata_as_dict(mi, encode_cover_data=False):
            cover = {COVER_BYTES!r}
            return {{
                "title": mi.title,
                "authors": ["José", "Иван"],
                "pubdate": datetime.datetime(2026, 11, 17, tzinfo=datetime.timezone.utc),
                "tags": {{"b", "a"}},
                "identifiers": {{"isbn": "9780000000002"}},
                "rating": float("nan"),
                "comments": b"bytes \\xe2\\x80\\x94 text",
                "plugin": mi.plugin,
                "quick": mi.quick,
                "cover_data": (
                    ["jpeg", base64.b64encode(cover).decode("ascii")]
                    if encode_cover_data
                    else ("jpeg", cover)
                ),
            }}
    """,
}


def write_fake_calibre(root: Path, *, behaviour: str = "run") -> Path:
    """
    Write a fake calibre package and a stand-in ``calibre-debug`` program.

    Example:
        >>> write_fake_calibre(Path("/tmp/x"))  # doctest: +SKIP
        PosixPath('/tmp/x/bin/calibre-debug')


    :param root: An empty folder to write into.
    :param behaviour: ``run`` to run the probe, ``crash`` to exit without a
        result, or ``hang`` to sleep for a minute.
    :return: The stand-in program's path.
    """
    package_root = root / "fake-calibre"
    for name, source in FAKE_MODULES.items():
        target = package_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(source), encoding="utf-8")
    program = root / "bin" / "calibre-debug"
    program.parent.mkdir(parents=True)
    program.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import os, runpy, sys, time
            behaviour = {behaviour!r}
            if behaviour == "crash":
                sys.stderr.write("calibre crashed while starting\\n")
                sys.exit(3)
            if behaviour == "hang":
                time.sleep(60)
            option, script, *rest = sys.argv[1:]
            assert option == "-e", option
            log = os.environ.get("FAKE_CALIBRE_FOLDERS")
            if log:
                folder = os.path.dirname(os.path.abspath(script))
                with open(log, "a") as handle:
                    handle.write(",".join(sorted(os.listdir(folder))) + "\\n")
            sys.path.insert(0, {str(package_root)!r})
            sys.path.insert(0, os.path.dirname(os.path.abspath(script)))
            sys.argv = [script, *rest]
            runpy.run_path(script, run_name="__main__")
            """
        ),
        encoding="utf-8",
    )
    program.chmod(0o755)
    return program


@pytest.fixture(autouse=True)
def empty_cache() -> None:
    """
    Start every test with no remembered calibre answers.

    Example:
        >>> "empty_cache" in globals()
        True


    :return: None after the cache is emptied.
    """
    upstream_calibre._cache.clear()


@pytest.fixture
def calibre(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    Install the stand-in calibre and count how often it reads a file.

    Example:
        >>> "calibre" in globals()
        True


    :param tmp_path: Pytest scratch folder.
    :param monkeypatch: Pytest helper for environment variables.
    :return: The file the stand-in appends a line to for every read.
    """
    program = write_fake_calibre(tmp_path / "calibre")
    counter = tmp_path / "runs.txt"
    monkeypatch.setenv(CALIBRE_DEBUG_VARIABLE, str(program))
    monkeypatch.setenv("FAKE_CALIBRE_COUNTER", str(counter))
    monkeypatch.setenv("FAKE_CALIBRE_FOLDERS", str(tmp_path / "folders.txt"))
    return counter


def context(work_dir: Path) -> ProviderContext:
    """
    Build a provider context with a fresh report.

    Example:
        >>> context(Path.cwd()).options
        {}


    :param work_dir: The call's work folder.
    :return: The context.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    return ProviderContext(
        work_dir=work_dir,
        options={},
        log=logging.getLogger("tests.upstream-calibre"),
        report=ConversionReport(),
    )


def epub_reader() -> type[UpstreamCalibreReader]:
    """
    Make the provider for calibre's EPUB reader.

    Example:
        >>> epub_reader().calibre_plugin_name
        'Read EPUB metadata'


    :return: The provider class.
    """
    return adapt_upstream_reader("Read EPUB metadata", frozenset({"epub", "kepub"}))


def runs(counter: Path) -> int:
    """
    Count the reads the stand-in calibre has made.

    Example:
        >>> runs(Path("/no/such/file"))
        0


    :param counter: The counter file.
    :return: The number of reads.
    """
    return len(counter.read_text().splitlines()) if counter.exists() else 0


def test_every_upstream_reader_is_a_provider() -> None:
    """
    Register one provider per calibre 9.18 reader, each also a cover extractor.

    Example:
        >>> test_every_upstream_reader_is_a_provider()


    :return: None after every provider is checked.
    """
    registry = ProviderRegistry()
    made = register_upstream_calibre_readers(registry)
    assert len(made) == len(UPSTREAM_READERS) == 25
    assert len({provider.provider_id for provider in made}) == 25
    for (name, formats), provider in zip(UPSTREAM_READERS, made, strict=True):
        assert provider.calibre_plugin_name == name
        assert provider.calibre_read_formats == provider.cover_source_formats == formats
        assert provider.priority == UPSTREAM_PRIORITY
        assert provider.requires == ("calibre",)
        assert capabilities_of(provider) == (CalibreMetadataReader, CoverExtractor)
        for file_format in formats:
            assert provider in registry.calibre_metadata_readers(file_format)


def test_a_missing_calibre_makes_the_providers_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Report calibre as missing, and refuse to read, when it cannot be found.

    Example:
        >>> test_a_missing_calibre_makes_the_providers_unavailable(Path.cwd(), None)  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param monkeypatch: Pytest helper for environment variables.
    :return: None after availability and reading are checked.
    """
    monkeypatch.setenv(CALIBRE_DEBUG_VARIABLE, str(tmp_path / "missing"))
    provider = epub_reader()
    availability = provider.availability()
    assert not availability.available
    assert CALIBRE_DEBUG_VARIABLE in str(availability.reason)
    book = tmp_path / "book.epub"
    book.write_text("A Book")
    with pytest.raises(CalibreRunError, match="calibre-debug was not found"):
        provider().read_calibre_metadata(book, "epub", context(tmp_path / "work"))


def test_calibre_debug_is_found_by_variable_then_path(tmp_path: Path) -> None:
    """
    Prefer the named program, then ``calibre-debug`` on the ``PATH``.

    Example:
        >>> test_calibre_debug_is_found_by_variable_then_path(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :return: None after each lookup is checked.
    """
    on_path = write_fake_calibre(tmp_path / "on-path")
    named = write_fake_calibre(tmp_path / "named")
    path = str(on_path.parent)
    assert find_calibre_debug({"PATH": path}, "linux") == on_path
    assert (
        find_calibre_debug({"PATH": path, CALIBRE_DEBUG_VARIABLE: str(named)}) == named
    )
    missing = {"PATH": path, CALIBRE_DEBUG_VARIABLE: str(tmp_path / "nothing")}
    assert find_calibre_debug(missing) is None
    assert find_calibre_debug({"PATH": ""}, "linux") is None


def test_a_reading_is_calibres_own_serialisation(tmp_path: Path, calibre: Path) -> None:
    """
    Return calibre's fields with dates as text, sets sorted and the cover apart.

    Example:
        >>> test_a_reading_is_calibres_own_serialisation(Path.cwd(), Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param calibre: The stand-in calibre's run counter.
    :return: None after the fields and the report are checked.
    """
    book = tmp_path / "book.epub"
    book.write_text("A Book", encoding="utf-8")
    call = context(tmp_path / "work")
    fields = epub_reader()().read_calibre_metadata(book, "epub", call)

    assert fields.values["title"] == "A Book"
    assert fields.values["authors"] == ("José", "Иван")
    assert fields.values["pubdate"] == "2026-11-17T00:00:00+00:00"
    assert fields.values["tags"] == ("a", "b")
    assert fields.values["plugin"] == "Read EPUB metadata"
    assert fields.values["quick"] is False
    assert fields.values["rating"] == "nan"
    assert fields.values["comments"] == "bytes — text"
    assert "cover_data" not in fields.values
    warnings = " ".join(call.report.to_mapping()["warnings"])
    assert "rating" in warnings and "comments" in warnings
    assert runs(calibre) == 1


def test_the_probe_runs_from_a_folder_of_its_own(tmp_path: Path, calibre: Path) -> None:
    """
    Give calibre a folder holding only the probe and its request.

    ``calibre-debug`` puts that folder first on ``sys.path``; nothing of
    LiuXin's may be beside the script.

    Example:
        >>> test_the_probe_runs_from_a_folder_of_its_own(Path.cwd(), Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param calibre: The stand-in calibre's run counter.
    :return: None after the folder's contents are checked.
    """
    book = tmp_path / "book.epub"
    book.write_text("A Book", encoding="utf-8")
    epub_reader()().read_calibre_metadata(book, "epub", context(tmp_path / "work"))
    listing = (tmp_path / "folders.txt").read_text().strip()
    assert listing == "liuxin_calibre_metadata_probe.py,request.json"


def test_the_cover_comes_from_the_same_run(tmp_path: Path, calibre: Path) -> None:
    """
    Read the metadata and then the cover of a file with one calibre run.

    Example:
        >>> test_the_cover_comes_from_the_same_run(Path.cwd(), Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param calibre: The stand-in calibre's run counter.
    :return: None after the cover and the run count are checked.
    """
    book = tmp_path / "book.epub"
    book.write_text("A Book", encoding="utf-8")
    provider = epub_reader()()
    call = context(tmp_path / "work")
    provider.read_calibre_metadata(book, "epub", call)
    cover = provider.extract_cover(book, "epub", call)
    assert cover is not None
    assert cover.data == COVER_BYTES and cover.image_format == "jpeg"
    assert runs(calibre) == 1

    book.write_text("A Changed Book", encoding="utf-8")
    fields = provider.read_calibre_metadata(book, "epub", call)
    assert fields.values["title"] == "A Changed Book"
    assert runs(calibre) == 2


def test_a_reader_that_finds_nothing_gives_empty_fields(
    tmp_path: Path, calibre: Path
) -> None:
    """
    Return empty fields and no cover when calibre's reader returns nothing.

    Example:
        >>> test_a_reader_that_finds_nothing_gives_empty_fields(Path.cwd(), Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param calibre: The stand-in calibre's run counter.
    :return: None after both results are checked.
    """
    book = tmp_path / "book.epub"
    book.write_bytes(b"NONE")
    provider = epub_reader()()
    call = context(tmp_path / "work")
    assert dict(provider.read_calibre_metadata(book, "epub", call).values) == {}
    assert provider.extract_cover(book, "epub", call) is None


@pytest.mark.parametrize(
    ("plugin", "content", "message"),
    [
        ("Read EPUB metadata", b"FAIL", "bad file for epub"),
        ("Read PDF metadata", b"A Book", "has no plugin named 'Read PDF metadata'"),
    ],
)
def test_calibres_errors_are_reported_with_its_traceback(
    tmp_path: Path, calibre: Path, plugin: str, content: bytes, message: str
) -> None:
    """
    Raise with calibre's own explanation when its reader fails or is missing.

    Example:
        >>> test_calibres_errors_are_reported_with_its_traceback(Path.cwd(), Path.cwd(), "", b"", "")  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param calibre: The stand-in calibre's run counter.
    :param plugin: The calibre plugin to ask for.
    :param content: The file's bytes.
    :param message: Part of the expected error message.
    :return: None after the error is checked.
    """
    book = tmp_path / "book.pdf"
    book.write_bytes(content)
    provider = adapt_upstream_reader(plugin, frozenset({"epub", "pdf"}))
    with pytest.raises(CalibreRunError, match=message):
        provider().read_calibre_metadata(book, "epub", context(tmp_path / "work"))


@pytest.mark.parametrize(
    ("behaviour", "timeout", "message"),
    [
        ("crash", 30.0, "status 3 without a result:\ncalibre crashed"),
        ("hang", 1.0, "longer than 1 seconds"),
    ],
)
def test_a_failed_calibre_process_is_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    behaviour: str,
    timeout: float,
    message: str,
) -> None:
    """
    Raise when calibre exits without a result or does not finish in time.

    Example:
        >>> test_a_failed_calibre_process_is_reported(Path.cwd(), None, "", 0, "")  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder.
    :param monkeypatch: Pytest helper for environment variables.
    :param behaviour: How the stand-in calibre misbehaves.
    :param timeout: Seconds the provider waits.
    :param message: Part of the expected error message.
    :return: None after the error is checked.
    """
    program = write_fake_calibre(tmp_path / "calibre", behaviour=behaviour)
    monkeypatch.setenv(CALIBRE_DEBUG_VARIABLE, str(program))
    book = tmp_path / "book.epub"
    book.write_text("A Book", encoding="utf-8")
    impatient = declare("Impatient", epub_reader(), calibre_timeout=timeout)
    assert issubclass(impatient, UpstreamCalibreReader)
    with pytest.raises(CalibreRunError, match=message):
        impatient().read_calibre_metadata(book, "epub", context(tmp_path / "work"))


def test_the_probe_imports_without_calibre() -> None:
    """
    Import the probe module where calibre is absent, as LiuXin does.

    Example:
        >>> test_the_probe_imports_without_calibre()


    :return: None after the module is imported and its examples run.
    """
    probe = importlib.import_module("LiuXin_alpha.file_formats.calibre_metadata_probe")
    assert "calibre" not in sys.modules
    assert doctest.testmod(probe, report=False).failed == 0
    assert doctest.testmod(upstream_calibre, report=False).failed == 0
