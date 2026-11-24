"""
Verify RTF metadata and encoding cleanup.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test rtf metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py
"""
from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.book.base import calibreMetaInformation


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first(raw):
    """
    Perform the first test-helper operation with deterministic inputs.

    Example:
        Exercise first through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _snapshot(md) -> dict:
    """
    Perform the snapshot test-helper operation with deterministic inputs.

    Example:
        Exercise snapshot through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param md: Value supplied for md in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    identifiers = {}
    try:
        identifiers = {str(k): sorted(str(v) for v in vals) for k, vals in (md.get_identifiers() or {}).items()}
    except Exception:
        identifiers = {}
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
        "comments": sorted(_values(getattr(md, "comments", None))),
        "publisher": _first(getattr(md, "publisher", None)),
        "tags": sorted(_values(getattr(md, "tags", None))),
        "identifiers": identifiers,
    }


def _rtf_payload(info_block: bytes) -> bytes:
    """
    Perform the rtf payload test-helper operation with deterministic inputs.

    Example:
        Exercise rtf payload through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param info_block: Value supplied for info block in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return b"{\\rtf1\\ansi\\ansicpg1252" + info_block + b"\\n\\sect\\nBody\\n}"


def test_rtf_metadata_module_import_smoke() -> None:
    """
    Verify rtf metadata module import smoke.

    Example:
        Exercise test rtf metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.rtf as rtf_md

    assert rtf_md is not None


def test_rtf_reader_plugin_is_available_and_preserves_stream_position() -> None:
    """
    Verify rtf reader plugin remains available and preserves stream position.

    Example:
        Exercise test rtf reader plugin is available and preserves stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.builtin_readers import get_metadata_reader_plugins

    payload = _rtf_payload(b"{\\info{\\title Plugin Title}{\\author Plugin Author}}")
    stream = io.BytesIO(payload)
    stream.seek(6)

    plugins = get_metadata_reader_plugins()
    rtf_cls = next((p for p in plugins if p.__name__ == "RTFMetadataReader"), None)
    assert rtf_cls is not None

    reader = rtf_cls(None)
    md = reader.get_metadata(stream=stream, ftype="rtf")

    assert stream.tell() == 6
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]


def test_rtf_get_metadata_parses_unicode_and_cp1252_escapes() -> None:
    """
    Verify rtf get metadata parses unicode and cp1252 escapes.

    Example:
        Exercise test rtf get metadata parses unicode and cp1252 escapes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    info = (
        b"{\\info"
        b"{\\title Caf\\'e9 \\u945? \\u26481?}"
        b"{\\author Jos\\'e9 and Ana\\'efs}"
        b"{\\subject Subject Text}"
        b"{\\comment Comment Wins}"
        b"{\\manager Pub House}"
        b"{\\category tag-one, tag-two}"
        b"{\\keywords tag-three}"
        b"}"
    )
    md = get_metadata(io.BytesIO(_rtf_payload(info)))

    assert "Café" in md.title
    assert "α" in md.title
    assert "東" in md.title
    # One name: RTF authors are split on commas only, as calibre splits them.
    assert _values(md.authors) == ["José and Anaïs"]
    assert _first(md.comments) == "Comment Wins"
    assert _first(md.publisher) == "Pub House"
    tags = {x.lower() for x in _values(md.tags)}
    assert tags >= {"tag-one", "tag-two", "tag-three"}


def test_rtf_get_metadata_pathlike_input(tmp_path: Path) -> None:
    """
    Verify rtf get metadata pathlike input.

    Example:
        Exercise test rtf get metadata pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    path = tmp_path / "sample.rtf"
    payload = _rtf_payload(b"{\\info{\\title Path Title}{\\author Path Author}}")
    path.write_bytes(payload)

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_rtf_invalid_payload_raises_by_default_and_can_opt_into_fallback() -> None:
    """
    Verify rtf invalid payload raises by default and can opt into fallback.

    Example:
        Exercise test rtf invalid payload raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import RtfFormatError, get_metadata

    with pytest.raises(RtfFormatError):
        get_metadata(io.BytesIO(b"not-an-rtf"))

    md = get_metadata(io.BytesIO(b"not-an-rtf"), fallback_on_parse_error=True)
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_rtf_set_metadata_roundtrip_unicode() -> None:
    """
    Verify rtf set metadata roundtrip unicode.

    Example:
        Exercise test rtf set metadata roundtrip unicode through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata, set_metadata

    stream = io.BytesIO(b"{\\rtf1\\ansi\\ansicpg1252\\deff0 {\\fonttbl{\\f0 Arial;}}\\n\\par body\\n}")
    mi = calibreMetaInformation("Updated Καλημέρα 😀", ["Alice", "Боб"])
    mi.comments = "Comment こんにちは"
    mi.publisher = "Pub Δ"
    mi.tags = ["one", "two"]

    set_metadata(stream, mi)
    stream.seek(0)
    out = stream.read()
    assert b"{\\info" in out

    parsed = get_metadata(io.BytesIO(out))
    assert "Updated" in parsed.title
    assert _values(parsed.authors) == ["Alice", "Боб"]
    assert "Comment" in _first(parsed.comments)
    assert _first(parsed.publisher) == "Pub Δ"
    assert {"one", "two"} <= set(_values(parsed.tags))


def test_rtf_set_metadata_escapes_hostile_markup_without_mutating_input() -> None:
    """
    Verify rtf set metadata escapes hostile markup without mutating input.

    Example:
        Exercise test rtf set metadata escapes hostile markup without mutating input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_document_info, get_metadata, set_metadata

    stream = io.BytesIO(_rtf_payload(b"{\\info{\\title Old}{\\author Old}}"))
    title = "Danger {Title}\\path \x00\ud800 😀"
    authors = ["Alice {One}", "Bob\\Two"]
    mi = calibreMetaInformation(title, authors)
    mi.comments = "Comment {one}\\two \x01\udfff"
    mi.publisher = "Pub {House}\\Desk"
    mi.tags = ["tag{one}", "tag\\two"]

    set_metadata(stream, mi)
    out = stream.getvalue()

    assert b"Body" in out
    assert b"\x00" not in out
    assert b"\\{Title\\}" in out
    assert b"\\\\path" in out
    info_block, _pos = get_document_info(io.BytesIO(out))
    assert info_block is not None

    parsed = get_metadata(io.BytesIO(out))
    assert parsed.title == "Danger {Title}\\path 😀"
    assert _values(parsed.authors) == ["Alice {One}", "Bob\\Two"]
    assert _first(parsed.comments) == "Comment {one}\\two"
    assert _first(parsed.publisher) == "Pub {House}\\Desk"
    assert set(_values(parsed.tags)) == {"tag{one}", "tag\\two"}

    assert mi.title == title
    assert mi.authors == authors


def test_rtf_set_metadata_salvages_malformed_info_block() -> None:
    """
    Verify rtf set metadata salvages malformed info block.

    Example:
        Exercise test rtf set metadata salvages malformed info block through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata, set_metadata

    # Existing info block contains invalid bytes and malformed structure.
    stream = io.BytesIO(
        b"{\\rtf1\\ansi\\ansicpg1252{\\info{\\title Broken\xff\xfe}{\\author A\\'e9}}\\n\\par body\\n}"
    )
    mi = calibreMetaInformation("Rescue Καλημέρα 😀", ["Alice", "Боб"])
    mi.comments = "Recovered comments"
    mi.tags = ["rtf-rescue", "δοκιμή"]

    set_metadata(stream, mi)
    stream.seek(0)
    out = stream.read()
    assert b"{\\info" in out

    parsed = get_metadata(io.BytesIO(out))
    assert "Rescue" in str(parsed.title)
    assert any("Alice" in str(author) for author in _values(parsed.authors))
    assert "Recovered comments" in str(_first(parsed.comments))
    assert "rtf-rescue" in {str(tag).casefold() for tag in _values(parsed.tags)}


def test_rtf_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    """
    Verify rtf md fixture smoke and deterministic.

    Example:
        Exercise test rtf md fixture smoke and deterministic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    fixture = md_test_fixture(file_ext="rtf", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert bool(_first(md_1.title))


class _EndOfFileGuard(io.BytesIO):
    """
    A stream that fails a test which keeps reading after its end.

    Example:
        >>> _EndOfFileGuard(b"ab").read()
        b'ab'
    """

    def __init__(self, data: bytes) -> None:
        """
        Hold the data and start counting reads at the end.

        Example:
            >>> _EndOfFileGuard(b"").empty_reads
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
            >>> _EndOfFileGuard(b"x").read(1)
            b'x'


        :param size: How many bytes to read; all if negative.
        :return: The bytes read.
        :raises AssertionError: When the reader keeps reading at the end.
        """
        data = super().read(size)
        if not data and size != 0:
            self.empty_reads += 1
            if self.empty_reads > 64:
                raise AssertionError("the reader kept reading after the end of the file")
        return data


def test_rtf_info_group_cut_off_by_the_end_of_the_file_is_read() -> None:
    """
    Stop at the end of a file whose info group is never closed.

    calibre's reader loops forever here; this one stops, and keeps what it can.

    Example:
        Exercise the cut-off info group through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rtf import get_document_info, get_metadata

    stream = _EndOfFileGuard(b"{\\rtf1\\ansi{\\info{\\title Kept}{\\author Cut off")

    block, _pos = get_document_info(stream)
    md = get_metadata(stream)

    assert block == b"{\\info{\\title Kept}{\\author Cut off"
    assert _first(md.title) == "Kept"
    assert _values(md.authors) == ["Unknown"]


@pytest.mark.parametrize(
    "block",
    [
        b"{\\info{\\title A}{\\author B}}",
        b"{\\info{\\title A \\} still A}}",
        b"{\\info{\\title A \\\\}}",
        b"{\\info{\\titlepg}{\\title Real}}",
        b"{\\title Outside}{\\info{\\title Inside}}",
        b"{\\info{\\author A}{\\title never closed",
        b"{\\info{\\title}",
        b"{\\info{\\info{\\title X}",
        b"no info here {\\title X}",
        b"",
    ],
)
def test_rtf_info_fields_match_the_patterns_they_replace(block: bytes) -> None:
    """
    Find the same field values the old regular expressions found.

    Example:
        Exercise the field lookup through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :param block: An info block, well formed or not.
    :return: None; the function records state or raises through its assertions.
    """
    import re

    from LiuXin_alpha.metadata.file_sources.rtf import info_field, replace_fields

    for name in ("title", "author", "subject", "comment", "category", "keywords", "manager", "company", "operator"):
        pattern = re.compile(rb"\{\\info.*?\{\\" + name.encode() + rb"(.*?)(?<!\\)\}", re.DOTALL)
        match = pattern.search(block)
        assert info_field(block, name.encode()) == (None if match is None else match.group(1))

        text = block.decode("ascii")
        replacement = "{\\" + name + " New}"
        subn = re.compile(r"\{\\" + name + r"(.*?)(?<!\\)\}", re.DOTALL).subn(lambda _match, value=replacement: value, text)
        assert replace_fields(text, name, replacement) == subn


def test_rtf_many_unclosed_info_groups_are_read_in_linear_time() -> None:
    """
    Read and rewrite a file of repeated, never-closed info groups quickly.

    The regular expressions this module used backtracked in cubic time on such a
    file: 15 kB took nearly a minute to read, and 150 kB took 15 seconds to
    rewrite. The 1.5 MB file here runs in a child process with a time limit, so a
    regression fails rather than hanging the suite.

    Example:
        Exercise the repeated groups through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rtf_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import os
    import subprocess
    import sys

    script = (
        "import io\n"
        "from types import SimpleNamespace\n"
        "from LiuXin_alpha.metadata.file_sources import rtf\n"
        "data = b'{\\\\rtf1' + b'{\\\\info{\\\\title x' * 100000\n"
        "assert rtf.get_metadata(io.BytesIO(data)).title is not None\n"
        "try:\n"
        "    rtf.set_metadata(io.BytesIO(data), SimpleNamespace(title='New', authors=['A']))\n"
        "except ValueError:\n"
        "    pass\n"
        "print('done')\n"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(path for path in sys.path if path)}
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, env=env, check=False
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "done"
