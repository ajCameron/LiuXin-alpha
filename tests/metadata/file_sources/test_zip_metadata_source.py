"""
Verify ZIP metadata member dispatch and fallback behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test zip metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py
"""
from __future__ import annotations

import io
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _make_md(title: str, authors: list[str] | None = None):
    """
    Perform the make md test-helper operation with deterministic inputs.

    Example:
        Exercise make md through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param title: Value supplied for title in the focused test operation.
    :param authors: Value supplied for authors in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return calibreMetaInformation(title, authors or ["Unknown"])


def _zip_bytes(entries: dict[str, bytes], *, comment: bytes = b"") -> bytes:
    """
    Perform the zip bytes test-helper operation with deterministic inputs.

    Example:
        Exercise zip bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param entries: Value supplied for entries in the focused test operation.
    :param comment: Value supplied for comment in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
        zf.comment = comment
    return out.getvalue()


def _snapshot(md) -> dict:
    """
    Perform the snapshot test-helper operation with deterministic inputs.

    Example:
        Exercise snapshot through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


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
        "publisher": _first(getattr(md, "publisher", None)),
        "isbn": _first(getattr(md, "isbn", None)),
        "identifiers": identifiers,
    }


def test_zip_metadata_module_import_smoke() -> None:
    """
    Verify zip metadata module import smoke.

    Example:
        Exercise test zip metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    assert zip_md is not None


def test_zip_reader_plugin_is_available_and_preserves_stream_position(monkeypatch) -> None:
    """
    Verify zip reader plugin remains available and preserves stream position.

    Example:
        Exercise test zip reader plugin is available and preserves stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    payload = _zip_bytes({"book.epub": b"epub-bytes"})
    stream = io.BytesIO(payload)
    stream.name = "bundle.zip"
    stream.seek(9)

    seen = {}

    def _fake_dispatch(target, *, force_type: str):
        """
        Perform the fake dispatch test-helper operation with deterministic inputs.

        Example:
            Exercise test zip reader plugin is available and preserves stream position.fake dispatch through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


        :param target: Container or object receiving hydrated metadata.
        :param force_type: Value supplied for force type in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        seen["force_type"] = force_type
        seen["target_name"] = getattr(target, "name", "")
        seen["payload"] = target.read()
        target.seek(0)
        return _make_md("Plugin Title", ["Plugin Author"])

    monkeypatch.setattr(zip_md, "_dispatch_metadata", _fake_dispatch)

    plugins = get_metadata_reader_plugins()
    zip_cls = next((p for p in plugins if p.__name__ == "ZipMetadataReader"), None)
    assert zip_cls is not None

    reader = zip_cls(None)
    md = reader.get_metadata(stream=stream, ftype="zip")

    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]
    assert seen["force_type"] == "epub"
    assert seen["target_name"] == "book.epub"
    assert seen["payload"] == b"epub-bytes"
    assert stream.tell() == 9


def test_zip_get_metadata_routes_comic_archive_to_cbz(monkeypatch) -> None:
    """
    Verify zip get metadata routes comic archive to cbz.

    Example:
        Exercise test zip get metadata routes comic archive to cbz through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    payload = _zip_bytes({"page001.jpg": b"img1", "page002.png": b"img2"})
    stream = io.BytesIO(payload)
    stream.name = "comic.zip"

    seen = {}

    def _fake_dispatch(target, *, force_type: str):
        """
        Perform the fake dispatch test-helper operation with deterministic inputs.

        Example:
            Exercise test zip get metadata routes comic archive to cbz.fake dispatch through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


        :param target: Container or object receiving hydrated metadata.
        :param force_type: Value supplied for force type in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        seen["force_type"] = force_type
        seen["target"] = target
        return _make_md("Comic Title", ["Artist One"])

    monkeypatch.setattr(zip_md, "_dispatch_metadata", _fake_dispatch)

    md = zip_md.get_metadata(stream)
    assert md.title == "Comic Title"
    assert _values(md.authors) == ["Artist One"]
    assert seen["force_type"] == "cbz"
    assert seen["target"] is stream


def test_zip_get_metadata_extracts_first_supported_member(monkeypatch) -> None:
    """
    Verify zip get metadata extracts first supported member.

    Example:
        Exercise test zip get metadata extracts first supported member through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    payload = _zip_bytes(
        {
            "readme.txt": b"ignore-me",
            "books/book.epub": b"epub-bytes",
            "books/book.mobi": b"mobi-bytes",
        }
    )
    stream = io.BytesIO(payload)
    stream.name = "bundle.zip"

    def _fake_dispatch(target, *, force_type: str):
        """
        Perform the fake dispatch test-helper operation with deterministic inputs.

        Example:
            Exercise test zip get metadata extracts first supported member.fake dispatch through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


        :param target: Container or object receiving hydrated metadata.
        :param force_type: Value supplied for force type in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        assert force_type == "epub"
        assert getattr(target, "name", "") == "book.epub"
        assert target.read() == b"epub-bytes"
        target.seek(0)
        md = _make_md("Nested EPUB", ["Alice"])
        md.timestamp = "will-be-cleared"
        return md

    monkeypatch.setattr(zip_md, "_dispatch_metadata", _fake_dispatch)

    md = zip_md.get_metadata(stream)
    assert md.title == "Nested EPUB"
    assert _values(md.authors) == ["Alice"]
    assert getattr(md, "timestamp", None) is None


def test_zip_get_metadata_raises_for_archive_with_no_supported_members() -> None:
    """
    Verify zip get metadata raises for archive with no supported members.

    Example:
        Exercise test zip get metadata raises for archive with no supported members through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    payload = _zip_bytes({"readme.txt": b"text", "images/page.jpg": b"img"})
    stream = io.BytesIO(payload)
    stream.name = "empty.zip"

    with pytest.raises(ValueError, match="No ebook found in ZIP archive"):
        zip_md.get_metadata(stream)


def test_zip_get_metadata_accepts_pathlike_input(tmp_path: Path, monkeypatch) -> None:
    """
    Verify zip get metadata accepts pathlike input.

    Example:
        Exercise test zip get metadata accepts pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    archive_path = tmp_path / "sample.zip"
    archive_path.write_bytes(_zip_bytes({"book.epub": b"payload"}))

    monkeypatch.setattr(zip_md, "_dispatch_metadata", lambda target, force_type: _make_md("Pathlike ZIP", ["P"]))

    md = zip_md.get_metadata(archive_path)
    assert md.title == "Pathlike ZIP"
    assert _values(md.authors) == ["P"]


def test_zip_md_fixtures_smoke_and_deterministic(md_test_fixture) -> None:
    """
    Verify zip md fixtures smoke and deterministic.

    Example:
        Exercise test zip md fixtures smoke and deterministic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_zip_metadata_source.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    fixture = md_test_fixture(file_ext="zip", file_num=3, verify_hash=True)
    md_1 = zip_md.get_metadata(fixture)
    md_2 = zip_md.get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert bool(_first(md_1.title))
