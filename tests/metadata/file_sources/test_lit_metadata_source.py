"""
Verify LIT metadata extraction and optional-tool failure behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test lit metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
"""
from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


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


def _build_basic_opf(*, guide: str) -> str:
    """
    Perform the build basic opf test-helper operation with deterministic inputs.

    Example:
        Exercise build basic opf through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param guide: Value supplied for guide in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:opf="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata>"
        "<dc:title>Lit Metadata Title</dc:title>"
        '<dc:creator opf:role="aut">Ada Lovelace</dc:creator>'
        "</metadata>"
        "<manifest>"
        '<item id="c1" href="cover.jpg" media-type="image/jpeg"/>'
        '<item id="c2" href="cover-standard.jpg" media-type="image/jpeg"/>'
        "</manifest>"
        "<spine/>"
        f"<guide>{guide}</guide>"
        "</package>"
    )


class _ManifestItem:
    """
    Provide the ManifestItem test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ManifestItem through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
    """
    def __init__(self, path: str, internal: str) -> None:
        """
        Initialize the ManifestItem test double.

        Example:
            Exercise ManifestItem.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param path: Value supplied for path in the focused test operation.
        :param internal: Value supplied for internal in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.path = path
        self.internal = internal


def test_lit_metadata_module_import_smoke() -> None:
    """
    Verify lit metadata module import smoke.

    Example:
        Exercise test lit metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    assert lit_md is not None


def test_lit_reader_plugin_is_available() -> None:
    """
    Verify lit reader plugin remains available.

    Example:
        Exercise test lit reader plugin is available through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    plugins = get_metadata_reader_plugins()
    lit_cls = next((p for p in plugins if p.__name__ == "LITMetadataReader"), None)
    assert lit_cls is not None


def test_lit_get_metadata_extracts_title_author_and_cover(monkeypatch) -> None:
    """
    Verify lit get metadata extracts title author and cover.

    Example:
        Exercise test lit get metadata extracts title author and cover through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        """
        Provide the InnerLit test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit get metadata extracts title author and cover.InnerLit through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        manifest = {"cover": _ManifestItem("cover.jpg", "cover-internal")}

        @staticmethod
        def get_file(path: str) -> bytes:
            """
            Return file from deterministic test state.

            Example:
                Exercise test lit get metadata extracts title author and cover.InnerLit.get file through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            assert path == "/data/cover-internal"
            return b"\x11\x22\x33\x44\x55"

    class _Container:
        """
        Provide the Container test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit get metadata extracts title author and cover.Container through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the Container test double.

            Example:
                Exercise test lit get metadata extracts title author and cover.Container.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test lit get metadata extracts title author and cover.Container.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return _build_basic_opf(guide='<reference type="cover" href="cover.jpg"/>')

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    md = lit_md.get_metadata(io.BytesIO(b"not-a-real-lit"))

    assert md.title == "Lit Metadata Title"
    assert _values(md.authors) == ["Ada Lovelace"]
    assert isinstance(md.cover_data, tuple) and len(md.cover_data) == 2
    assert md.cover_data[0] == "jpg"
    assert md.cover_data[1] == b"\x11\x22\x33\x44\x55"


def test_lit_cover_standard_reference_preferred_when_present(monkeypatch) -> None:
    """
    Verify lit cover standard reference preferred when present.

    Example:
        Exercise test lit cover standard reference preferred when present through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        """
        Provide the InnerLit test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit cover standard reference preferred when present.InnerLit through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        manifest = {
            "cover": _ManifestItem("cover.jpg", "cover-internal"),
            "cover-standard": _ManifestItem("cover-standard.jpg", "cover-standard-internal"),
        }

        @staticmethod
        def get_file(path: str) -> bytes:
            """
            Return file from deterministic test state.

            Example:
                Exercise test lit cover standard reference preferred when present.InnerLit.get file through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            if path.endswith("cover-internal"):
                return b"A" * 16
            if path.endswith("cover-standard-internal"):
                return b"B" * 8
            raise KeyError(path)

    class _Container:
        """
        Provide the Container test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit cover standard reference preferred when present.Container through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the Container test double.

            Example:
                Exercise test lit cover standard reference preferred when present.Container.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test lit cover standard reference preferred when present.Container.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return _build_basic_opf(
                guide=(
                    '<reference type="cover" href="cover.jpg"/>'
                    '<reference type="cover-standard" href="cover-standard.jpg"/>'
                )
            )

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    md = lit_md.get_metadata(io.BytesIO(b"not-a-real-lit"))
    assert md.cover_data[1] == b"B" * 8


def test_lit_get_metadata_reader_error_raises_by_default_and_can_opt_into_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """
    Verify lit get metadata reader error raises by default and can opt into fallback.

    Example:
        Exercise test lit get metadata reader error raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _BrokenContainer:
        """
        Provide the BrokenContainer test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit get metadata reader error raises by default and can opt into fallback.BrokenContainer through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the BrokenContainer test double.

            Example:
                Exercise test lit get metadata reader error raises by default and can opt into fallback.BrokenContainer.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            raise ValueError("broken lit fixture")

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _BrokenContainer)

    lit_path = tmp_path / "fallback_title.lit"
    lit_path.write_bytes(b"broken")

    with pytest.raises(lit_md.LitFormatError):
        lit_md.get_metadata(lit_path)

    md = lit_md.get_metadata(lit_path, fallback_on_parse_error=True)
    assert md.title == "fallback_title"
    assert _values(md.authors) == ["Unknown"]


def test_lit_stream_position_restored_after_read(monkeypatch) -> None:
    """
    Verify lit stream position restored after read.

    Example:
        Exercise test lit stream position restored after read through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources import lit as lit_md

    class _InnerLit:
        """
        Provide the InnerLit test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit stream position restored after read.InnerLit through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        manifest = {}

        @staticmethod
        def get_file(_path: str) -> bytes:
            """
            Return file from deterministic test state.

            Example:
                Exercise test lit stream position restored after read.InnerLit.get file through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param _path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise KeyError("no cover")

    class _Container:
        """
        Provide the Container test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit stream position restored after read.Container through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the Container test double.

            Example:
                Exercise test lit stream position restored after read.Container.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self._litfile = _InnerLit()

        @staticmethod
        def get_metadata() -> str:
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test lit stream position restored after read.Container.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return _build_basic_opf(guide="")

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)

    stream = io.BytesIO(b"prefix-and-suffix")
    stream.seek(6)
    md = lit_md.get_metadata(stream)

    assert md.title == "Lit Metadata Title"
    assert stream.tell() == 6
