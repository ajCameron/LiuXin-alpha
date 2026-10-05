"""
Exercise legacy metadata dispatcher and worker compatibility edge cases.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test legacy dispatcher worker edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
"""
from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
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

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _pml_comment(**fields: str) -> bytes:
    """
    Perform the pml comment test-helper operation with deterministic inputs.

    Example:
        Exercise pml comment through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param fields: Value supplied for fields in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    inner = " ".join(f'{key}="{value}"' for key, value in fields.items())
    return f"\\v{inner}\\v".encode("utf-8")


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    """
    Perform the zip bytes test-helper operation with deterministic inputs.

    Example:
        Exercise zip bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param entries: Value supplied for entries in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return out.getvalue()


class _SeekTellBroken(io.BytesIO):
    """
    Provide the SeekTellBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise SeekTellBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
    """
    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise SeekTellBroken.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")

    def seek(self, *args, **kwargs):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise SeekTellBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("seek unavailable")


def test_lit_private_helpers_and_cover_resolution_edges(monkeypatch) -> None:
    """
    Verify lit private helpers and cover resolution edges.

    Example:
        Exercise test lit private helpers and cover resolution edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    events: list[tuple[str, str]] = []

    class _Logger:
        """
        Provide the Logger test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit private helpers and cover resolution edges.Logger through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        @staticmethod
        def warning(message):
            """
            Perform the warning test-helper operation with deterministic inputs.

            Example:
                Exercise test lit private helpers and cover resolution edges.Logger.warning through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param message: Value supplied for message in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            events.append(("warning", message))

        @staticmethod
        def info(message):
            """
            Perform the info test-helper operation with deterministic inputs.

            Example:
                Exercise test lit private helpers and cover resolution edges.Logger.info through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param message: Value supplied for message in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            events.append(("info", message))

        @staticmethod
        def debug(message):
            """
            Perform the debug test-helper operation with deterministic inputs.

            Example:
                Exercise test lit private helpers and cover resolution edges.Logger.debug through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param message: Value supplied for message in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            events.append(("debug", message))

    monkeypatch.setattr(lit_md, "default_log", _Logger())
    proxy = lit_md._LitLogProxy()
    proxy.warn("warn", None, "message")
    proxy.info("info message")
    proxy.debug("debug message")
    proxy._emit("warning")
    assert ("warning", "warn message") in events
    assert ("info", "info message") in events
    assert ("debug", "debug message") in events

    assert lit_md._default_metadata("/tmp/Cafebook.lit").title == "Cafebook"
    assert lit_md._normalize_href(r"./Images\Caf%C3%A9%20%26%20Cover.JPG#frag") == "Images/Café & Cover.JPG"
    assert lit_md._normalize_href("") == ""
    assert lit_md._href_candidates(None) == ()
    assert "Images/Café & Cover.JPG" in lit_md._href_candidates("Images/Caf%C3%A9 & Cover.JPG#frag")

    monkeypatch.setattr(lit_md, "identify", lambda _data: ("jpeg", 1, 1))
    assert lit_md._guess_cover_format("cover.bin", b"jpegish") == "jpg"
    monkeypatch.setattr(lit_md, "identify", lambda _data: (_ for _ in ()).throw(ValueError("not an image")))
    assert lit_md._guess_cover_format("cover.jpe", b"not-image") == "jpg"
    assert lit_md._guess_cover_format("", b"not-image") == "jpg"

    class _ManifestItem:
        """
        Provide the ManifestItem test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit private helpers and cover resolution edges.ManifestItem through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self, path: str, internal: str | None) -> None:
            """
            Initialize the ManifestItem test double.

            Example:
                Exercise test lit private helpers and cover resolution edges.ManifestItem.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param path: Value supplied for path in the focused test operation.
            :param internal: Value supplied for internal in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self.path = path
            self.internal = internal

    class _LitFile:
        """
        Provide the LitFile test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit private helpers and cover resolution edges.LitFile through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        manifest = {
            "blank": _ManifestItem("", "ignored"),
            "missing-internal": _ManifestItem("missing-internal.jpg", None),
            "casefold": _ManifestItem("images/café & cover.jpg", "cover-internal"),
            "empty": _ManifestItem("empty.jpg", "empty"),
            "broken": _ManifestItem("broken.jpg", "broken"),
        }

        @staticmethod
        def get_file(path: str):
            """
            Return file from deterministic test state.

            Example:
                Exercise test lit private helpers and cover resolution edges.LitFile.get file through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param path: Value supplied for path in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            if path.endswith("cover-internal"):
                return bytearray(b"cover-bytes")
            if path.endswith("empty"):
                return b""
            raise KeyError(path)

    opf = SimpleNamespace(
        iterguide=lambda: [
            {"type": "text", "href": "images/café & cover.jpg"},
            {"type": "cover", "href": "missing.jpg"},
            {"type": "cover", "href": "missing-internal.jpg"},
            {"type": "cover", "href": "empty.jpg"},
            {"type": "cover", "href": "broken.jpg"},
            {"type": "cover", "href": "Images/Caf%C3%A9%20%26%20Cover.JPG#frag"},
        ]
    )

    assert lit_md._extract_cover_from_guide(opf, _LitFile()) == ("jpg", b"cover-bytes")
    assert lit_md._extract_cover_from_guide(SimpleNamespace(iterguide=lambda: []), _LitFile()) is None


def test_lit_reader_fallbacks_type_error_and_stream_position(monkeypatch) -> None:
    """
    Verify lit reader fallbacks type error and stream position.

    Example:
        Exercise test lit reader fallbacks type error and stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    with pytest.raises(TypeError, match="target_file"):
        lit_md.get_metadata(123)  # type: ignore[arg-type]

    class _Container:
        """
        Provide the Container test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit reader fallbacks type error and stream position.Container through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the Container test double.

            Example:
                Exercise test lit reader fallbacks type error and stream position.Container.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            pass

        @staticmethod
        def get_metadata() -> bytes:
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test lit reader fallbacks type error and stream position.Container.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return (
                b'<package xmlns="http://www.idpf.org/2007/opf" '
                b'xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">'
                b"<metadata></metadata><manifest/><spine/></package>"
            )

    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _Container)
    stream = _SeekTellBroken(b"lit")
    stream.name = "fallback-name.lit"
    md = lit_md.get_metadata(stream)
    assert _first(md.title) in {"fallback-name", "Unknown"}

    class _NoLogException:
        """
        Provide the NoLogException test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit reader fallbacks type error and stream position.NoLogException through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        @staticmethod
        def warning(message):
            """
            Perform the warning test-helper operation with deterministic inputs.

            Example:
                Exercise test lit reader fallbacks type error and stream position.NoLogException.warning through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param message: Value supplied for message in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            assert "Failed to read metadata from LIT file" in message

    class _BrokenContainer:
        """
        Provide the BrokenContainer test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test lit reader fallbacks type error and stream position.BrokenContainer through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self, _stream, _log) -> None:
            """
            Initialize the BrokenContainer test double.

            Example:
                Exercise test lit reader fallbacks type error and stream position.BrokenContainer.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _stream: Value supplied for stream in the focused test operation.
            :param _log: Value supplied for log in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            raise RuntimeError("broken")

    monkeypatch.setattr(lit_md, "default_log", _NoLogException())
    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _BrokenContainer)
    with pytest.raises(lit_md.LitFormatError):
        lit_md.read_metadata_from_stream(io.BytesIO(b"bad"), source_name="broken.lit")

    md = lit_md.read_metadata_from_stream(
        io.BytesIO(b"bad"),
        source_name="broken.lit",
        fallback_on_parse_error=True,
    )
    assert md.title == "broken"


def test_pml_private_helpers_sources_and_author_fallbacks(tmp_path: Path, monkeypatch) -> None:
    """
    Verify pml private helpers sources and author fallbacks.

    Example:
        Exercise test pml private helpers sources and author fallbacks through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.pml as pml_md

    assert pml_md._source_name(tmp_path / "sample.pml") == str(tmp_path / "sample.pml")
    assert pml_md._read_source_bytes(bytearray(b"abc")) == (b"abc", "")

    path = tmp_path / "source.pml"
    path.write_bytes(b"path-bytes")
    assert pml_md._read_source_bytes(path) == (b"path-bytes", str(path))

    text_stream = io.StringIO("unicode text")
    text_stream.name = "text-stream.pml"
    assert pml_md._read_source_bytes(text_stream) == (b"unicode text", "text-stream.pml")

    broken = _SeekTellBroken(b"broken-seek")
    broken.name = "broken.pml"
    assert pml_md._read_source_bytes(broken) == (b"broken-seek", "broken.pml")
    with pytest.raises(TypeError, match="PML metadata reader"):
        pml_md._read_source_bytes(object())

    assert pml_md._decode_field(b"") == ""
    assert pml_md._sanitize_field(b"Caf\xe9 <tag>\x01") == "Café &lt;tag&gt;"
    assert pml_md._normalize_zip_name(r".\nested\book.pml") == "nested/book.pml"
    assert pml_md._is_probable_pmlz("book.pml", b"not-zip") is False
    assert pml_md._is_probable_pmlz("book.pmlz", b"not-zip") is True
    assert pml_md._is_probable_pmlz("book.zip", b"PK\x03\x04broken") is False

    class _AuthorSetter:
        """
        Provide the AuthorSetter test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pml private helpers sources and author fallbacks.AuthorSetter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self) -> None:
            """
            Initialize the AuthorSetter test double.

            Example:
                Exercise test pml private helpers sources and author fallbacks.AuthorSetter.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: None; the function records state or raises through its assertions.
            """
            self.calls: list[str] = []

        @property
        def authors(self):
            """
            Perform the authors test-helper operation with deterministic inputs.

            Example:
                Exercise test pml private helpers sources and author fallbacks.AuthorSetter.authors through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("current unavailable")

        @authors.setter
        def authors(self, value):
            """
            Perform the authors test-helper operation with deterministic inputs.

            Example:
                Exercise test pml private helpers sources and author fallbacks.AuthorSetter.authors through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param value: Value stored, compared or projected by the operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.calls.append(value)
            if value == "Stop":
                raise RuntimeError("stop")

    target = _AuthorSetter()
    pml_md._set_authors(target, ["Alice", "Stop", "Ignored"])
    assert target.calls == [[], "Alice", "Stop"]
    pml_md._set_authors(target, [])
    assert target.calls == [[], "Alice", "Stop"]

    class _DataBroken:
        """
        Provide the DataBroken test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pml private helpers sources and author fallbacks.DataBroken through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        @property
        def _data(self):
            """
            Perform the data test-helper operation with deterministic inputs.

            Example:
                Exercise test pml private helpers sources and author fallbacks.DataBroken.data through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("no raw data")

        @property
        def authors(self):
            """
            Perform the authors test-helper operation with deterministic inputs.

            Example:
                Exercise test pml private helpers sources and author fallbacks.DataBroken.authors through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return ("Unknown",)

        @authors.setter
        def authors(self, _value):
            """
            Perform the authors test-helper operation with deterministic inputs.

            Example:
                Exercise test pml private helpers sources and author fallbacks.DataBroken.authors through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _value: Value supplied for value in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("cannot set")

    pml_md._clear_default_authors(_DataBroken())


def test_pml_zip_cover_payload_and_parse_error_edges(monkeypatch) -> None:
    """
    Verify pml zip cover payload and parse error edges.

    Example:
        Exercise test pml zip cover payload and parse error edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.pml as pml_md

    payload = _zip_bytes(
        {
            "folder/book.pml": _pml_comment(TITLE="Folder Title", AUTHOR="Folder Author"),
            "folder/book_img/cover.png": b"folder-cover",
        }
    )
    pml_data, cover = pml_md._extract_pmlz_payload(payload, source_name="archive.pmlz", extract_cover=True)
    assert b"Folder Title" in pml_data
    assert cover == b"folder-cover"

    payload_no_cover = _zip_bytes({"book.pml": b"payload", "images/cover.png": b"ignored"})
    _, cover = pml_md._extract_pmlz_payload(payload_no_cover, source_name="", extract_cover=False)
    assert cover is None
    with pytest.raises(pml_md.PmlFormatError):
        pml_md._extract_pmlz_payload(b"not-a-zip", source_name="", extract_cover=True)
    assert pml_md._extract_pmlz_payload(
        b"not-a-zip",
        source_name="",
        extract_cover=True,
        fallback_on_parse_error=True,
    ) == (b"", None)

    class _FakeZip:
        """
        Provide the FakeZip test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pml zip cover payload and parse error edges.FakeZip through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self) -> None:
            """
            Initialize the FakeZip test double.

            Example:
                Exercise test pml zip cover payload and parse error edges.FakeZip.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: None; the function records state or raises through its assertions.
            """
            self.calls = 0

        @staticmethod
        def namelist():
            """
            Perform the namelist test-helper operation with deterministic inputs.

            Example:
                Exercise test pml zip cover payload and parse error edges.FakeZip.namelist through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return ["main_img/cover.png", "z/cover.png"]

        def read(self, name):
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test pml zip cover payload and parse error edges.FakeZip.read through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param name: Value supplied for name in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first read fails")
            return b"fallback-cover"

    assert pml_md._read_cover_from_zip(_FakeZip(), source_name="main.pmlz", pml_entries=[]) == b"fallback-cover"

    class _BadZip:
        """
        Provide the BadZip test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pml zip cover payload and parse error edges.BadZip through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        @staticmethod
        def namelist():
            """
            Perform the namelist test-helper operation with deterministic inputs.

            Example:
                Exercise test pml zip cover payload and parse error edges.BadZip.namelist through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return ["book.pml", "cover.png"]

        @staticmethod
        def read(_name):
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test pml zip cover payload and parse error edges.BadZip.read through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _name: Value supplied for name in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("always fails")

    assert pml_md._read_cover_from_zip(_BadZip(), source_name="", pml_entries=["book.pml"]) is None

    class _BrokenGetCover:
        """
        Provide the BrokenGetCover test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test pml zip cover payload and parse error edges.BrokenGetCover through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        pass

    monkeypatch.setattr(pml_md, "get_cover", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("cover fail")))
    md = pml_md.get_metadata(io.BytesIO(_pml_comment(TITLE="No Cover Error")), extract_cover=True)
    assert md.title == "No Cover Error"
    with pytest.raises(TypeError):
        pml_md.get_metadata(object())
    assert pml_md.get_metadata(object(), fallback_on_parse_error=True).title == "Unknown"
    assert pml_md.get_metadata_inplace(io.BytesIO(_pml_comment(TITLE="Inplace")), extract_cover=False).title == "Inplace"


def test_haodoo_author_normalization_and_reader_edges(monkeypatch) -> None:
    """
    Verify haodoo author normalization and reader edges.

    Example:
        Exercise test haodoo author normalization and reader edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.pdb.haodoo as haodoo_md

    assert haodoo_md._normalize_authors(["Alice", "", "Bob"]) == ["Alice", "Bob"]
    assert haodoo_md._normalize_authors({"Alice": 1, "": 2}) == ["Alice"]
    assert haodoo_md._normalize_authors("Solo") == ["Solo"]
    assert haodoo_md._normalize_authors("") == []
    assert haodoo_md._normalize_authors(42) == ["42"]
    assert haodoo_md._normalize_authors(None) == []

    class _Header:
        """
        Provide the Header test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test haodoo author normalization and reader edges.Header through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        ident = "BOOKMTIT"
        title = "Header Title"

    class _Reader:
        """
        Provide the Reader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test haodoo author normalization and reader edges.Reader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self, pheader, stream, log, extra) -> None:
            """
            Initialize the Reader test double.

            Example:
                Exercise test haodoo author normalization and reader edges.Reader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param pheader: Value supplied for pheader in the focused test operation.
            :param stream: Value supplied for stream in the focused test operation.
            :param log: Value supplied for log in the focused test operation.
            :param extra: Value supplied for extra in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            assert pheader.ident == b"BOOKMTIT"
            assert stream.tell() == 0
            assert extra is None

        @staticmethod
        def get_metadata():
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test haodoo author normalization and reader edges.Reader.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return SimpleNamespace(title="", authors={"Ada": 1, "李白": 2}, language="ZH_CN")

    monkeypatch.setattr(haodoo_md, "PdbHeaderReader", lambda _stream: _Header())
    monkeypatch.setattr(haodoo_md, "Reader", _Reader)

    md = haodoo_md.get_metadata(io.BytesIO(b"pdb"), extract_cover=False)
    assert md.title == "Header Title"
    assert _values(md.authors) == ["Ada", "李白"]
    assert _first(md.language) == "ZH_CN"

    class _BrokenReader:
        """
        Provide the BrokenReader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test haodoo author normalization and reader edges.BrokenReader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        def __init__(self, *_args, **_kwargs) -> None:
            """
            Initialize the BrokenReader test double.

            Example:
                Exercise test haodoo author normalization and reader edges.BrokenReader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _args: Value supplied for args in the focused test operation.
            :param _kwargs: Value supplied for kwargs in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            raise RuntimeError("reader fail")

    monkeypatch.setattr(haodoo_md, "Reader", _BrokenReader)
    fallback = haodoo_md.get_metadata(io.BytesIO(b"pdb"), extract_cover=True)
    assert fallback.title == "Header Title"
    assert _values(fallback.authors) == ["Unknown"]


def test_dispatcher_plugin_adapter_and_failure_edges(tmp_path: Path, monkeypatch) -> None:
    """
    Verify dispatcher plugin adapter and failure edges.

    Example:
        Exercise test dispatcher plugin adapter and failure edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources as dispatcher

    assert dispatcher._normalize_ext(".XHTML") == "html"
    assert dispatcher._normalize_ext("AZW") == "mobi"
    assert dispatcher._normalize_ext("ODS") == "odt"
    assert dispatcher._target_path_hint(tmp_path / "book.epub") == str(tmp_path / "book.epub")
    assert dispatcher._target_path_hint(SimpleNamespace(name="named.mobi")) == "named.mobi"
    assert dispatcher._target_path_hint(SimpleNamespace(name=123)) is None
    assert dispatcher._resolve_extension(SimpleNamespace(name="stream.HTML")) == "html"
    assert dispatcher._is_path_like(b"bytes-path") is True

    class _InplacePlugin:
        """
        Provide the InplacePlugin test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test dispatcher plugin adapter and failure edges.InplacePlugin through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        file_types = ["txt", "html"]
        inplace_run_cost = "medium"
        __module__ = "fake.plugins"

        def __init__(self, _context) -> None:
            """
            Initialize the InplacePlugin test double.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.InplacePlugin.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _context: Value supplied for context in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            pass

        @staticmethod
        def get_metadata_inplace(path, ftype):
            """
            Return metadata inplace from deterministic test state.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.InplacePlugin.get metadata inplace through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param path: Value supplied for path in the focused test operation.
            :param ftype: Value supplied for ftype in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return ("inplace", path, ftype)

    adapter = dispatcher.MetaDataReaderPlugin(_InplacePlugin)
    assert adapter.module_name == "_InplacePlugin"
    assert adapter.file_path == "fake/plugins.py"
    assert adapter.VALID_FOR == ["TXT", "HTML"]
    assert adapter.PRIORITY_FOR == ["TXT", "HTML"]
    assert adapter.RUN_COST == ["MEDIUM"]

    txt_path = tmp_path / "book.txt"
    txt_path.write_text("body", encoding="utf-8")
    assert adapter.get_metadata(txt_path, force_type="txt") == ("inplace", str(txt_path), "txt")

    class _StreamPlugin:
        """
        Provide the StreamPlugin test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test dispatcher plugin adapter and failure edges.StreamPlugin through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        file_types = ["bin"]

        def __init__(self, _context) -> None:
            """
            Initialize the StreamPlugin test double.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.StreamPlugin.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _context: Value supplied for context in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.StreamPlugin.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param stream: Value supplied for stream in the focused test operation.
            :param ftype: Value supplied for ftype in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return ("stream", stream.read(), ftype)

    stream_path = tmp_path / "stream.bin"
    stream_path.write_bytes(b"stream-bytes")
    assert dispatcher._run_metadata_reader(_StreamPlugin, stream_path, ftype="bin") == ("stream", b"stream-bytes", "bin")
    assert dispatcher._run_metadata_reader(_StreamPlugin, io.BytesIO(b"inline"), ftype="bin") == (
        "stream",
        b"inline",
        "bin",
    )
    with pytest.raises(TypeError, match="target_object"):
        dispatcher._run_metadata_reader(_StreamPlugin, object(), ftype="bin")

    class _BadCost:
        """
        Provide the BadCost test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test dispatcher plugin adapter and failure edges.BadCost through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        RUN_COST = ["WEIRD"]
        module_name = "Bad"

    with pytest.raises(AssertionError, match="Unrecognized run cost"):
        dispatcher.sort_plugins_by_run_cost([_BadCost()])

    monkeypatch.setattr(dispatcher, "valid_plugins", [])
    monkeypatch.setattr(dispatcher, "valid_file_formats", set())
    monkeypatch.setattr(dispatcher, "get_metadata_reader_plugins", lambda: [_InplacePlugin])
    dispatcher.valid_plugins.clear()
    dispatcher.valid_file_formats.clear()
    dispatcher.load_plugins()
    assert "TXT" in dispatcher.valid_file_formats
    assert dispatcher.get_plugins_for_extension("xhtml")[0].module_name == "_InplacePlugin"

    class _NoneAdapter:
        """
        Provide the NoneAdapter test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test dispatcher plugin adapter and failure edges.NoneAdapter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        module_name = "NoneAdapter"
        file_path = "none.py"

        @staticmethod
        def get_metadata(_target, force_type=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.NoneAdapter.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _target: Value supplied for target in the focused test operation.
            :param force_type: Value supplied for force type in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return None

    class _FailAdapter:
        """
        Provide the FailAdapter test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test dispatcher plugin adapter and failure edges.FailAdapter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        module_name = "FailAdapter"
        file_path = "fail.py"

        @staticmethod
        def get_metadata(_target, force_type=None):
            """
            Return metadata from deterministic test state.

            Example:
                Exercise test dispatcher plugin adapter and failure edges.FailAdapter.get metadata through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


            :param _target: Value supplied for target in the focused test operation.
            :param force_type: Value supplied for force type in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("reader failed")

    monkeypatch.setattr(dispatcher, "get_plugins_for_extension", lambda _ext: [_NoneAdapter()])
    assert dispatcher.get_metadata(io.BytesIO(b"data"), force_type="txt") is None
    monkeypatch.setattr(dispatcher, "get_plugins_for_extension", lambda _ext: [_FailAdapter()])
    with pytest.raises(RuntimeError, match="FailAdapter"):
        dispatcher.get_metadata(io.BytesIO(b"data"), force_type="txt")


def test_worker_helpers_metadata_merge_and_import_edges(tmp_path: Path, monkeypatch) -> None:
    """
    Verify worker helpers metadata merge and import edges.

    Example:
        Exercise test worker helpers metadata merge and import edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.worker as worker

    assert worker._values(None) == []
    assert worker._values({"a": 1}) == ["a"]
    assert worker._values("x") == ["x"]
    assert worker._values(7) == [7]
    assert worker._flatten_paths(["a.txt", [tmp_path / "b.epub"], object(), [object()]]) == [
        "a.txt",
        str(tmp_path / "b.epub"),
    ]
    assert worker._path_priority("book.unknown") == -1

    md = calibreMetaInformation("Cover", ["A"])
    md.cover_data = {("png", b"dict-cover"): True}
    assert worker._extract_cover_payload(md) == b"dict-cover"
    md.cover_data = ("jpg", "not-bytes")
    assert worker._extract_cover_payload(md) is None

    monkeypatch.setattr(worker, "metadata_to_opf", lambda *_a, **_k: "<opf/>")
    assert worker._metadata_to_opf_bytes(calibreMetaInformation("T", ["A"]), str(tmp_path)) == b"<opf/>"

    missing = tmp_path / "missing.txt"
    assert worker.metadata_from_formats([missing]).title == "Unknown"

    readable = tmp_path / "book.txt"
    readable.write_text("body", encoding="utf-8")
    opf = tmp_path / "book.opf"
    opf.write_text("bad-opf", encoding="utf-8")

    responses = {
        "opf": RuntimeError("opf fail"),
        "txt": calibreMetaInformation("TXT Title", ["TXT Author"]),
    }

    def fake_get_file_metadata(path, force_type=False):
        """
        Perform the fake get file metadata test-helper operation with deterministic inputs.

        Example:
            Exercise test worker helpers metadata merge and import edges.fake get file metadata through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


        :param path: Value supplied for path in the focused test operation.
        :param force_type: Value supplied for force type in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        response = responses[str(force_type)]
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(worker, "get_file_metadata", fake_get_file_metadata)
    out = worker.metadata_from_formats([opf, readable])
    assert out.title == "TXT Title"
    assert _values(out.authors) == ["TXT Author"]

    class _SmartUpdateBroken:
        """
        Provide the SmartUpdateBroken test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test worker helpers metadata merge and import edges.SmartUpdateBroken through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py
        """
        title = "Fallback Title"
        authors = ("Fallback Author",)

    responses["txt"] = _SmartUpdateBroken()
    out = worker.metadata_from_formats([readable])
    assert out.title == "Fallback Title"
    assert _values(out.authors) == ["Fallback Author"]

    responses["txt"] = None
    out = worker.metadata_from_formats([readable])
    assert out.title == "Unknown"
    assert _values(out.authors) == ["Unknown"]

    responses["txt"] = worker.InvalidMetadataExtractor("skip")
    out = worker.metadata_from_formats([readable])
    assert out.title == "Unknown"
    responses["txt"] = ValueError("skip")
    out = worker.metadata_from_formats([readable])
    assert out.title == "Unknown"
    responses["txt"] = RuntimeError("logged skip")
    out = worker.metadata_from_formats([readable])
    assert out.title == "Unknown"

    source = tmp_path / "source.epub"
    source.write_text("source", encoding="utf-8")
    converted = tmp_path / "converted.mobi"
    converted.write_text("converted", encoding="utf-8")
    final_paths: list[str] = []

    monkeypatch.setattr(worker, "run_plugins_on_import", lambda _path: converted)
    monkeypatch.setattr(worker, "samefile", lambda *_a: (_ for _ in ()).throw(OSError("samefile fail")))
    monkeypatch.setattr(worker.os, "replace", lambda *_a: (_ for _ in ()).throw(OSError("replace fail")))
    worker.do_import_plugins_one_book(str(source), str(tmp_path), "group", final_paths)
    expected = tmp_path / "group" / "source.mobi"
    assert final_paths == [str(expected)]
    assert expected.read_text("utf-8") == "converted"

    final_paths.clear()
    monkeypatch.setattr(worker, "run_plugins_on_import", lambda _path: (_ for _ in ()).throw(RuntimeError("plugin fail")))
    worker.do_import_plugins_one_book(str(source), str(tmp_path), "group", final_paths)
    assert final_paths == [str(source)]


def test_worker_job_wrappers(monkeypatch) -> None:
    """
    Verify worker job wrappers.

    Example:
        Exercise test worker job wrappers through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.worker as worker
    import LiuXin_alpha.utils.ipc.simple_worker as simple_worker

    calls = []

    def fake_fork_job(module, function_name, *, args, timeout, no_output, heartbeat, abort, backend):
        """
        Perform the fake fork job test-helper operation with deterministic inputs.

        Example:
            Exercise test worker job wrappers.fake fork job through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_legacy_dispatcher_worker_edge_cases.py


        :param module: Value supplied for module in the focused test operation.
        :param function_name: Value supplied for function name in the focused test
            operation.
        :param args: Positional values forwarded by the test double.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param no_output: Value supplied for no output in the focused test operation.
        :param heartbeat: Value supplied for heartbeat in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param backend: Value supplied for backend in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append((module, function_name, args, timeout, no_output, heartbeat, abort, backend))
        return {"result": ("job-result", function_name, args)}

    monkeypatch.setattr(simple_worker, "fork_job", fake_fork_job)

    assert worker._run_in_job("read_metadata", (["a"], "1", "/tmp"), timeout=5, backend="serial") == (
        "job-result",
        "read_metadata",
        (["a"], "1", "/tmp"),
    )
    assert worker.read_metadata_in_job(["a"], "1", "/tmp", common_data={"t"}, timeout=7, backend="serial")[1] == (
        "read_metadata"
    )
    assert worker.read_metadata_bulk_in_job(True, False, ["a"], timeout=9, backend="serial")[1] == "read_metadata_bulk"
    assert calls[-1][2] == (True, False, ["a"])
