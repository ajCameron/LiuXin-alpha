from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


def _values(raw):
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
    vals = _values(raw)
    return vals[0] if vals else None


def _pml_comment(**fields: str) -> bytes:
    inner = " ".join(f'{key}="{value}"' for key, value in fields.items())
    return f"\\v{inner}\\v".encode("utf-8")


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return out.getvalue()


class _SeekTellBroken(io.BytesIO):
    def tell(self):
        raise OSError("tell unavailable")

    def seek(self, *args, **kwargs):
        raise OSError("seek unavailable")


def test_lit_private_helpers_and_cover_resolution_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    events: list[tuple[str, str]] = []

    class _Logger:
        @staticmethod
        def warning(message):
            events.append(("warning", message))

        @staticmethod
        def info(message):
            events.append(("info", message))

        @staticmethod
        def debug(message):
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
        def __init__(self, path: str, internal: str | None) -> None:
            self.path = path
            self.internal = internal

    class _LitFile:
        manifest = {
            "blank": _ManifestItem("", "ignored"),
            "missing-internal": _ManifestItem("missing-internal.jpg", None),
            "casefold": _ManifestItem("images/café & cover.jpg", "cover-internal"),
            "empty": _ManifestItem("empty.jpg", "empty"),
            "broken": _ManifestItem("broken.jpg", "broken"),
        }

        @staticmethod
        def get_file(path: str):
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
    import LiuXin_alpha.metadata.file_sources.lit as lit_md

    with pytest.raises(TypeError, match="target_file"):
        lit_md.get_metadata(123)  # type: ignore[arg-type]

    class _Container:
        def __init__(self, _stream, _log) -> None:
            pass

        @staticmethod
        def get_metadata() -> bytes:
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
        @staticmethod
        def warning(message):
            assert "Failed to read metadata from LIT file" in message

    class _BrokenContainer:
        def __init__(self, _stream, _log) -> None:
            raise RuntimeError("broken")

    monkeypatch.setattr(lit_md, "default_log", _NoLogException())
    monkeypatch.setattr(lit_md, "_load_lit_container_class", lambda: _BrokenContainer)
    md = lit_md.read_metadata_from_stream(io.BytesIO(b"bad"), source_name="broken.lit")
    assert md.title == "broken"


def test_pml_private_helpers_sources_and_author_fallbacks(tmp_path: Path, monkeypatch) -> None:
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
        def __init__(self) -> None:
            self.calls: list[str] = []

        @property
        def authors(self):
            raise RuntimeError("current unavailable")

        @authors.setter
        def authors(self, value):
            self.calls.append(value)
            if value == "Stop":
                raise RuntimeError("stop")

    target = _AuthorSetter()
    pml_md._set_authors(target, ["Alice", "Stop", "Ignored"])
    assert target.calls == [[], "Alice", "Stop"]
    pml_md._set_authors(target, [])
    assert target.calls == [[], "Alice", "Stop"]

    class _DataBroken:
        @property
        def _data(self):
            raise RuntimeError("no raw data")

        @property
        def authors(self):
            return ("Unknown",)

        @authors.setter
        def authors(self, _value):
            raise RuntimeError("cannot set")

    pml_md._clear_default_authors(_DataBroken())


def test_pml_zip_cover_payload_and_parse_error_edges(monkeypatch) -> None:
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
    assert pml_md._extract_pmlz_payload(b"not-a-zip", source_name="", extract_cover=True) == (b"", None)

    class _FakeZip:
        def __init__(self) -> None:
            self.calls = 0

        @staticmethod
        def namelist():
            return ["main_img/cover.png", "z/cover.png"]

        def read(self, name):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first read fails")
            return b"fallback-cover"

    assert pml_md._read_cover_from_zip(_FakeZip(), source_name="main.pmlz", pml_entries=[]) == b"fallback-cover"

    class _BadZip:
        @staticmethod
        def namelist():
            return ["book.pml", "cover.png"]

        @staticmethod
        def read(_name):
            raise RuntimeError("always fails")

    assert pml_md._read_cover_from_zip(_BadZip(), source_name="", pml_entries=["book.pml"]) is None

    class _BrokenGetCover:
        pass

    monkeypatch.setattr(pml_md, "get_cover", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("cover fail")))
    md = pml_md.get_metadata(io.BytesIO(_pml_comment(TITLE="No Cover Error")), extract_cover=True)
    assert md.title == "No Cover Error"
    assert pml_md.get_metadata(object()).title == "Unknown"
    assert pml_md.get_metadata_inplace(io.BytesIO(_pml_comment(TITLE="Inplace")), extract_cover=False).title == "Inplace"


def test_haodoo_author_normalization_and_reader_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.pdb.haodoo as haodoo_md

    assert haodoo_md._normalize_authors(["Alice", "", "Bob"]) == ["Alice", "Bob"]
    assert haodoo_md._normalize_authors({"Alice": 1, "": 2}) == ["Alice"]
    assert haodoo_md._normalize_authors("Solo") == ["Solo"]
    assert haodoo_md._normalize_authors("") == []
    assert haodoo_md._normalize_authors(42) == ["42"]
    assert haodoo_md._normalize_authors(None) == []

    class _Header:
        ident = "BOOKMTIT"
        title = "Header Title"

    class _Reader:
        def __init__(self, pheader, stream, log, extra) -> None:
            assert pheader.ident == b"BOOKMTIT"
            assert stream.tell() == 0
            assert extra is None

        @staticmethod
        def get_metadata():
            return SimpleNamespace(title="", authors={"Ada": 1, "李白": 2}, language="ZH_CN")

    monkeypatch.setattr(haodoo_md, "PdbHeaderReader", lambda _stream: _Header())
    monkeypatch.setattr(haodoo_md, "Reader", _Reader)

    md = haodoo_md.get_metadata(io.BytesIO(b"pdb"), extract_cover=False)
    assert md.title == "Header Title"
    assert _values(md.authors) == ["Ada", "李白"]
    assert _first(md.language) == "ZH_CN"

    class _BrokenReader:
        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("reader fail")

    monkeypatch.setattr(haodoo_md, "Reader", _BrokenReader)
    fallback = haodoo_md.get_metadata(io.BytesIO(b"pdb"), extract_cover=True)
    assert fallback.title == "Header Title"
    assert _values(fallback.authors) == ["Unknown"]


def test_dispatcher_plugin_adapter_and_failure_edges(tmp_path: Path, monkeypatch) -> None:
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
        file_types = ["txt", "html"]
        inplace_run_cost = "medium"
        __module__ = "fake.plugins"

        def __init__(self, _context) -> None:
            pass

        @staticmethod
        def get_metadata_inplace(path, ftype):
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
        file_types = ["bin"]

        def __init__(self, _context) -> None:
            pass

        @staticmethod
        def get_metadata(stream=None, ftype=None):
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
        module_name = "NoneAdapter"
        file_path = "none.py"

        @staticmethod
        def get_metadata(_target, force_type=None):
            return None

    class _FailAdapter:
        module_name = "FailAdapter"
        file_path = "fail.py"

        @staticmethod
        def get_metadata(_target, force_type=None):
            raise RuntimeError("reader failed")

    monkeypatch.setattr(dispatcher, "get_plugins_for_extension", lambda _ext: [_NoneAdapter()])
    assert dispatcher.get_metadata(io.BytesIO(b"data"), force_type="txt") is None
    monkeypatch.setattr(dispatcher, "get_plugins_for_extension", lambda _ext: [_FailAdapter()])
    with pytest.raises(RuntimeError, match="FailAdapter"):
        dispatcher.get_metadata(io.BytesIO(b"data"), force_type="txt")


def test_worker_helpers_metadata_merge_and_import_edges(tmp_path: Path, monkeypatch) -> None:
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
        response = responses[str(force_type)]
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(worker, "get_file_metadata", fake_get_file_metadata)
    out = worker.metadata_from_formats([opf, readable])
    assert out.title == "TXT Title"
    assert _values(out.authors) == ["TXT Author"]

    class _SmartUpdateBroken:
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
    import LiuXin_alpha.metadata.file_sources.worker as worker
    import LiuXin_alpha.utils.ipc.simple_worker as simple_worker

    calls = []

    def fake_fork_job(module, function_name, *, args, timeout, no_output, heartbeat, abort, backend):
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
