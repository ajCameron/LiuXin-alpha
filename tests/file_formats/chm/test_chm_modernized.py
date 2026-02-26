from __future__ import annotations

import io
import sys
import types

from pathlib import Path

import pytest


class _Log:
    def __init__(self):
        self.debug_messages = []
        self.warning_messages = []
        self.exception_messages = []

    def debug(self, msg):
        self.debug_messages.append(str(msg))

    def warning(self, msg):
        self.warning_messages.append(str(msg))

    def warn(self, msg):
        self.warning_messages.append(str(msg))

    def exception(self, msg):
        self.exception_messages.append(str(msg))

    def log_exception(self, message=None, exception=None, level=None):
        self.exception_messages.append(f"{level}:{message}:{exception}")


def test_chm_modules_import_smoke() -> None:
    import importlib

    importlib.import_module("LiuXin_alpha.file_formats.chm")
    importlib.import_module("LiuXin_alpha.file_formats.chm.reader")
    importlib.import_module("LiuXin_alpha.file_formats.chm.metadata")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.chm_input")


def test_chm_reader_init_calculates_hhc_path_without_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.chm.reader as reader_mod

    def fake_load(self, input_path):
        self.filename = input_path
        self.file = object()
        self.home = "/book/home.html"
        self.topics = "/book/contents.hhc"
        self.title = "Demo"
        self.encoding = "ANSI,0,0"
        return True

    monkeypatch.setattr(reader_mod.CHMFile, "LoadCHM", fake_load)

    rdr = reader_mod.CHMReader("dummy.chm", _Log())
    assert rdr.hhc_path == "book/contents.hhc"
    assert rdr.get_encoding() in {"iso8859_1", "cp1252"}


def test_chm_metadata_extraction_from_home_html() -> None:
    import LiuXin_alpha.file_formats.chm.metadata as md_mod

    class _Reader:
        home = "/index.html"
        title = b"Metadata Title"
        root = "/"

        def get_home(self):
            return b"""
            <html><body>
                <span class='author'>Jane Doe</span>
                <span class='imprint'>Acme Press</span>
                <span class='isbn'>9781234567890</span>
                <span class='cwdate'>2024</span>
                <span class='pages'>(321 pages)</span>
            </body></html>
            """

        def GetEncoding(self):
            return "cp1252"

        def GetFile(self, _path):
            raise FileNotFoundError(_path)

    mi = md_mod.get_metadata_from_reader(_Reader(), calibre=True)

    assert mi.title == "Metadata Title"
    assert mi.authors == ["Jane Doe"]
    assert mi.publisher == "Acme Press"
    assert mi.isbn == "9781234567890"
    assert "321 pages" in (mi.comments or "")


def test_chm_input_convert_glue_with_fakes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.chm_input as chm_input_mod
    from LiuXin_alpha.metadata.utils import calibreMetaInformation

    class _Opt:
        def __init__(self, name, value):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    class _HTMLPlugin:
        options = (_Opt("breadth_first", False),)

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: _HTMLPlugin() if fmt == "html" else None

    fake_metadata_mod = types.ModuleType("LiuXin_alpha.file_formats.chm.metadata")
    fake_metadata_mod.get_metadata_from_reader = lambda _rdr, calibre=True: calibreMetaInformation(
        "Stub Title", ["Stub Author"]
    )

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.chm.metadata", fake_metadata_mod)

    class _FakeReader:
        re_encoded_files = set()

        def __init__(self):
            self.closed = False

        def get_encoding(self):
            return "cp1252"

        def CloseCHM(self):
            self.closed = True

    fake_reader = _FakeReader()

    def fake_chmtohtml(self, output_dir, chm_path, no_images, log, debug_dump=False):
        self._chm_reader = fake_reader
        return "index.hhc"

    monkeypatch.setattr(chm_input_mod.CHMInput, "_chmtohtml", fake_chmtohtml)
    monkeypatch.setattr(chm_input_mod.CHMInput, "_create_html_root", lambda self, _m, _l, _e: ("x.html", _FakeTOC()))

    sentinel_oeb = object()
    monkeypatch.setattr(chm_input_mod.CHMInput, "_create_oebbook_html", lambda self, *_a, **_k: sentinel_oeb)

    class _FakeTOC:
        def count(self):
            return 0

    stream_path = tmp_path / "in.chm"
    stream_path.write_bytes(b"dummy")

    options = types.SimpleNamespace(input_encoding=None, debug_pipeline=None)
    plugin = chm_input_mod.CHMInput(None)

    with stream_path.open("rb") as stream:
        out = plugin.convert(stream, options, "chm", _Log(), {})

    assert out is sentinel_oeb
    assert fake_reader.closed is True


def test_chm_input_create_html_root_falls_back_to_default_topic() -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.chm_input as chm_input_mod

    plugin = chm_input_mod.CHMInput(None)

    class _Reader:
        def relpath_to_first_html_file(self):
            return "fallback/index.html"

    plugin._chm_reader = _Reader()

    htmlpath, toc = plugin._create_html_root("/definitely/missing.hhc", _Log(), "cp1252")

    assert htmlpath.endswith("fallback/index.html")
    assert toc.count() == 0
