"""
Provide test chm modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test chm modernized through a consuming regression::

        python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
"""
from __future__ import annotations

import io
import sys
import types

from pathlib import Path

import pytest


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
    """
    def __init__(self):
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.debug_messages = []
        self.warning_messages = []
        self.exception_messages = []

    def debug(self, msg):
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.debug_messages.append(str(msg))

    def warning(self, msg):
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.warning_messages.append(str(msg))

    def warn(self, msg):
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warn through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.warning_messages.append(str(msg))

    def exception(self, msg):
        """
        Perform the exception operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.exception through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.exception_messages.append(str(msg))

    def log_exception(self, message=None, exception=None, level=None):
        """
        Perform the log exception operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.log exception through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param exception: Value supplied for exception under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.exception_messages.append(f"{level}:{message}:{exception}")


def test_chm_modules_import_smoke() -> None:
    """
    Perform the test chm modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test chm modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import importlib

    importlib.import_module("LiuXin_alpha.file_formats.chm")
    importlib.import_module("LiuXin_alpha.file_formats.chm.reader")
    importlib.import_module("LiuXin_alpha.file_formats.chm.metadata")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.chm_input")


def test_chm_reader_init_calculates_hhc_path_without_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test chm reader init calculates hhc path without backend operation under explicit file-format and conversion rules.

    Example:
        Exercise test chm reader init calculates hhc path without backend through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.chm.reader as reader_mod

    def fake_load(self, input_path):
        """
        Perform the fake load operation under explicit file-format and conversion rules.

        Example:
            Exercise test chm reader init calculates hhc path without backend.fake load through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param self: Value supplied for self under the utility contract.
        :param input_path: Value supplied for input path under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test chm metadata extraction from home html operation under explicit file-format and conversion rules.

    Example:
        Exercise test chm metadata extraction from home html through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.chm.metadata as md_mod

    class _Reader:
        """
        Parse reader data into normalized ebook structures.

        Example:
            Exercise test chm metadata extraction from home html. Reader through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
        home = "/index.html"
        title = b"Metadata Title"
        root = "/"

        def get_home(self):
            """
            Return home under the format's safety and compatibility rules.

            Example:
                Exercise test chm metadata extraction from home html. Reader.get home through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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
            """
            Perform the GetEncoding operation under explicit file-format and conversion rules.

            Example:
                Exercise test chm metadata extraction from home html. Reader.GetEncoding through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "cp1252"

        def GetFile(self, _path):
            """
            Perform the GetFile operation under explicit file-format and conversion rules.

            Example:
                Exercise test chm metadata extraction from home html. Reader.GetFile through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param _path: Value supplied for path under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise FileNotFoundError(_path)

    mi = md_mod.get_metadata_from_reader(_Reader(), calibre=True)

    assert mi.title == "Metadata Title"
    assert mi.authors == ["Jane Doe"]
    assert mi.publisher == "Acme Press"
    assert mi.isbn == "9781234567890"
    assert "321 pages" in (mi.comments or "")


def test_chm_input_convert_glue_with_fakes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test chm input convert glue with fakes operation under explicit file-format and conversion rules.

    Example:
        Exercise test chm input convert glue with fakes through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.chm_input as chm_input_mod
    from LiuXin_alpha.metadata.utils import calibreMetaInformation

    class _Opt:
        """
        Provide the opt contract for validated ebook processing.

        Example:
            Exercise test chm input convert glue with fakes. Opt through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
        def __init__(self, name, value):
            """
            Initialize and validate the opt state.

            Example:
                Exercise test chm input convert glue with fakes. Opt.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    class _HTMLPlugin:
        """
        Provide the htmlplugin contract for validated ebook processing.

        Example:
            Exercise test chm input convert glue with fakes. HTMLPlugin through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
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
        """
        Parse fakereader data into normalized ebook structures.

        Example:
            Exercise test chm input convert glue with fakes. FakeReader through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
        re_encoded_files = set()

        def __init__(self):
            """
            Initialize and validate the fakereader state.

            Example:
                Exercise test chm input convert glue with fakes. FakeReader.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            self.closed = False

        def get_encoding(self):
            """
            Return encoding under the format's safety and compatibility rules.

            Example:
                Exercise test chm input convert glue with fakes. FakeReader.get encoding through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "cp1252"

        def CloseCHM(self):
            """
            Perform the CloseCHM operation under explicit file-format and conversion rules.

            Example:
                Exercise test chm input convert glue with fakes. FakeReader.CloseCHM through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.closed = True

    fake_reader = _FakeReader()

    def fake_chmtohtml(self, output_dir, chm_path, no_images, log, debug_dump=False):
        """
        Perform the fake chmtohtml operation under explicit file-format and conversion rules.

        Example:
            Exercise test chm input convert glue with fakes.fake chmtohtml through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


        :param self: Value supplied for self under the utility contract.
        :param output_dir: Value supplied for output dir under the utility contract.
        :param chm_path: Value supplied for chm path under the utility contract.
        :param no_images: Value supplied for no images under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param debug_dump: Value supplied for debug dump under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._chm_reader = fake_reader
        return "index.hhc"

    monkeypatch.setattr(chm_input_mod.CHMInput, "_chmtohtml", fake_chmtohtml)
    monkeypatch.setattr(chm_input_mod.CHMInput, "_create_html_root", lambda self, _m, _l, _e: ("x.html", _FakeTOC()))

    sentinel_oeb = object()
    monkeypatch.setattr(chm_input_mod.CHMInput, "_create_oebbook_html", lambda self, *_a, **_k: sentinel_oeb)

    class _FakeTOC:
        """
        Provide the faketoc contract for validated ebook processing.

        Example:
            Exercise test chm input convert glue with fakes. FakeTOC through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
        def count(self):
            """
            Perform the count operation under explicit file-format and conversion rules.

            Example:
                Exercise test chm input convert glue with fakes. FakeTOC.count through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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
    """
    Perform the test chm input create html root falls back to default topic operation under explicit file-format and conversion rules.

    Example:
        Exercise test chm input create html root falls back to default topic through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.chm_input as chm_input_mod

    plugin = chm_input_mod.CHMInput(None)

    class _Reader:
        """
        Parse reader data into normalized ebook structures.

        Example:
            Exercise test chm input create html root falls back to default topic. Reader through a consuming regression::

                python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
        """
        def relpath_to_first_html_file(self):
            """
            Perform the relpath to first html file operation under explicit file-format and conversion rules.

            Example:
                Exercise test chm input create html root falls back to default topic. Reader.relpath to first html file through a consuming regression::

                    python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "fallback/index.html"

    plugin._chm_reader = _Reader()

    htmlpath, toc = plugin._create_html_root("/definitely/missing.hhc", _Log(), "cp1252")

    assert htmlpath.endswith("fallback/index.html")
    assert toc.count() == 0
