"""
Provide test azw4 reader and input utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test azw4 reader and input through a consuming regression::

        python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
"""
from __future__ import annotations

import io
import types
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.azw4.reader import Reader, extract_embedded_pdf_bytes, unwrap


def _sample_azw4_like_payload() -> bytes:
    """
    Perform the sample azw4 like payload operation under explicit file-format and conversion rules.

    Example:
        Exercise  sample azw4 like payload through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return b"AZW4PREFIX\x00\x01%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\nAZW4SUFFIX"


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
    """
    def __init__(self):
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages = []

    def info(self, msg):
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(msg)


def test_extract_embedded_pdf_bytes_extracts_pdf_slice() -> None:
    """
    Perform the test extract embedded pdf bytes extracts pdf slice operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract embedded pdf bytes extracts pdf slice through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    raw = _sample_azw4_like_payload()
    extracted = extract_embedded_pdf_bytes(raw)

    assert extracted.startswith(b"%PDF")
    assert extracted.rstrip().endswith(b"%%EOF")


def test_extract_embedded_pdf_bytes_raises_for_missing_pdf() -> None:
    """
    Perform the test extract embedded pdf bytes raises for missing pdf operation under explicit file-format and conversion rules.

    Example:
        Exercise test extract embedded pdf bytes raises for missing pdf through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="No embedded PDF"):
        extract_embedded_pdf_bytes(b"not-a-pdf-container")


def test_unwrap_writes_embedded_pdf(tmp_path: Path) -> None:
    """
    Perform the test unwrap writes embedded pdf operation under explicit file-format and conversion rules.

    Example:
        Exercise test unwrap writes embedded pdf through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    out = tmp_path / "out.pdf"
    unwrap(io.BytesIO(_sample_azw4_like_payload()), out)

    data = out.read_bytes()
    assert data.startswith(b"%PDF")
    assert data.rstrip().endswith(b"%%EOF")


def test_reader_extract_content_uses_pdf_plugin_and_cleans_temp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test reader extract content uses pdf plugin and cleans temp operation under explicit file-format and conversion rules.

    Example:
        Exercise test reader extract content uses pdf plugin and cleans temp through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.azw4.reader as reader_mod

    converted = {"called": False, "pdf_path": None}

    class _Option:
        """
        Provide the option contract for validated ebook processing.

        Example:
            Exercise test reader extract content uses pdf plugin and cleans temp. Option through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
        """
        def __init__(self, name, value):
            """
            Initialize and validate the option state.

            Example:
                Exercise test reader extract content uses pdf plugin and cleans temp. Option.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param value: Value normalized, stored, formatted or returned.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = value

    class _PdfPlugin:
        """
        Provide the pdfplugin contract for validated ebook processing.

        Example:
            Exercise test reader extract content uses pdf plugin and cleans temp. PdfPlugin through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
        """
        options = (_Option("new_option", 123),)

        def convert(self, stream, options, file_ext, log, accelerators):
            """
            Convert the supplied source into the stage's normalized output representation.

            Example:
                Exercise test reader extract content uses pdf plugin and cleans temp. PdfPlugin.convert through a consuming regression::

                    python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param options: Value supplied for options under the utility contract.
            :param file_ext: Value supplied for file ext under the utility contract.
            :param log: Value supplied for log under the utility contract.
            :param accelerators: Value supplied for accelerators under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            converted["called"] = True
            converted["pdf_path"] = Path(stream.name)
            payload = stream.read()
            assert payload.startswith(b"%PDF")
            assert file_ext == "pdf"
            return "metadata.opf"

    monkeypatch.setattr(reader_mod, "_plugin_for_input_format", lambda fmt: _PdfPlugin())

    options = types.SimpleNamespace(existing_option=True)
    log = _Log()
    reader = Reader(header=object(), stream=io.BytesIO(_sample_azw4_like_payload()), log=log, options=options)

    result = reader.extract_content(tmp_path)

    assert result == "metadata.opf"
    assert converted["called"] is True
    assert options.new_option == 123
    assert converted["pdf_path"] is not None
    assert not converted["pdf_path"].exists()


def test_azw4_input_plugin_delegates_to_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test azw4 input plugin delegates to reader operation under explicit file-format and conversion rules.

    Example:
        Exercise test azw4 input plugin delegates to reader through a consuming regression::

            python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.azw4_input as plugin_mod
    import sys

    calls = {}

    class _FakeReader:
        """
        Parse fakereader data into normalized ebook structures.

        Example:
            Exercise test azw4 input plugin delegates to reader. FakeReader through a consuming regression::

                python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py
        """
        def __init__(self, header, stream, log, options):
            """
            Initialize and validate the fakereader state.

            Example:
                Exercise test azw4 input plugin delegates to reader. FakeReader.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


            :param header: Value supplied for header under the utility contract.
            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param log: Value supplied for log under the utility contract.
            :param options: Value supplied for options under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            calls["reader_init"] = {
                "header": header,
                "stream": stream,
                "log": log,
                "options": options,
            }

        def extract_content(self, output_dir):
            """
            Extract content under the format's safety and compatibility rules.

            Example:
                Exercise test azw4 input plugin delegates to reader. FakeReader.extract content through a consuming regression::

                    python -m pytest -q tests/file_formats/azw4/test_azw4_reader_and_input.py


            :param output_dir: Value supplied for output dir under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            calls["output_dir"] = output_dir
            return "delegated.opf"

    fake_reader_module = types.ModuleType("LiuXin_alpha.file_formats.azw4.reader")
    fake_reader_module.Reader = _FakeReader

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.azw4.reader", fake_reader_module)
    monkeypatch.setattr(plugin_mod.os, "getcwd", lambda: "/tmp/azw4-test-cwd")

    plugin = plugin_mod.AZW4Input(None)
    stream = io.BytesIO(b"fake-stream")
    options = types.SimpleNamespace()
    log = _Log()

    result = plugin.convert(stream, options, "azw4", log, {})

    assert result == "delegated.opf"
    assert calls["reader_init"]["header"] is None
    assert calls["reader_init"]["stream"] is stream
    assert calls["reader_init"]["options"] is options
    assert calls["output_dir"] == "/tmp/azw4-test-cwd"
