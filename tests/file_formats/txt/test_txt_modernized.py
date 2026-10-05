"""
Provide test txt modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test txt modernized through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import importlib
import io
import sys
import types
import zipfile
from pathlib import Path

import pytest


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, *parts) -> None:
        """
        Perform the record operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log. record through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param level: Value supplied for level under the utility contract.
        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append((level, " ".join(str(x) for x in parts)))

    def __call__(self, *parts) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("call", *parts)

    def debug(self, *parts) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("debug", *parts)

    def info(self, *parts) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("info", *parts)

    def warn(self, *parts) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warn through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("warn", *parts)

    def warning(self, *parts) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("warning", *parts)

    def error(self, *parts) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.error through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record("error", *parts)


def test_txt_modules_import_smoke() -> None:
    """
    Perform the test txt modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    modules = (
        "LiuXin_alpha.file_formats.txt",
        "LiuXin_alpha.file_formats.txt.processor",
        "LiuXin_alpha.file_formats.txt.txtml",
        "LiuXin_alpha.file_formats.txt.markdownml",
        "LiuXin_alpha.file_formats.txt.textileml",
        "LiuXin_alpha.file_formats.conversion.plugins.txt_input",
        "LiuXin_alpha.file_formats.conversion.plugins.txt_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_txt_processor_clean_and_detect_basics() -> None:
    """
    Perform the test txt processor clean and detect basics operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt processor clean and detect basics through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    cleaned = mod.clean_txt(b"  alpha\r\n\r\n\x01beta")
    assert isinstance(cleaned, str)
    assert "\r" not in cleaned
    assert "\x01" not in cleaned

    assert mod.detect_paragraph_type("") == "single"
    assert mod.detect_formatting_type("plain text only\nno markup") == "heuristic"


def test_txt_processor_split_and_format_detection() -> None:
    """
    Perform the test txt processor split and format detection operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt processor split and format detection through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    src = ("Sentence one. " * 3000).strip()
    out = mod.split_txt(src, epub_split_size_kb=2)
    assert isinstance(out, str)
    assert len(out) > 0

    markdownish = "\n".join(
        [
            "# H1",
            "## H2",
            "### H3",
            "![img](a.png)",
            "[x](https://example.com)",
            "----",
            "====",
        ]
    )
    textileish = "\n".join(
        [
            "h1. One",
            "h2. Two",
            "h3. Three",
            '\"link\":https://example.com',
            "bq. quote",
            "p. para",
            "!img.png!",
        ]
    )
    assert mod.detect_formatting_type(markdownish) == "markdown"
    assert mod.detect_formatting_type(textileish) == "textile"


def test_txt_input_convert_plain_and_txtz_smoke(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test txt input convert plain and txtz smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input convert plain and txtz smoke through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")

    class _Opt:
        """
        Provide the opt contract for validated ebook processing.

        Example:
            Exercise test txt input convert plain and txtz smoke. Opt through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
        """
        def __init__(self, name, val):
            """
            Initialize and validate the opt state.

            Example:
                Exercise test txt input convert plain and txtz smoke. Opt.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param val: Template or metadata value evaluated by the operation.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = val

    class _HTMLInput:
        """
        Convert htmlinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test txt input convert plain and txtz smoke. HTMLInput through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
        """
        options = (_Opt("breadth_first", False), _Opt("dont_package", False))

        def __init__(self):
            """
            Initialize and validate the htmlinput state.

            Example:
                Exercise test txt input convert plain and txtz smoke. HTMLInput.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            self.last_html = b""

        def convert(self, stream, options, file_ext, log, accelerators):
            """
            Convert the supplied source into the stage's normalized output representation.

            Example:
                Exercise test txt input convert plain and txtz smoke. HTMLInput.convert through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param stream: Input or output stream wrapped by the terminal or compatibility
                layer.
            :param options: Value supplied for options under the utility contract.
            :param file_ext: Value supplied for file ext under the utility contract.
            :param log: Value supplied for log under the utility contract.
            :param accelerators: Value supplied for accelerators under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            self.last_html = stream.read()
            return types.SimpleNamespace(metadata=types.SimpleNamespace())

    html_input = _HTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: html_input if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="TXT Smoke", authors=["A"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)

    plugin = txt_input_mod.TXTInput(None)
    options = types.SimpleNamespace(
        input_encoding="utf-8",
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="footnotes, tables, toc",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
        flow_size=0,
    )

    plain = tmp_path / "book.txt"
    plain.write_bytes("Hello Ω 世界".encode("utf-8"))
    with plain.open("rb") as stream:
        out = plugin.convert(stream, options, "txt", _Log(), {})
    assert out is not None
    assert b"Hello \xce\xa9" in html_input.last_html

    txtz = tmp_path / "book.txtz"
    with zipfile.ZipFile(txtz, "w") as zf:
        zf.writestr("x.txt", "Line A\n\nLine B")
    with txtz.open("rb") as stream:
        out2 = plugin.convert(stream, options, "txtz", _Log(), {})
    assert out2 is not None
    assert b"Line A" in html_input.last_html


def test_txt_output_convert_handles_write_only_stream(monkeypatch) -> None:
    """
    Perform the test txt output convert handles write only stream operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt output convert handles write only stream through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        """
        Provide the txtmlizer contract for validated ebook processing.

        Example:
            Exercise test txt output convert handles write only stream. TXTMLizer through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
        """
        def __init__(self, _log):
            """
            Initialize and validate the txtmlizer state.

            Example:
                Exercise test txt output convert handles write only stream. TXTMLizer.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param _log: Value supplied for log under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            pass

        def extract_content(self, _oeb, _opts):
            """
            Extract content under the format's safety and compatibility rules.

            Example:
                Exercise test txt output convert handles write only stream. TXTMLizer.extract content through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param _oeb: Value supplied for oeb under the utility contract.
            :param _opts: Value supplied for opts under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Unicode Ω 世界 👩🏽‍💻"

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    class _WriteOnly:
        """
        Provide the writeonly contract for validated ebook processing.

        Example:
            Exercise test txt output convert handles write only stream. WriteOnly through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
        """
        def __init__(self):
            """
            Initialize and validate the writeonly state.

            Example:
                Exercise test txt output convert handles write only stream. WriteOnly.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            self.data = b""

        def write(self, payload: bytes):
            """
            Perform the write operation under explicit file-format and conversion rules.

            Example:
                Exercise test txt output convert handles write only stream. WriteOnly.write through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param payload: Value supplied for payload under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.data += payload

    sink = _WriteOnly()
    opts = types.SimpleNamespace(
        txt_output_formatting="plain",
        newline="unix",
        txt_output_encoding="utf-8",
        remove_paragraph_spacing=False,
        max_line_length=0,
        force_max_line_length=False,
        inline_toc=False,
    )
    txt_output_mod.TXTOutput(None).convert(object(), sink, None, opts, _Log())
    assert b"Unicode \xce\xa9" in sink.data


def test_txt_input_does_not_leave_root_index_files_on_failure(
    monkeypatch,
    project_root: Path,
) -> None:
    """
    Perform the test txt input does not leave root index files on failure operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input does not leave root index files on failure through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param project_root: Value supplied for project root under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")

    class _Opt:
        """
        Provide the opt contract for validated ebook processing.

        Example:
            Exercise test txt input does not leave root index files on failure. Opt through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
        """
        def __init__(self, name, val):
            """
            Initialize and validate the opt state.

            Example:
                Exercise test txt input does not leave root index files on failure. Opt.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param val: Template or metadata value evaluated by the operation.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = val

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")

    def _raise_convert(stream, options, file_ext, log, accelerators):
        """
        Perform the raise convert operation under explicit file-format and conversion rules.

        Example:
            Exercise test txt input does not leave root index files on failure. raise convert through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("forced txt->html failure")

    fake_ui.plugin_for_input_format = lambda fmt: types.SimpleNamespace(
        options=(_Opt("breadth_first", False), _Opt("dont_package", False)),
        convert=_raise_convert,
    )
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="TXT Smoke", authors=["A"])
    )

    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)
    monkeypatch.chdir(project_root)

    plugin = txt_input_mod.TXTInput(None)
    options = types.SimpleNamespace(
        input_encoding="utf-8",
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="footnotes, tables, toc",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
        flow_size=0,
    )

    before = {p.name for p in project_root.glob("index*.html")}
    with io.BytesIO(b"hello from memory stream") as stream:
        with pytest.raises(RuntimeError, match="forced txt->html failure"):
            plugin.convert(stream, options, "txt", _Log(), {})
    after = {p.name for p in project_root.glob("index*.html")}

    assert after == before
