"""
Provide test txt markup hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test txt markup hostile through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py
"""
from __future__ import annotations

import importlib
import sys
import types

import pytest

from tests.support.file_format_markup import (
    MARKDOWN_HOSTILE_CASES,
    TEXTILE_HOSTILE_CASES,
    assert_markup_survives,
    repeated_delimiter_payload,
)


class _Opt:
    """
    Provide the opt contract for validated ebook processing.

    Example:
        Exercise  Opt through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py
    """
    def __init__(self, name: str, val: object) -> None:
        """
        Initialize and validate the opt state.

        Example:
            Exercise  Opt.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = val


class _CapturingHTMLInput:
    """
    Convert capturinghtmlinput sources into the normalized OEB pipeline model.

    Example:
        Exercise  CapturingHTMLInput through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py
    """
    options = (_Opt("breadth_first", False), _Opt("dont_package", False))

    def __init__(self) -> None:
        """
        Initialize and validate the capturinghtmlinput state.

        Example:
            Exercise  CapturingHTMLInput.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :return: None; validated state is stored on the receiving object.
        """
        self.last_html = b""

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise  CapturingHTMLInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


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


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def debug(self, message: str, *args) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(message % args if args else message)

    def info(self, message: str, *args) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(message % args if args else message)

    def warning(self, message: str, *args) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(message % args if args else message)

    warn = warning


@pytest.fixture()
def html_input(monkeypatch) -> _CapturingHTMLInput:
    """
    Perform the html input operation under explicit file-format and conversion rules.

    Example:
        Exercise html input through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    captured = _CapturingHTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: captured if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Hostile Markup", authors=["Tester"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)
    return captured


def _txt_options(**overrides):
    """
    Perform the txt options operation under explicit file-format and conversion rules.

    Example:
        Exercise  txt options through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


    :param overrides: Value supplied for overrides under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    values = {
        "input_encoding": "utf-8",
        "paragraph_type": "auto",
        "formatting_type": "auto",
        "preserve_spaces": False,
        "txt_in_remove_indents": False,
        "markdown_extensions": "footnotes, tables, toc",
        "debug_pipeline": None,
        "verbose": 0,
        "enable_heuristics": False,
        "dehyphenate": False,
        "flow_size": 0,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("file_ext", "source", "expected_formatting"),
    (
        ("md", MARKDOWN_HOSTILE_CASES[0].source, "markdown"),
        ("markdown", MARKDOWN_HOSTILE_CASES[1].source, "markdown"),
        ("textile", TEXTILE_HOSTILE_CASES[0].source, "textile"),
    ),
)
def test_txt_input_markup_extensions_preserve_foreign_text_in_hostile_markup(
    html_input: _CapturingHTMLInput,
    file_ext: str,
    source: str,
    expected_formatting: str,
) -> None:
    """
    Perform the test txt input markup extensions preserve foreign text in hostile markup operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input markup extensions preserve foreign text in hostile markup through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


    :param html_input: Value supplied for html input under the utility contract.
    :param file_ext: Value supplied for file ext under the utility contract.
    :param source: Value supplied for source under the utility contract.
    :param expected_formatting: Value supplied for expected formatting under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    payload = source.encode("utf-8") + b"\xff\xfe"
    options = _txt_options()
    log = _Log()

    txt_input_mod.TXTInput(None).convert(types.SimpleNamespace(read=lambda: payload, seek=lambda _pos: None), options, file_ext, log, {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert_markup_survives(decoded_html, context=f"TXTInput {file_ext}")
    assert "\ufffd" in decoded_html
    assert options.paragraph_type == "off"
    assert options.formatting_type == expected_formatting


def test_txt_input_auto_detection_stays_deterministic_on_markup_delimiter_stress(
    html_input: _CapturingHTMLInput,
) -> None:
    """
    Perform the test txt input auto detection stays deterministic on markup delimiter stress operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input auto detection stays deterministic on markup delimiter stress through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_markup_hostile.py


    :param html_input: Value supplied for html input under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    payload = repeated_delimiter_payload().encode("utf-8")
    rendered_outputs: list[str] = []
    detected_formats: list[str] = []

    for _ in range(2):
        options = _txt_options(paragraph_type="auto", formatting_type="auto")
        txt_input_mod.TXTInput(None).convert(
            types.SimpleNamespace(read=lambda payload=payload: payload, seek=lambda _pos: None),
            options,
            "txt",
            _Log(),
            {},
        )
        decoded_html = html_input.last_html.decode("utf-8", "replace")
        rendered_outputs.append(decoded_html)
        detected_formats.append(options.formatting_type)
        assert_markup_survives(decoded_html, context="TXTInput delimiter stress")
        assert options.formatting_type in {"markdown", "textile", "heuristic"}

    assert rendered_outputs[0] == rendered_outputs[1]
    assert detected_formats[0] == detected_formats[1]
