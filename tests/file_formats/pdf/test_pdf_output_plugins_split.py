"""
Provide test pdf output plugins split utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdf output plugins split through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_output_plugins_split.py
"""
from __future__ import annotations

import types


def test_pdf_output_plugin_variants_import_and_types() -> None:
    """
    Perform the test pdf output plugin variants import and types operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdf output plugin variants import and types through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_output_plugins_split.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_qt import PDFQtOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_headless import PDFHeadlessOutput

    assert PDFOutput.file_type == "pdf"
    assert PDFQtOutput.file_type == "pdfqt"
    assert PDFHeadlessOutput.file_type == "pdfheadless"


def test_pdf_output_plugin_variants_force_engine_mode(monkeypatch) -> None:
    """
    Perform the test pdf output plugin variants force engine mode operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdf output plugin variants force engine mode through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_output_plugins_split.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_qt import PDFQtOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_headless import PDFHeadlessOutput

    seen = []

    def fake_convert(self, oeb_book, output_path, input_plugin, opts, log):
        """
        Perform the fake convert operation under explicit file-format and conversion rules.

        Example:
            Exercise test pdf output plugin variants force engine mode.fake convert through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_output_plugins_split.py


        :param self: Value supplied for self under the utility contract.
        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_path: Value supplied for output path under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        seen.append(getattr(opts, "pdf_engine_mode", None))
        return "ok"

    monkeypatch.setattr(PDFOutput, "convert", fake_convert, raising=True)

    qt_opts = types.SimpleNamespace()
    hl_opts = types.SimpleNamespace()

    assert PDFQtOutput(None).convert(None, None, None, qt_opts, None) == "ok"
    assert PDFHeadlessOutput(None).convert(None, None, None, hl_opts, None) == "ok"
    assert seen == ["qt", "headless"]


def test_builtins_conversion_registers_pdf_plugin_variants() -> None:
    """
    Perform the test builtins conversion registers pdf plugin variants operation under explicit file-format and conversion rules.

    Example:
        Exercise test builtins conversion registers pdf plugin variants through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_output_plugins_split.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.customize.builtins.conversion import get_output_plugins

    names = {plugin.__name__ for plugin in get_output_plugins()}
    assert "PDFOutput" in names
    assert "PDFQtOutput" in names
    assert "PDFHeadlessOutput" in names
