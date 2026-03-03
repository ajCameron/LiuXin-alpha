from __future__ import annotations

import types


def test_pdf_output_plugin_variants_import_and_types() -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_qt import PDFQtOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_headless import PDFHeadlessOutput

    assert PDFOutput.file_type == "pdf"
    assert PDFQtOutput.file_type == "pdfqt"
    assert PDFHeadlessOutput.file_type == "pdfheadless"


def test_pdf_output_plugin_variants_force_engine_mode(monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_qt import PDFQtOutput
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output_headless import PDFHeadlessOutput

    seen = []

    def fake_convert(self, oeb_book, output_path, input_plugin, opts, log):
        seen.append(getattr(opts, "pdf_engine_mode", None))
        return "ok"

    monkeypatch.setattr(PDFOutput, "convert", fake_convert, raising=True)

    qt_opts = types.SimpleNamespace()
    hl_opts = types.SimpleNamespace()

    assert PDFQtOutput(None).convert(None, None, None, qt_opts, None) == "ok"
    assert PDFHeadlessOutput(None).convert(None, None, None, hl_opts, None) == "ok"
    assert seen == ["qt", "headless"]


def test_builtins_conversion_registers_pdf_plugin_variants() -> None:
    from LiuXin_alpha.customize.builtins.conversion import get_output_plugins

    names = {plugin.__name__ for plugin in get_output_plugins()}
    assert "PDFOutput" in names
    assert "PDFQtOutput" in names
    assert "PDFHeadlessOutput" in names
