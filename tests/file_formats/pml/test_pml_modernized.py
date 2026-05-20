from __future__ import annotations

import importlib
import builtins
import io
import types


class _DummyLog:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def debug(self, message: str, *args) -> None:
        self.messages.append(("debug", message % args if args else message))

    def info(self, message: str, *args) -> None:
        self.messages.append(("info", message % args if args else message))

    def warning(self, message: str, *args) -> None:
        self.messages.append(("warning", message % args if args else message))

    def warn(self, message: str, *args) -> None:
        self.warning(message, *args)

    def error(self, message: str, *args) -> None:
        self.messages.append(("error", message % args if args else message))

    def __call__(self, message: str, *args) -> None:
        self.messages.append(("call", message % args if args else message))


def test_pml_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.pml",
        "LiuXin_alpha.file_formats.pml.pmlconverter",
        "LiuXin_alpha.file_formats.pml.pmlml",
        "LiuXin_alpha.file_formats.conversion.plugins.pml_input",
        "LiuXin_alpha.file_formats.conversion.plugins.pml_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_pml_converter_toc_includes_x_headings() -> None:
    from LiuXin_alpha.file_formats.pml.pmlconverter import PML_HTMLizer

    pml = "\\xChapter 1\\x\nSome body text."
    hizer = PML_HTMLizer()
    html = hizer.parse_pml(pml, "chapter.pml")
    toc = hizer.get_toc()

    assert "<h1" in html
    assert len(toc) >= 1
    assert toc[0].text == "Chapter 1"


def test_pml_input_process_pml_handles_binary_stream_encoding_default(tmp_path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.pml_input import PMLInput

    plugin = PMLInput(None)
    plugin.options = types.SimpleNamespace(input_encoding=None)
    plugin.log = _DummyLog()

    pml_stream = io.BytesIO(b'\\x="A"Title\\x\\nBody')
    out_html = tmp_path / "index.html"

    toc = plugin.process_pml(pml_stream, str(out_html))
    raw = out_html.read_text(encoding="utf-8")
    assert "<html>" in raw
    assert len(toc) >= 1


def test_pml_output_image_export_without_pillow_is_non_fatal(monkeypatch, tmp_path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.pml_output import PMLOutput

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "PIL" or name.startswith("PIL."):
            raise ImportError("Pillow intentionally hidden for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    plugin = PMLOutput(None)
    plugin.log = _DummyLog()
    opts = types.SimpleNamespace(full_image_depth=False)
    manifest = [types.SimpleNamespace(media_type="image/png", href="cover.png", data=b"not-a-real-image")]
    plugin.write_images(manifest, {"cover.png": "cover.png"}, str(tmp_path), opts)

    assert any("Pillow not available" in msg for level, msg in plugin.log.messages if level == "warning")


def test_pmlml_clean_text_handles_control_sequences() -> None:
    from LiuXin_alpha.file_formats.pml.pmlml import PMLMLizer

    pml = PMLMLizer(_DummyLog())
    pml.opts = types.SimpleNamespace(remove_paragraph_spacing=False)
    cleaned = pml.clean_text("line1\\c \n\\c\n\\c  \n\\c\nline2")
    assert cleaned.count("\\c") <= 2
