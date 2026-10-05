"""
Provide test pml modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pml modernized through a consuming regression::

        python -m pytest -q tests/file_formats/pml/test_pml_modernized.py
"""
from __future__ import annotations

import importlib
import builtins
import io
import types


class _DummyLog:
    """
    Provide the dummylog contract for validated ebook processing.

    Example:
        Exercise  DummyLog through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the dummylog state.

        Example:
            Exercise  DummyLog.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[tuple[str, str]] = []

    def debug(self, message: str, *args) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.debug through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("debug", message % args if args else message))

    def info(self, message: str, *args) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.info through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("info", message % args if args else message))

    def warning(self, message: str, *args) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.warning through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("warning", message % args if args else message))

    def warn(self, message: str, *args) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.warn through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.warning(message, *args)

    def error(self, message: str, *args) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.error through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("error", message % args if args else message))

    def __call__(self, message: str, *args) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  DummyLog.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param message: Value supplied for message under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("call", message % args if args else message))


def test_pml_modules_import_smoke() -> None:
    """
    Perform the test pml modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test pml converter toc includes x headings operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml converter toc includes x headings through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pml.pmlconverter import PML_HTMLizer

    pml = "\\xChapter 1\\x\nSome body text."
    hizer = PML_HTMLizer()
    html = hizer.parse_pml(pml, "chapter.pml")
    toc = hizer.get_toc()

    assert "<h1" in html
    assert len(toc) >= 1
    assert toc[0].text == "Chapter 1"


def test_pml_input_process_pml_handles_binary_stream_encoding_default(tmp_path) -> None:
    """
    Perform the test pml input process pml handles binary stream encoding default operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml input process pml handles binary stream encoding default through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test pml output image export without pillow is non fatal operation under explicit file-format and conversion rules.

    Example:
        Exercise test pml output image export without pillow is non fatal through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.pml_output import PMLOutput

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        """
        Perform the fake import operation under explicit file-format and conversion rules.

        Example:
            Exercise test pml output image export without pillow is non fatal.fake import through a consuming regression::

                python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test pmlml clean text handles control sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test pmlml clean text handles control sequences through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pml_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pml.pmlml import PMLMLizer

    pml = PMLMLizer(_DummyLog())
    pml.opts = types.SimpleNamespace(remove_paragraph_spacing=False)
    cleaned = pml.clean_text("line1\\c \n\\c\n\\c  \n\\c\nline2")
    assert cleaned.count("\\c") <= 2
