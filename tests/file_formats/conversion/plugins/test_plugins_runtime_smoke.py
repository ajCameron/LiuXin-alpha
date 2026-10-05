"""
Provide test plugins runtime smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test plugins runtime smoke through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import sys
import types
import zipfile

from pathlib import Path


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log. record through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def debug(self, *parts) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def info(self, *parts) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def warn(self, *parts) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warn through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def warning(self, *parts) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def exception(self, *parts) -> None:
        """
        Perform the exception operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.exception through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)


class _Option:
    """
    Provide the option contract for validated ebook processing.

    Example:
        Exercise  Option through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    def __init__(self, name: str, value) -> None:
        """
        Initialize and validate the option state.

        Example:
            Exercise  Option.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; validated state is stored on the receiving object.
        """
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = value


class _HTMLInputStub:
    """
    Provide the htmlinputstub contract for validated ebook processing.

    Example:
        Exercise  HTMLInputStub through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    options = (_Option("breadth_first", False), _Option("dont_package", False))

    def __init__(self, returned_oeb):
        """
        Initialize and validate the htmlinputstub state.

        Example:
            Exercise  HTMLInputStub.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param returned_oeb: Value supplied for returned oeb under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._returned_oeb = returned_oeb

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise  HTMLInputStub.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._returned_oeb


def _install_html_pipeline_stubs(monkeypatch, returned_oeb) -> None:
    """
    Perform the install html pipeline stubs operation under explicit file-format and conversion rules.

    Example:
        Exercise  install html pipeline stubs through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param returned_oeb: Value supplied for returned oeb under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: _HTMLInputStub(returned_oeb) if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Smoke Title", authors=["Smoke Author"])
    )

    fake_oeb_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_oeb_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_oeb_meta)


def test_txt_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test txt input convert smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input convert smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.txt_input as txt_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    source = tmp_path / "book.txt"
    source.write_bytes(b"Smoke test paragraph.\n\nAnother line.")

    options = types.SimpleNamespace(
        input_encoding=None,
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
    )
    plugin = txt_input_mod.TXTInput(None)
    log = _Log()

    with source.open("rb") as stream:
        out = plugin.convert(stream, options, "txt", log, {})

    assert out is fake_oeb
    assert plugin.html_postprocess_title == "Smoke Title"


def test_txt_input_textile_fallback_smoke(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test txt input textile fallback smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input textile fallback smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.txt_input as txt_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    source = tmp_path / "book.textile"
    source.write_bytes(b"h1. Smoke textile\\n\\nSimple body")

    options = types.SimpleNamespace(
        input_encoding=None,
        paragraph_type="off",
        formatting_type="textile",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
    )
    plugin = txt_input_mod.TXTInput(None)
    log = _Log()

    with source.open("rb") as stream:
        out = plugin.convert(stream, options, "textile", log, {})

    assert out is fake_oeb
    assert plugin.html_postprocess_title == "Smoke Title"


def test_htmlz_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test htmlz input convert smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test htmlz input convert smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.htmlz_input as htmlz_input_mod

    fake_oeb = types.SimpleNamespace(metadata=types.SimpleNamespace())
    _install_html_pipeline_stubs(monkeypatch, fake_oeb)

    archive = tmp_path / "book.htmlz"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("index.html", "<html><body><p>Smoke</p></body></html>")

    options = types.SimpleNamespace(input_encoding=None, debug_pipeline=None)
    plugin = htmlz_input_mod.HTMLZInput(None)
    log = _Log()

    monkeypatch.chdir(tmp_path)
    with archive.open("rb") as stream:
        out = plugin.convert(stream, options, "htmlz", log, {})

    assert out is fake_oeb


def test_epub_input_convert_smoke(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test epub input convert smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test epub input convert smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.epub_input as epub_input_mod

    epub_path = tmp_path / "book.epub"
    with zipfile.ZipFile(epub_path, "w") as zf:
        mimetype = zipfile.ZipInfo("mimetype")
        mimetype.compress_type = zipfile.ZIP_STORED
        zf.writestr(mimetype, b"application/epub+zip")
        zf.writestr(
            "META-INF/container.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
""",
        )
        zf.writestr(
            "content.opf",
            """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>EPUB Smoke</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Smoke Author</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:12121212-3434-5656-7878-909090909090</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap1"/>
  </spine>
</package>
""",
        )
        zf.writestr("text/chap1.xhtml", "<html xmlns='http://www.w3.org/1999/xhtml'><body><p>Smoke</p></body></html>")
        zf.writestr(
            "toc.ncx",
            """<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>EPUB Smoke</text></docTitle>
  <navMap/>
</ncx>
""",
        )

    plugin = epub_input_mod.EPUBInput(None)
    options = types.SimpleNamespace(input_encoding=None, debug_pipeline=None)
    log = _Log()

    monkeypatch.chdir(tmp_path)
    with epub_path.open("rb") as stream:
        out = plugin.convert(stream, options, "epub", log, {})

    assert Path(out).exists()
    assert Path(out).name == "content.opf"
