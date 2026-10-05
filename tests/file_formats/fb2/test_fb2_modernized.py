"""
Provide test fb2 modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test fb2 modernized through a consuming regression::

        python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
"""
from __future__ import annotations

import base64
import importlib
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

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log. record through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)

    def error(self, *parts) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.error through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


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

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)


def _minimal_fb2() -> bytes:
    """
    Perform the minimal fb2 operation under explicit file-format and conversion rules.

    Example:
        Exercise  minimal fb2 through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return b"""<?xml version='1.0' encoding='utf-8'?>
<FictionBook xmlns='http://www.gribuser.ru/xml/fictionbook/2.0'
             xmlns:l='http://www.w3.org/1999/xlink'>
  <description>
    <title-info>
      <book-title>FB2 Smoke</book-title>
      <author><first-name>Smoke</first-name><last-name>Author</last-name></author>
      <lang>en</lang>
    </title-info>
  </description>
  <body>
    <section><title><p>Chapter</p></title><p>Hello FB2.</p></section>
  </body>
</FictionBook>
"""


def test_fb2_modules_import_smoke() -> None:
    """
    Perform the test fb2 modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2 modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.fb2")
    importlib.import_module("LiuXin_alpha.file_formats.fb2.archive")
    importlib.import_module("LiuXin_alpha.file_formats.fb2.fb2ml")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_output")


def test_fb2_base64_decode_roundtrip() -> None:
    """
    Perform the test fb2 base64 decode roundtrip operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2 base64 decode roundtrip through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.fb2 import base64_decode

    raw = b"Smoke decode payload"
    encoded = base64.b64encode(raw)

    assert base64_decode(encoded) == raw


def test_fb2_input_extract_embedded_content_writes_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test fb2 input extract embedded content writes files operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2 input extract embedded content writes files through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    payload = b"\x89PNG\r\n\x1a\nsmoke"
    encoded = base64.b64encode(payload).decode("ascii")
    xml = (
        "<FictionBook xmlns='http://www.gribuser.ru/xml/fictionbook/2.0'>"
        f"<binary id='cover' content-type='image/png'>{encoded}</binary>"
        "</FictionBook>"
    ).encode("utf-8")

    plugin = FB2Input(None)
    plugin.log = _Log()

    monkeypatch.chdir(tmp_path)
    plugin.extract_embedded_content(etree.fromstring(xml))

    extracted = tmp_path / "cover.png"
    assert extracted.exists()
    assert extracted.read_bytes() == payload
    assert plugin.binary_map["cover"] == "cover.png"


def test_fb2mlizer_images_preserve_original_mime_when_not_converted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Perform the test fb2mlizer images preserve original mime when not converted operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2mlizer images preserve original mime when not converted through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.fb2.fb2ml as fb2ml_mod

    item = types.SimpleNamespace(
        href="images/cover.png",
        media_type="image/png",
        data=b"\x89PNG\r\n\x1a\n" + b"a" * 200,
    )
    mlizer = fb2ml_mod.FB2MLizer(_Log())
    mlizer.oeb_book = types.SimpleNamespace(manifest=[item])
    mlizer.image_hrefs = {"images/cover.png": "_0.jpg"}

    monkeypatch.setattr(fb2ml_mod, "_convert_to_jpeg", lambda data, quality=70: None)

    out = mlizer.fb2mlize_images()
    assert 'content-type="image/png"' in out
    assert 'id="_0.jpg"' in out


def test_fb2mlizer_images_switch_to_jpeg_when_converter_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Perform the test fb2mlizer images switch to jpeg when converter available operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2mlizer images switch to jpeg when converter available through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.fb2.fb2ml as fb2ml_mod

    item = types.SimpleNamespace(href="images/cover.png", media_type="image/png", data=b"rawpng")
    mlizer = fb2ml_mod.FB2MLizer(_Log())
    mlizer.oeb_book = types.SimpleNamespace(manifest=[item])
    mlizer.image_hrefs = {"images/cover.png": "_0.jpg"}

    monkeypatch.setattr(fb2ml_mod, "_convert_to_jpeg", lambda data, quality=70: b"jpeg-data")

    out = mlizer.fb2mlize_images()
    assert 'content-type="image/jpeg"' in out
    assert base64.b64encode(b"jpeg-data").decode("ascii") in out


def test_fb2_output_convert_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test fb2 output convert smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2 output convert smoke through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.fb2_output as fb2_output_mod

    fake_fb2ml = types.ModuleType("LiuXin_alpha.file_formats.fb2.fb2ml")

    class _FB2MLizer:
        """
        Provide the fb2mlizer contract for validated ebook processing.

        Example:
            Exercise test fb2 output convert smoke. FB2MLizer through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
        """
        def __init__(self, log):
            """
            Initialize and validate the fb2mlizer state.

            Example:
                Exercise test fb2 output convert smoke. FB2MLizer.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param log: Value supplied for log under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.log = log

        def extract_content(self, oeb_book, opts):
            """
            Extract content under the format's safety and compatibility rules.

            Example:
                Exercise test fb2 output convert smoke. FB2MLizer.extract content through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param oeb_book: Value supplied for oeb book under the utility contract.
            :param opts: Value supplied for opts under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "<FictionBook><body><section><p>Smoke</p></section></body></FictionBook>"

    fake_fb2ml.FB2MLizer = _FB2MLizer

    fake_rasterize = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.rasterize")

    class _Unavailable(Exception):
        """
        Provide the unavailable contract for validated ebook processing.

        Example:
            Exercise test fb2 output convert smoke. Unavailable through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
        """
        pass

    class _Rasterizer:
        """
        Provide the rasterizer contract for validated ebook processing.

        Example:
            Exercise test fb2 output convert smoke. Rasterizer through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
        """
        def __call__(self, oeb_book, opts):
            """
            Perform the call operation under explicit file-format and conversion rules.

            Example:
                Exercise test fb2 output convert smoke. Rasterizer.  call   through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param oeb_book: Value supplied for oeb book under the utility contract.
            :param opts: Value supplied for opts under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    fake_rasterize.SVGRasterizer = _Rasterizer
    fake_rasterize.Unavailable = _Unavailable

    fake_jacket = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.jacket")
    fake_jacket.linearize_jacket = lambda oeb_book: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.fb2.fb2ml", fake_fb2ml)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.rasterize", fake_rasterize)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.jacket", fake_jacket)

    out_file = tmp_path / "out.fb2"
    plugin = fb2_output_mod.FB2Output(None)
    plugin.convert(types.SimpleNamespace(), str(out_file), None, types.SimpleNamespace(), _Log())

    assert out_file.exists()
    assert b"<FictionBook>" in out_file.read_bytes()


def test_fb2_input_convert_smoke_with_metadata_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test fb2 input convert smoke with metadata fallback operation under explicit file-format and conversion rules.

    Example:
        Exercise test fb2 input convert smoke with metadata fallback through a consuming regression::

            python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.fb2_input as fb2_input_mod

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.get_file_type_metadata = lambda stream, file_ext, calibre=True: types.SimpleNamespace(
        title="FB2 Smoke",
        authors=["Smoke Author"],
        cover_data=(None, None),
    )

    fake_opf2 = types.ModuleType("LiuXin_alpha.file_formats.opf.opf2")

    class _Guide:
        """
        Provide the guide contract for validated ebook processing.

        Example:
            Exercise test fb2 input convert smoke with metadata fallback. Guide through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
        """
        def __init__(self) -> None:
            """
            Initialize and validate the guide state.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. Guide.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :return: None; validated state is stored on the receiving object.
            """
            self.cover = None

        def set_cover(self, cpath):
            """
            Replace the container's cover while preserving required package references.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. Guide.set cover through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param cpath: Value supplied for cpath under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.cover = cpath

    class _OPFCreator:
        """
        Provide the opfcreator contract for validated ebook processing.

        Example:
            Exercise test fb2 input convert smoke with metadata fallback. OPFCreator through a consuming regression::

                python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py
        """
        def __init__(self, cwd, mi):
            """
            Initialize and validate the opfcreator state.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. OPFCreator.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param cwd: Value supplied for cwd under the utility contract.
            :param mi: Metadata object exposed to the template function.
            :return: None; validated state is stored on the receiving object.
            """
            self.cwd = cwd
            self.mi = mi
            self.guide = _Guide()

        def create_manifest(self, entries):
            """
            Create the OPF manifest from normalized resource paths.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. OPFCreator.create manifest through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param entries: Value supplied for entries under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.entries = entries

        def create_spine(self, spine):
            """
            Create the OPF spine in the requested reading order.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. OPFCreator.create spine through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param spine: Value supplied for spine under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.spine = spine

        def render(self, fobj):
            """
            Perform the render operation under explicit file-format and conversion rules.

            Example:
                Exercise test fb2 input convert smoke with metadata fallback. OPFCreator.render through a consuming regression::

                    python -m pytest -q tests/file_formats/fb2/test_fb2_modernized.py


            :param fobj: Value supplied for fobj under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            fobj.write(b"<package/>")

    fake_opf2.OPFCreator = _OPFCreator

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.opf.opf2", fake_opf2)
    monkeypatch.chdir(tmp_path)

    plugin = fb2_input_mod.FB2Input(None)
    opts = types.SimpleNamespace(no_inline_fb2_toc=False)
    out = plugin.convert(io.BytesIO(_minimal_fb2()), opts, "fb2", _Log(), {})

    out_path = Path(out)
    assert out_path.exists()
    assert out_path.name == "metadata.opf"
    assert (tmp_path / "index.xhtml").exists()
    assert (tmp_path / "inline-styles.css").exists()
