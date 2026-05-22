from __future__ import annotations

import base64
import importlib
import io
import sys
import types

from pathlib import Path

import pytest


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        self._record(*parts)

    def debug(self, *parts) -> None:
        self._record(*parts)

    def info(self, *parts) -> None:
        self._record(*parts)

    def warn(self, *parts) -> None:
        self._record(*parts)

    def warning(self, *parts) -> None:
        self._record(*parts)

    def error(self, *parts) -> None:
        self._record(*parts)

    def exception(self, *parts) -> None:
        self._record(*parts)


def _minimal_fb2() -> bytes:
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
    importlib.import_module("LiuXin_alpha.file_formats.fb2")
    importlib.import_module("LiuXin_alpha.file_formats.fb2.archive")
    importlib.import_module("LiuXin_alpha.file_formats.fb2.fb2ml")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.fb2_output")


def test_fb2_base64_decode_roundtrip() -> None:
    from LiuXin_alpha.file_formats.fb2 import base64_decode

    raw = b"Smoke decode payload"
    encoded = base64.b64encode(raw)

    assert base64_decode(encoded) == raw


def test_fb2_input_extract_embedded_content_writes_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    import LiuXin_alpha.file_formats.conversion.plugins.fb2_output as fb2_output_mod

    fake_fb2ml = types.ModuleType("LiuXin_alpha.file_formats.fb2.fb2ml")

    class _FB2MLizer:
        def __init__(self, log):
            self.log = log

        def extract_content(self, oeb_book, opts):
            return "<FictionBook><body><section><p>Smoke</p></section></body></FictionBook>"

    fake_fb2ml.FB2MLizer = _FB2MLizer

    fake_rasterize = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.rasterize")

    class _Unavailable(Exception):
        pass

    class _Rasterizer:
        def __call__(self, oeb_book, opts):
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
    import LiuXin_alpha.file_formats.conversion.plugins.fb2_input as fb2_input_mod

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.get_file_type_metadata = lambda stream, file_ext, calibre=True: types.SimpleNamespace(
        title="FB2 Smoke",
        authors=["Smoke Author"],
        cover_data=(None, None),
    )

    fake_opf2 = types.ModuleType("LiuXin_alpha.file_formats.opf.opf2")

    class _Guide:
        def __init__(self) -> None:
            self.cover = None

        def set_cover(self, cpath):
            self.cover = cpath

    class _OPFCreator:
        def __init__(self, cwd, mi):
            self.cwd = cwd
            self.mi = mi
            self.guide = _Guide()

        def create_manifest(self, entries):
            self.entries = entries

        def create_spine(self, spine):
            self.spine = spine

        def render(self, fobj):
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
