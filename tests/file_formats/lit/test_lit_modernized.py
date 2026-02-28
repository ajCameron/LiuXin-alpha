from __future__ import annotations

import importlib
import io
import sys
import types


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


def test_lit_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.lit")
    importlib.import_module("LiuXin_alpha.file_formats.lit.maps")
    importlib.import_module("LiuXin_alpha.file_formats.lit.maps.opf")
    importlib.import_module("LiuXin_alpha.file_formats.lit.maps.html")
    importlib.import_module("LiuXin_alpha.file_formats.lit.mssha1")
    importlib.import_module("LiuXin_alpha.file_formats.lit.reader")
    importlib.import_module("LiuXin_alpha.file_formats.lit.writer")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.lit_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.lit_output")


def test_mssha1_incremental_and_one_shot_match() -> None:
    from LiuXin_alpha.file_formats.lit import mssha1

    one_shot = mssha1.new(b"abc").hexdigest()

    inc = mssha1.new()
    inc.update(b"a")
    inc.update(b"bc")
    incremental = inc.hexdigest()

    assert one_shot == incremental
    assert isinstance(inc.digest(), bytes)
    assert len(inc.digest()) == 20


def test_mssha1_copy_is_independent() -> None:
    from LiuXin_alpha.file_formats.lit import mssha1

    base = mssha1.new(b"ab")
    left = base.copy()
    right = base.copy()
    left.update(b"c")
    right.update(b"d")

    assert left.hexdigest() != right.hexdigest()


def test_reader_helpers_parse_utf8_and_varints() -> None:
    from LiuXin_alpha.file_formats.lit.reader import encint, read_utf8_char, consume_sized_utf8_string

    ch, pos = read_utf8_char("é".encode("utf-8"), 0)
    assert ch == "é"
    assert pos == 2

    # Length-prefixed UTF-8 string with trailing NUL padding.
    text, rem = consume_sized_utf8_string(bytes([3]) + "abc".encode("utf-8") + b"\x00TAIL", zpad=True)
    assert text == "abc"
    assert rem == b"TAIL"

    val, rem2, left = encint(b"\x81\x01rest", 6)
    assert val == 129
    assert rem2.startswith(b"rest")
    assert left == 4


def test_lit_input_convert_glue_smoke(monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.lit_input as lit_input_mod

    sentinel_oeb = object()

    fake_reader_mod = types.ModuleType("LiuXin_alpha.file_formats.lit.reader")

    class _LitReader:
        pass

    fake_reader_mod.LitReader = _LitReader

    def _create_oebbook(log, stream, options, reader):
        assert reader is _LitReader
        return sentinel_oeb

    fake_plumber = types.ModuleType("LiuXin_alpha.file_formats.conversion.plumber")
    fake_plumber.create_oebbook = _create_oebbook

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.lit.reader", fake_reader_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.conversion.plumber", fake_plumber)

    plugin = lit_input_mod.LITInput(None)
    out = plugin.convert(io.BytesIO(b"lit"), types.SimpleNamespace(), "lit", _Log(), {})

    assert out is sentinel_oeb


def test_lit_input_postprocess_book_pre_to_div() -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.lit_input as lit_input_mod
    from LiuXin_alpha.file_formats.oeb.base import XHTML
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    root = etree.fromstring(
        b"<html xmlns='http://www.w3.org/1999/xhtml'><body><pre>Line one\nLine two</pre></body></html>"
    )
    oeb = types.SimpleNamespace(spine=[types.SimpleNamespace(data=root)])

    plugin = lit_input_mod.LITInput(None)
    plugin.postprocess_book(oeb, types.SimpleNamespace(), _Log())

    body = root.find(XHTML("body"))
    assert body is not None
    assert len(body) == 1
    assert body[0].tag == XHTML("div")


def test_lit_output_convert_glue_smoke(monkeypatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.lit_output as lit_output_mod

    calls: list[str] = []

    fake_writer_mod = types.ModuleType("LiuXin_alpha.file_formats.lit.writer")

    class _Writer:
        def __init__(self, opts):
            self.opts = opts

        def __call__(self, oeb_book, output_path):
            calls.append(f"write:{output_path}")

    fake_writer_mod.LitWriter = _Writer

    def _mk_transform(name: str):
        class _T:
            def __call__(self, oeb, opts):
                calls.append(name)

        return _T

    fake_htmltoc = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.htmltoc")
    fake_htmltoc.HTMLTOCAdder = _mk_transform("htmltoc")

    fake_case = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.manglecase")
    fake_case.CaseMangler = _mk_transform("case")

    fake_raster = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.rasterize")
    fake_raster.SVGRasterizer = _mk_transform("raster")

    fake_split = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.split")

    class _Split:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __call__(self, oeb, opts):
            calls.append("split")

    fake_split.Split = _Split

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.lit.writer", fake_writer_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.htmltoc", fake_htmltoc)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.manglecase", fake_case)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.rasterize", fake_raster)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.split", fake_split)

    plugin = lit_output_mod.LITOutput(None)
    plugin.convert(types.SimpleNamespace(), "out.lit", None, types.SimpleNamespace(), _Log())

    assert calls == ["split", "htmltoc", "case", "raster", "write:out.lit"]
