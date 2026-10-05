"""
Provide test lit modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test lit modernized through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
"""
from __future__ import annotations

import importlib
import io
import sys
import types

from tests.support.file_format_lit import LitLog, lit_options


def test_lit_modules_import_smoke() -> None:
    """
    Perform the test lit modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test mssha1 incremental and one shot match operation under explicit file-format and conversion rules.

    Example:
        Exercise test mssha1 incremental and one shot match through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test mssha1 copy is independent operation under explicit file-format and conversion rules.

    Example:
        Exercise test mssha1 copy is independent through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.lit import mssha1

    base = mssha1.new(b"ab")
    left = base.copy()
    right = base.copy()
    left.update(b"c")
    right.update(b"d")

    assert left.hexdigest() != right.hexdigest()


def test_reader_helpers_parse_utf8_and_varints() -> None:
    """
    Perform the test reader helpers parse utf8 and varints operation under explicit file-format and conversion rules.

    Example:
        Exercise test reader helpers parse utf8 and varints through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test lit input convert glue smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit input convert glue smoke through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.lit_input as lit_input_mod

    sentinel_oeb = object()

    fake_reader_mod = types.ModuleType("LiuXin_alpha.file_formats.lit.reader")

    class _LitReader:
        """
        Parse litreader data into normalized ebook structures.

        Example:
            Exercise test lit input convert glue smoke. LitReader through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
        """
        pass

    fake_reader_mod.LitReader = _LitReader

    def _create_oebbook(log, stream, options, reader):
        """
        Create oebbook under the format's safety and compatibility rules.

        Example:
            Exercise test lit input convert glue smoke. create oebbook through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param log: Value supplied for log under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param reader: Value supplied for reader under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert reader is _LitReader
        return sentinel_oeb

    fake_plumber = types.ModuleType("LiuXin_alpha.file_formats.conversion.plumber")
    fake_plumber.create_oebbook = _create_oebbook

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.lit.reader", fake_reader_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.conversion.plumber", fake_plumber)

    plugin = lit_input_mod.LITInput(None)
    out = plugin.convert(io.BytesIO(b"lit"), lit_options(), "lit", LitLog(), {})

    assert out is sentinel_oeb


def test_lit_input_postprocess_book_pre_to_div() -> None:
    """
    Perform the test lit input postprocess book pre to div operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit input postprocess book pre to div through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.lit_input as lit_input_mod
    from LiuXin_alpha.file_formats.oeb.base import XHTML
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    root = etree.fromstring(
        b"<html xmlns='http://www.w3.org/1999/xhtml'><body><pre>Line one\nLine two</pre></body></html>"
    )
    oeb = types.SimpleNamespace(spine=[types.SimpleNamespace(data=root)])

    plugin = lit_input_mod.LITInput(None)
    plugin.postprocess_book(oeb, lit_options(), LitLog())

    body = root.find(XHTML("body"))
    assert body is not None
    assert len(body) == 1
    assert body[0].tag == XHTML("div")


def test_lit_output_convert_glue_smoke(monkeypatch) -> None:
    """
    Perform the test lit output convert glue smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit output convert glue smoke through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.lit_output as lit_output_mod

    calls: list[str] = []

    fake_writer_mod = types.ModuleType("LiuXin_alpha.file_formats.lit.writer")

    class _Writer:
        """
        Provide the writer contract for validated ebook processing.

        Example:
            Exercise test lit output convert glue smoke. Writer through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
        """
        def __init__(self, opts):
            """
            Initialize and validate the writer state.

            Example:
                Exercise test lit output convert glue smoke. Writer.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


            :param opts: Value supplied for opts under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.opts = opts

        def __call__(self, oeb_book, output_path):
            """
            Perform the call operation under explicit file-format and conversion rules.

            Example:
                Exercise test lit output convert glue smoke. Writer.  call   through a consuming regression::

                    python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


            :param oeb_book: Value supplied for oeb book under the utility contract.
            :param output_path: Value supplied for output path under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            calls.append(f"write:{output_path}")

    fake_writer_mod.LitWriter = _Writer

    def _mk_transform(name: str):
        """
        Perform the mk transform operation under explicit file-format and conversion rules.

        Example:
            Exercise test lit output convert glue smoke. mk transform through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        class _T:
            """
            Provide the t contract for validated ebook processing.

            Example:
                Exercise test lit output convert glue smoke. mk transform. T through a consuming regression::

                    python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
            """
            def __call__(self, oeb, opts):
                """
                Perform the call operation under explicit file-format and conversion rules.

                Example:
                    Exercise test lit output convert glue smoke. mk transform. T.  call   through a consuming regression::

                        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


                :param oeb: Value supplied for oeb under the utility contract.
                :param opts: Value supplied for opts under the utility contract.
                :return: None; the operation mutates state, writes output or performs cleanup in
                    place.
                """
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
        """
        Provide the split contract for validated ebook processing.

        Example:
            Exercise test lit output convert glue smoke. Split through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
        """
        def __init__(self, **kwargs):
            """
            Initialize and validate the split state.

            Example:
                Exercise test lit output convert glue smoke. Split.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: None; validated state is stored on the receiving object.
            """
            self.kwargs = kwargs

        def __call__(self, oeb, opts):
            """
            Perform the call operation under explicit file-format and conversion rules.

            Example:
                Exercise test lit output convert glue smoke. Split.  call   through a consuming regression::

                    python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


            :param oeb: Value supplied for oeb under the utility contract.
            :param opts: Value supplied for opts under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            calls.append("split")

    fake_split.Split = _Split

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.lit.writer", fake_writer_mod)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.htmltoc", fake_htmltoc)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.manglecase", fake_case)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.rasterize", fake_raster)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.split", fake_split)

    plugin = lit_output_mod.LITOutput(None)
    plugin.convert(types.SimpleNamespace(), "out.lit", None, lit_options(), LitLog())

    assert calls == ["split", "htmltoc", "case", "raster", "write:out.lit"]
