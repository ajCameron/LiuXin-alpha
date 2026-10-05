"""
Provide test txt unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test txt unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py
"""
from __future__ import annotations

import importlib
import io
import random
import sys
import types
from pathlib import Path


UNICODE_TORTURE = (
    "Latin café naïve coöperate façade déjà vu\n"
    "Greek Καλημέρα κόσμε\n"
    "Cyrillic Здравствуйте, мир\n"
    "Arabic مرحبا بالعالم\n"
    "Hebrew שלום עולם\n"
    "Hindi नमस्ते दुनिया\n"
    "Thai สวัสดีโลก\n"
    "CJK 你好，世界 / こんにちは世界 / 안녕하세요 세계\n"
    "Emoji 👩🏽‍💻🧪📚🧬\n"
    "Combining cafe\u0301 co\u0308operate A\u030A\n"
    "Bidi \u200fمرحبا\u200f and ZWJ A\u200dB\n"
)


def test_convert_basic_unicode_torture_deterministic() -> None:
    """
    Perform the test convert basic unicode torture deterministic operation under explicit file-format and conversion rules.

    Example:
        Exercise test convert basic unicode torture deterministic through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    a = mod.convert_basic(UNICODE_TORTURE, title="Ω 世界", epub_split_size_kb=8)
    b = mod.convert_basic(UNICODE_TORTURE, title="Ω 世界", epub_split_size_kb=8)
    assert a == b
    assert "<title>Ω 世界 " in a
    assert "👩🏽‍💻🧪📚🧬" in a
    assert "cafe\u0301" in a


def test_convert_markdown_and_textile_unicode_paths() -> None:
    """
    Perform the test convert markdown and textile unicode paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test convert markdown and textile unicode paths through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    md = mod.convert_markdown("# 多言語\n\n- Ω\n- 世界\n\n`👩🏽‍💻`", title="MD")
    tx = mod.convert_textile("h1. 多言語\n\n\"参照\":https://example.com/路径", title="TX")
    assert "<title>MD " in md and "多言語</h1>" in md
    assert "<title>TX " in tx and '<a href="https://example.com/路径">参照</a>' in tx


def test_txt_input_broken_encoding_falls_back_to_replacement(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test txt input broken encoding falls back to replacement operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt input broken encoding falls back to replacement through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")

    class _Opt:
        """
        Provide the opt contract for validated ebook processing.

        Example:
            Exercise test txt input broken encoding falls back to replacement. Opt through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py
        """
        def __init__(self, name, val):
            """
            Initialize and validate the opt state.

            Example:
                Exercise test txt input broken encoding falls back to replacement. Opt.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param val: Template or metadata value evaluated by the operation.
            :return: None; validated state is stored on the receiving object.
            """
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = val

    class _HTMLInput:
        """
        Convert htmlinput sources into the normalized OEB pipeline model.

        Example:
            Exercise test txt input broken encoding falls back to replacement. HTMLInput through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py
        """
        options = (_Opt("breadth_first", False), _Opt("dont_package", False))

        def __init__(self):
            """
            Initialize and validate the htmlinput state.

            Example:
                Exercise test txt input broken encoding falls back to replacement. HTMLInput.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


            :return: None; validated state is stored on the receiving object.
            """
            self.last_html = b""

        def convert(self, stream, options, file_ext, log, accelerators):
            """
            Convert the supplied source into the stage's normalized output representation.

            Example:
                Exercise test txt input broken encoding falls back to replacement. HTMLInput.convert through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


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

    html_input = _HTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: html_input if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Broken", authors=["A"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)

    payload = UNICODE_TORTURE.encode("utf-8") + bytes([0x81, 0x8D, 0x90, 0xFF])
    source = tmp_path / "broken.txt"
    source.write_bytes(payload)

    plugin = txt_input_mod.TXTInput(None)
    opts = types.SimpleNamespace(
        input_encoding="utf-8",
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="footnotes, tables, toc",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
        flow_size=0,
    )

    with source.open("rb") as stream:
        plugin.convert(stream, opts, "txt", types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None), {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert "Latin café" in decoded_html
    assert "\ufffd" in decoded_html

    report = opts.conversion_report
    events = [event for event in report.loss_events if event.code == "input-decoding-byte-replacement"]
    assert len(events) == 1
    event = events[0]
    assert event.phase == "txt-input"
    assert event.source_format == "txt"
    assert event.target_format == "oeb"
    assert event.edge_name == "txt-to-oeb"
    assert event.recoverable is True
    assert event.count >= 1
    assert event.details["encoding"] == "utf-8"
    assert event.details["replacement"] == "\ufffd"
    assert event.samples[0].codepoints == ("U+FFFD",)


def test_detect_formatting_type_unicode_fuzz_stable() -> None:
    """
    Perform the test detect formatting type unicode fuzz stable operation under explicit file-format and conversion rules.

    Example:
        Exercise test detect formatting type unicode fuzz stable through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    rng = random.Random(20260303)
    alphabet = "abcXYZΩЖשלוםمرحباनमस्ते世界👩🏽‍💻_*#[]()!\":./-\n "
    fuzz = "".join(rng.choice(alphabet) for _ in range(3000))
    a = mod.detect_formatting_type(fuzz)
    b = mod.detect_formatting_type(fuzz)
    assert a == b
    assert a in {"markdown", "textile", "heuristic"}


def test_txt_output_roundtrip_bytes_with_unicode_writer(monkeypatch) -> None:
    """
    Perform the test txt output roundtrip bytes with unicode writer operation under explicit file-format and conversion rules.

    Example:
        Exercise test txt output roundtrip bytes with unicode writer through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    txt_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        """
        Provide the txtmlizer contract for validated ebook processing.

        Example:
            Exercise test txt output roundtrip bytes with unicode writer. TXTMLizer through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py
        """
        def __init__(self, _log):
            """
            Initialize and validate the txtmlizer state.

            Example:
                Exercise test txt output roundtrip bytes with unicode writer. TXTMLizer.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


            :param _log: Value supplied for log under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            pass

        def extract_content(self, _oeb, _opts):
            """
            Extract content under the format's safety and compatibility rules.

            Example:
                Exercise test txt output roundtrip bytes with unicode writer. TXTMLizer.extract content through a consuming regression::

                    python -m pytest -q tests/file_formats/txt/test_txt_unicode_torture.py


            :param _oeb: Value supplied for oeb under the utility contract.
            :param _opts: Value supplied for opts under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return UNICODE_TORTURE

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    out = io.BytesIO()
    opts = types.SimpleNamespace(
        txt_output_formatting="plain",
        newline="unix",
        txt_output_encoding="utf-8",
        remove_paragraph_spacing=False,
        max_line_length=0,
        force_max_line_length=False,
        inline_toc=False,
    )
    log = types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None)
    txt_output_mod.TXTOutput(None).convert(object(), out, None, opts, log)
    out.seek(0)
    data = out.read().decode("utf-8", "replace")
    assert "Greek Καλημέρα κόσμε" in data
    assert "Emoji 👩🏽‍💻🧪📚🧬" in data
