"""
Provide test rtf end to end and unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test rtf end to end and unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py
"""
from __future__ import annotations

import os
import re
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log. record through a consuming regression::

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


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

                python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


        :param parts: Value supplied for parts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._record(*parts)


def _opts() -> SimpleNamespace:
    """
    Perform the opts operation under explicit file-format and conversion rules.

    Example:
        Exercise  opts through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return SimpleNamespace(input_encoding=None, debug_pipeline=None, ignore_wmf=True)


UNICODE_TORTURE_STANDARD_CASES = [
    {
        "title": "Unicode Torture Καλημέρα 你好",
        "author": "José Иван",
        "paragraphs": [
            "Latin accents: naïve coöperate façade déjà vu.",
            "Greek: Καλημέρα κόσμε.",
            "Cyrillic: Здравствуйте, мир.",
            "Arabic: مرحبا بالعالم.",
            "Hebrew: שלום עולם.",
            "Devanagari: नमस्ते दुनिया।",
            "CJK: 你好，世界。",
            "Combining marks: cafe\u0301 co\u0308perate A\u030A.",
        ],
        "required_probes": ["naïve", "Καλημέρα", "Здравствуйте", "مرحبا", "שלום", "नमस्ते", "你好"],
    },
    {
        "title": "Emoji + Math",
        "author": "Alice & Bob",
        "paragraphs": [
            "Emoji sequence: 😀 😃 😄",
            "ZWJ family: 👨‍👩‍👧‍👦 and engineer: 👩‍💻",
            "Math and symbols: ∑∫√∞≈≠≤≥ ←→↔↦",
            "Mixed scripts: кириллица عربى हिन्दी 漢字 한글",
        ],
        "required_probes": ["Emoji", "Math and symbols", "Mixed scripts", "한글"],
    },
    {
        "title": "Directionality Stress",
        "author": "RTL/LTR",
        "paragraphs": [
            "English then العربية ثم English again.",
            "Hebrew with punctuation: שלום, עולם!",
            "Thai + Japanese + Chinese: สวัสดี 日本語 中文",
            "Control-like chars escaped safely: braces { } and backslash \\.",
        ],
        "required_probes": ["العربية", "שלום", "日本語", "中文", "backslash"],
    },
]


def _rtf_escape(text: str) -> str:
    """
    Perform the rtf escape operation under explicit file-format and conversion rules.

    Example:
        Exercise  rtf escape through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    out = []
    for ch in text:
        if ch == "\\":
            out.append(r"\\")
        elif ch == "{":
            out.append(r"\{")
        elif ch == "}":
            out.append(r"\}")
        elif ord(ch) > 127:
            out.append(r"\u%d?" % ord(ch))
        else:
            out.append(ch)
    return "".join(out)


def _build_rtf(title: str, author: str, paragraphs: list[str]) -> bytes:
    """
    Perform the build rtf operation under explicit file-format and conversion rules.

    Example:
        Exercise  build rtf through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param title: Value supplied for title under the utility contract.
    :param author: Value supplied for author under the utility contract.
    :param paragraphs: Value supplied for paragraphs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    body = "\\par\n".join(_rtf_escape(p) for p in paragraphs)
    raw = (
        "{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        "{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        "{\\info{\\title %s}{\\author %s}}\n"
        "\\viewkind4\\uc1\\pard\n"
        "%s\\par\n"
        "}\n"
    ) % (_rtf_escape(title), _rtf_escape(author), body)
    return raw.encode("latin-1", "replace")


@contextmanager
def _chdir(path: Path):
    """
    Perform the chdir operation under explicit file-format and conversion rules.

    Example:
        Exercise  chdir through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: An iterator yielding the normalized values described above.
    """
    old = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def _run_rtf_input_convert(input_path: Path, workdir: Path):
    """
    Perform the run rtf input convert operation under explicit file-format and conversion rules.

    Example:
        Exercise  run rtf input convert through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param input_path: Value supplied for input path under the utility contract.
    :param workdir: Value supplied for workdir under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.rtf_input import RTFInput

    workdir.mkdir(parents=True, exist_ok=True)
    log = _Log()
    plugin = RTFInput(None)
    with _chdir(workdir):
        with input_path.open("rb") as stream:
            out = plugin.convert(stream, _opts(), "rtf", log, {})
    opf_path = Path(out)
    return log, opf_path, opf_path.parent / "index.xhtml", opf_path.parent / "styles.css"


def _normalize_opf(opf_text: str) -> str:
    """
    Normalize opf under the format's safety and compatibility rules.

    Example:
        Exercise  normalize opf through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param opf_text: Value supplied for opf text under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    opf_text = re.sub(
        r"<dc:identifier[^>]*>.*?</dc:identifier>",
        "<dc:identifier id='LiuXin_id'>NORMALIZED-ID</dc:identifier>",
        opf_text,
        flags=re.DOTALL,
    )
    opf_text = re.sub(
        r'(<meta name="calibre:timestamp" content=")[^"]+(")',
        r"\1NORMALIZED-TS\2",
        opf_text,
    )
    return opf_text


def _contains_normalized(text: str, probe: str) -> bool:
    """
    Perform the contains normalized operation under explicit file-format and conversion rules.

    Example:
        Exercise  contains normalized through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param text: Text parsed, normalized or rendered.
    :param probe: Value supplied for probe under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return unicodedata.normalize("NFC", probe) in unicodedata.normalize("NFC", text)


def test_rtf_input_end_to_end_unicode_torture(tmp_path: Path) -> None:
    """
    Perform the test rtf input end to end unicode torture operation under explicit file-format and conversion rules.

    Example:
        Exercise test rtf input end to end unicode torture through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    paragraphs = [
        "Latin accents: naïve coöperate façade déjà vu.",
        "Greek: Καλημέρα κόσμε.",
        "Cyrillic: Здравствуйте, мир.",
        "Arabic: مرحبا بالعالم.",
        "Hebrew: שלום עולם.",
        "Devanagari: नमस्ते दुनिया।",
        "CJK: 你好，世界。",
        "Combining marks: cafe\u0301 co\u0308perate A\u030A.",
    ]
    source = tmp_path / "unicode_torture.rtf"
    source.write_bytes(_build_rtf("Unicode Stress", "Alice & Bob", paragraphs))

    _log, opf_path, html_path, css_path = _run_rtf_input_convert(source, tmp_path / "run_unicode")

    assert opf_path.exists()
    assert html_path.exists()
    assert css_path.exists()

    html = html_path.read_text("utf-8", "replace")
    probes = ["naïve", "Καλημέρα", "Здравствуйте", "مرحبا", "שלום", "नमस्ते", "你好"]
    hits = sum(1 for p in probes if p in html)
    assert hits >= 5


@pytest.mark.parametrize("case", UNICODE_TORTURE_STANDARD_CASES)
def test_rtf_input_end_to_end_unicode_torture_standard_matrix(tmp_path: Path, case: dict) -> None:
    """
    Perform the test rtf input end to end unicode torture standard matrix operation under explicit file-format and conversion rules.

    Example:
        Exercise test rtf input end to end unicode torture standard matrix through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param case: Value supplied for case under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = tmp_path / (case["title"].replace(" ", "_") + ".rtf")
    source.write_bytes(_build_rtf(case["title"], case["author"], case["paragraphs"]))

    _log, opf_path, html_path, css_path = _run_rtf_input_convert(source, tmp_path / ("run_" + source.stem))

    assert opf_path.exists()
    assert html_path.exists()
    assert css_path.exists()

    html = html_path.read_text("utf-8", "replace")
    for probe in case["required_probes"]:
        assert _contains_normalized(html, probe)


def test_rtf_input_end_to_end_deterministic_output(tmp_path: Path) -> None:
    """
    Perform the test rtf input end to end deterministic output operation under explicit file-format and conversion rules.

    Example:
        Exercise test rtf input end to end deterministic output through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    paragraphs = [
        "Deterministic run one.",
        "More text, with punctuation, commas, and Ω marker.",
    ]
    source = tmp_path / "deterministic.rtf"
    source.write_bytes(_build_rtf("Deterministic", "Tester", paragraphs))

    _log_a, opf_a, html_a, css_a = _run_rtf_input_convert(source, tmp_path / "run_a")
    _log_b, opf_b, html_b, css_b = _run_rtf_input_convert(source, tmp_path / "run_b")

    assert html_a.read_bytes() == html_b.read_bytes()
    assert css_a.read_bytes() == css_b.read_bytes()
    assert _normalize_opf(opf_a.read_text("utf-8", "replace")) == _normalize_opf(
        opf_b.read_text("utf-8", "replace")
    )


def test_rtf_input_end_to_end_broken_encoding_tolerant(tmp_path: Path) -> None:
    """
    Perform the test rtf input end to end broken encoding tolerant operation under explicit file-format and conversion rules.

    Example:
        Exercise test rtf input end to end broken encoding tolerant through a consuming regression::

            python -m pytest -q tests/file_formats/rtf/test_rtf_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    broken = (
        b"{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        b"{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        b"{\\info{\\title Broken Encoding}{\\author Author}}\n"
        b"\\viewkind4\\uc1\\pard\n"
        + b"Broken bytes: "
        + bytes([0x81, 0x8D, 0x90, 0x9D, 0xFF])
        + b" and tail text.\\par\n"
        + b"}\n"
    )
    source = tmp_path / "broken_encoding.rtf"
    source.write_bytes(broken)

    log, opf_path, html_path, _css_path = _run_rtf_input_convert(source, tmp_path / "run_broken")

    assert opf_path.exists()
    assert html_path.exists()
    html = html_path.read_text("utf-8", "replace")
    assert "Broken bytes" in html
    assert "tail text" in html
    # Accept both behaviors:
    # 1) legacy path warns and falls back
    # 2) hardened parser extracts metadata and does not warn
    warned = any("Failed to read RTF metadata" in msg for msg in log.messages)
    if not warned:
        opf = opf_path.read_text("utf-8", "replace")
        assert "Broken Encoding" in opf
        assert "Author" in opf
