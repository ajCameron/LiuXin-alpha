"""
Provide test pmlconverter unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pmlconverter unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.pml.pmlconverter import (
    PML_HTMLizer,
    footnote_to_html,
    pml_to_html,
    sidebar_to_html,
)


def _parse(pml: str, name: str = "unicode_torture.pml"):
    """
    Perform the parse operation under explicit file-format and conversion rules.

    Example:
        Exercise  parse through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :param pml: Value supplied for pml under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    hizer = PML_HTMLizer()
    html = hizer.parse_pml(pml, name)
    toc = hizer.get_toc()
    return html, toc


UNICODE_TORTURE = (
    "Latin café naïve coöperate — Greek Ω≈ç√∫˜µ≤≥÷ — Cyrillic До свидания — "
    "Arabic السَّلَامُ عَلَيْكُمْ — Hebrew שָׁלוֹם — Hindi नमस्ते — Thai สวัสดี — "
    "CJK 中文測試 日本語テスト 한글테스트 — Emoji 👩🏽‍💻🧪📚 — Combining e\u0301 o\u0308 n\u0303 — "
    "ZWJ A\u200dB — VS ❤\ufe0f — Math ∀∑∏∫∞ — Music 𝄞"
)


@pytest.mark.parametrize(
    ("heading", "html_fragment", "toc_fragment"),
    [
        ("Привет мир", "Привет мир", "Привет мир"),
        ("नमस्ते दुनिया", "नमस्ते दुनिया", "नमस्ते दुनिया"),
        ("こんにちは世界", "こんにちは世界", "こんにちは世界"),
        ("مرحبا بالعالم", "مرحبا بالعالم", "مرحبا بالعالم"),
        ("שָׁלוֹם עוֹלָם", "שָׁלוֹם עוֹלָם", "שָׁלוֹם עוֹלָם"),
        ("中文測試", "中文測試", "中文測試"),
        ("👩🏽‍💻 unicode", "👩🏽‍💻 unicode", "👩🏽\u200d💻 unicode"),
        ("Cafe\u0301", "Cafe\u0301", "Cafe\u0301"),
        ("A & B < C > D", "A &amp; B &lt; C &gt; D", "A &amp; B &lt; C &gt; D"),
        (UNICODE_TORTURE, "👩🏽‍💻🧪📚", "👩🏽\u200d💻🧪📚"),
    ],
)
def test_unicode_headings_survive_parse_and_toc(heading: str, html_fragment: str, toc_fragment: str) -> None:
    """
    Perform the test unicode headings survive parse and toc operation under explicit file-format and conversion rules.

    Example:
        Exercise test unicode headings survive parse and toc through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :param heading: Value supplied for heading under the utility contract.
    :param html_fragment: Value supplied for html fragment under the utility contract.
    :param toc_fragment: Value supplied for toc fragment under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    html, toc = _parse(f"\\x{heading}\\x")
    assert html_fragment in html
    assert len(toc) >= 1
    assert toc_fragment in toc[0].text


@pytest.mark.parametrize(
    ("pml", "expected"),
    [
        ("\\x\\U03a9\\x", "Ω"),
        ("\\x\\U03A9\\x", "Ω"),
        ("\\x\\a169\\x", "©"),
        ("\\xMix \\U03A9 and \\a169\\x", "Mix Ω and ©"),
    ],
)
def test_unicode_escape_sequences_decode_in_output(pml: str, expected: str) -> None:
    """
    Perform the test unicode escape sequences decode in output operation under explicit file-format and conversion rules.

    Example:
        Exercise test unicode escape sequences decode in output through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :param pml: Value supplied for pml under the utility contract.
    :param expected: Value supplied for expected under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    html = pml_to_html(pml)
    assert expected in html


def test_unicode_escape_codes_do_not_leak_as_hex_into_toc() -> None:
    """
    Perform the test unicode escape codes do not leak as hex into toc operation under explicit file-format and conversion rules.

    Example:
        Exercise test unicode escape codes do not leak as hex into toc through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    html, toc = _parse("\\xTitle \\U03A9 \\U03a9 \\a169\\x")
    assert "Ω" in html
    assert "03A9" not in toc[0].text
    assert "03a9" not in toc[0].text


def test_unicode_toc_hierarchy_across_all_levels() -> None:
    """
    Perform the test unicode toc hierarchy across all levels operation under explicit file-format and conversion rules.

    Example:
        Exercise test unicode toc hierarchy across all levels through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pml = "\n".join(
        (
            "\\X0α-root\\X0",
            "\\X1β-child\\X1",
            "\\X2γ-grandchild\\X2",
            "\\X3δ-deep\\X3",
            "\\X4ε-leaf\\X4",
        )
    )
    _, toc = _parse(pml, "levels.pml")
    assert toc[0].text == "α-root"
    assert toc[0][0].text == "β-child"
    assert toc[0][0][0].text == "γ-grandchild"
    assert toc[0][0][0][0].text == "δ-deep"
    assert toc[0][0][0][0][0].text == "ε-leaf"


@pytest.mark.parametrize(
    "bad_pml",
    [
        '\\x="unterminated heading',
        '\\FN="note1"Broken footnote open without closer',
        '\\Sd="id only no end',
        '\\q="#anchor missing quote',
        "\\X9bad level\\X9",
        "\\x👩🏽‍💻 text without close",
        "\\xnested \\x inner\\x",
    ],
)
def test_malformed_unicode_markup_does_not_crash(bad_pml: str) -> None:
    """
    Perform the test malformed unicode markup does not crash operation under explicit file-format and conversion rules.

    Example:
        Exercise test malformed unicode markup does not crash through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :param bad_pml: Value supplied for bad pml under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    html, _toc = _parse(bad_pml, "malformed_unicode.pml")
    assert isinstance(html, str)
    assert len(html) > 0


def test_footnote_and_sidebar_unicode_torture() -> None:
    """
    Perform the test footnote and sidebar unicode torture operation under explicit file-format and conversion rules.

    Example:
        Exercise test footnote and sidebar unicode torture through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    foot = footnote_to_html("αβ", "\\xПримечание\\x and \\U03A9")
    side = sidebar_to_html("測試", "\\x注釈\\x and \\a169")
    assert 'id="fn-αβ"' in foot
    assert 'id="sb-測試"' in side
    assert "Примечание" in foot
    assert "注釈" in side
    assert "Ω" in foot
    assert "©" in side


def test_wrapper_and_direct_parser_are_equivalent_on_unicode_payload() -> None:
    """
    Perform the test wrapper and direct parser are equivalent on unicode payload operation under explicit file-format and conversion rules.

    Example:
        Exercise test wrapper and direct parser are equivalent on unicode payload through a consuming regression::

            python -m pytest -q tests/file_formats/pml/test_pmlconverter_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pml = "\\x" + UNICODE_TORTURE + "\\x\n\\c\n\\xSecond 👨‍👩‍👧‍👦 block\\x"
    direct_html, _ = _parse(pml, "equiv.pml")
    wrapped_html = pml_to_html(pml)
    assert direct_html == wrapped_html
