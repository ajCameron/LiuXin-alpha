"""
Provide test textile unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test textile unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py
"""
from __future__ import annotations

import importlib
import random


def _textile(text: str) -> str:
    """
    Perform the textile operation under explicit file-format and conversion rules.

    Example:
        Exercise  textile through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    return mod.textile(text)


def _textile_restricted(text: str) -> str:
    """
    Perform the textile restricted operation under explicit file-format and conversion rules.

    Example:
        Exercise  textile restricted through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    return mod.textile_restricted(text)


def test_textile_unicode_torture_multiscript_blocks_and_links() -> None:
    """
    Perform the test textile unicode torture multiscript blocks and links operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile unicode torture multiscript blocks and links through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    src = (
        "h1. Ωμέγα 世界\n\n"
        "p. مرحبا שלום नमस्ते दुनिया\n\n"
        '"参照":https://example.com/路径?x=✓\n\n'
        "!images/图像.png(封面)!"
    )
    out = _textile(src)
    assert "<h1>Ωμέγα 世界</h1>" in out
    assert "مرحبا שלום नमस्ते दुनिया" in out
    assert '<a href="https://example.com/路径?x=✓">参照</a>' in out
    assert '<img src="images/图像.png" title="封面" alt="封面"' in out


def test_textile_preserves_combining_marks_without_normalizing() -> None:
    """
    Perform the test textile preserves combining marks without normalizing operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile preserves combining marks without normalizing through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    nfd = "Cafe\u0301 co\u0308operate A\u030A"
    out = _textile(f"h2. {nfd}")
    assert "<h2>" in out and "</h2>" in out
    assert nfd in out
    assert "\u0301" in out and "\u0308" in out and "\u030A" in out


def test_textile_handles_bidi_zwj_and_emoji_sequences() -> None:
    """
    Perform the test textile handles bidi zwj and emoji sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile handles bidi zwj and emoji sequences through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = "h3. RTL \u200fمرحبا\u200f / ZWJ A\u200dB / Emoji 👩🏽‍💻🧪📚"
    out = _textile(payload)
    assert "\u200fمرحبا\u200f" in out
    assert "A\u200dB" in out
    assert "👩🏽‍💻🧪📚" in out


def test_textile_notextile_block_preserves_unicode_markup_literals() -> None:
    """
    Perform the test textile notextile block preserves unicode markup literals operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile notextile block preserves unicode markup literals through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    src = "<notextile>*粗體* _курсив_ 👩🏽‍💻</notextile>"
    out = _textile(src)
    assert "*粗體* _курсив_ 👩🏽‍💻" in out
    assert "<strong>粗體</strong>" not in out
    assert "<em>курсив</em>" not in out


def test_textile_double_equals_no_textile_region_preserved() -> None:
    """
    Perform the test textile double equals no textile region preserved operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile double equals no textile region preserved through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    out = _textile("==*世界* مرحبا==")
    assert "*世界* مرحبا" in out
    assert "<strong>世界</strong>" not in out


def test_textile_restricted_unicode_torture_escapes_html_and_adds_nofollow() -> None:
    """
    Perform the test textile restricted unicode torture escapes html and adds nofollow operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile restricted unicode torture escapes html and adds nofollow through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    src = 'سلام <b>粗體</b> "资料":https://example.com/路径?鍵=值 !封面.png!'
    out = _textile_restricted(src)
    assert "&#60;b&#62;粗體&#60;/b&#62;" in out
    assert 'rel="nofollow"' in out
    assert '<a href="https://example.com/路径?鍵=值"' in out
    assert "!封面.png!" in out


def test_textile_unicode_reference_links_resolve() -> None:
    """
    Perform the test textile unicode reference links resolve operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile unicode reference links resolve through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    src = "[资料]https://example.com/路径?x=✓\n\n\"点击\":资料"
    out = _textile(src)
    assert '<a href="https://example.com/路径?x=✓">点击</a>' in out


def test_textile_smart_quotes_and_dash_with_unicode_content() -> None:
    """
    Perform the test textile smart quotes and dash with unicode content operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile smart quotes and dash with unicode content through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    out = _textile('p. "Καλημέρα" -- "世界"')
    assert "&#8220;Καλημέρα&#8221;" in out
    assert "&#8220;世界&#8221;" in out
    assert "&#8212;" in out


def test_textile_deterministic_output_under_unicode_fuzz() -> None:
    """
    Perform the test textile deterministic output under unicode fuzz operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile deterministic output under unicode fuzz through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    rng = random.Random(20260303)
    alphabet = (
        "abcXYZ"
        "ΩЖשלוםمرحباनमस्ते世界"
        "👩🏽‍💻🧪📚"
        " _*+-=~^%|!\"':;,.?/[](){}"
        "\u200d\u200f"
        "\u0301\u0308"
    )
    fuzz = "".join(rng.choice(alphabet) for _ in range(400))
    src = f"h4. Fuzz\n\n{fuzz}"
    out_a = _textile(src)
    out_b = _textile(src)
    assert out_a == out_b
    assert len(out_a) > 0


def test_convert_textile_processor_unicode_torture_wrapper() -> None:
    """
    Perform the test convert textile processor unicode torture wrapper operation under explicit file-format and conversion rules.

    Example:
        Exercise test convert textile processor unicode torture wrapper through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    src = (
        "h1. 多言語\n\n"
        "Arabic مرحبا / Hebrew שלום / Hindi नमस्ते / Emoji 👩🏽‍💻\n\n"
        '"参照":https://example.com/路径'
    )
    out = mod.convert_textile(src, title="Тест")
    assert out.startswith("<html>")
    assert "<title>Тест " in out
    assert "<h1>多言語</h1>" in out
    assert "Emoji 👩🏽‍💻" in out
    assert '<a href="https://example.com/路径">参照</a>' in out
