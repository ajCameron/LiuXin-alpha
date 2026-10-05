"""
Provide test textile modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test textile modernized through a consuming regression::

        python -m pytest -q tests/file_formats/textile/test_textile_modernized.py
"""
from __future__ import annotations

import importlib
from types import SimpleNamespace


def test_textile_modules_import_smoke() -> None:
    """
    Perform the test textile modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    modules = (
        "LiuXin_alpha.utils.libraries.smartypants",
        "LiuXin_alpha.file_formats.textile",
        "LiuXin_alpha.file_formats.textile.functions",
        "LiuXin_alpha.file_formats.textile.unsmarten",
        "LiuXin_alpha.file_formats.txt.textileml",
        "LiuXin_alpha.file_formats.txt.processor",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_textile_basic_markup_conversion() -> None:
    """
    Perform the test textile basic markup conversion operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile basic markup conversion through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    text = 'h1. Title\n\nA *bold* _italic_ "site":https://example.com and !img.png!'
    out = mod.textile(text)
    assert "<h1>Title</h1>" in out
    assert "<strong>bold</strong>" in out
    assert "<em>italic</em>" in out
    assert '<a href="https://example.com">site</a>' in out
    assert '<img src="img.png"' in out


def test_textile_restricted_escapes_html_and_disables_images() -> None:
    """
    Perform the test textile restricted escapes html and disables images operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile restricted escapes html and disables images through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    out = mod.textile_restricted('<b>raw</b> "x":https://example.com !img.png!')
    assert "&#60;b&#62;raw&#60;/b&#62;" in out
    assert 'rel="nofollow"' in out
    assert "!img.png!" in out


def test_textile_unicode_torture_is_stable() -> None:
    """
    Perform the test textile unicode torture is stable operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile unicode torture is stable through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.textile.functions")
    payload = (
        "h2. Unicode Ω 世界\n\n"
        "Arabic: مرحبا بالعالم\n\n"
        "Hebrew: שלום עולם\n\n"
        "Hindi: नमस्ते दुनिया\n\n"
        'Emoji 👩🏽‍💻 and link "参照":https://example.com\n\n'
        "Combining: cafe\u0301 co\u0308operate A\u030A\n"
    )
    out_a = mod.textile(payload)
    out_b = mod.textile(payload)
    assert out_a == out_b
    assert "Unicode Ω 世界" in out_a
    assert "مرحبا" in out_a
    assert "שלום" in out_a
    assert "नमस्ते" in out_a
    assert "👩🏽‍💻" in out_a
    assert '<a href="https://example.com">参照</a>' in out_a


def test_smartypants_quotes_and_dashes() -> None:
    """
    Perform the test smartypants quotes and dashes operation under explicit file-format and conversion rules.

    Example:
        Exercise test smartypants quotes and dashes through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.utils.libraries.smartypants")
    out = mod.smartyPants('"Hello" -- world')
    assert "&#8220;Hello&#8221;" in out
    assert "&#8212;" in out


def test_convert_textile_processor_wraps_html() -> None:
    """
    Perform the test convert textile processor wraps html operation under explicit file-format and conversion rules.

    Example:
        Exercise test convert textile processor wraps html through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    out = mod.convert_textile("h1. Ω 世界\n\n\"site\":https://example.com", title="T")
    assert out.startswith("<html>")
    assert "<h1>Ω 世界</h1>" in out
    assert '<a href="https://example.com">site</a>' in out


def test_textileml_check_id_tag_and_link_helpers() -> None:
    """
    Perform the test textileml check id tag and link helpers operation under explicit file-format and conversion rules.

    Example:
        Exercise test textileml check id tag and link helpers through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.textileml")
    ml = mod.TextileMLizer(log=SimpleNamespace(info=lambda *a, **k: None, debug=lambda *a, **k: None))
    ml.our_ids = []
    ml.id_no_text = ""
    assert ml.check_id_tag({"id": "chap1"}) == "(#chap1)"
    assert ml.our_ids == ["#chap1"]
    assert ml.id_no_text == "\xa0"
    assert ml.check_id_tag({}) == ""
