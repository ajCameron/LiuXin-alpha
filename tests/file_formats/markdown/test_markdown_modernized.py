"""
Provide test markdown modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test markdown modernized through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import annotations

import importlib
import io
import pkgutil

import pytest


def test_markdown_modules_import_smoke() -> None:
    """
    Perform the test markdown modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pkg = importlib.import_module("LiuXin_alpha.file_formats.markdown")
    importlib.import_module("LiuXin_alpha.file_formats.markdown.__main__")

    failed: list[tuple[str, Exception]] = []
    for mod in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        try:
            importlib.import_module(mod.name)
        except Exception as exc:  # pragma: no cover - only hit on failures
            failed.append((mod.name, exc))

    assert not failed, "Markdown import smoke failures: " + ", ".join(
        f"{name}: {exc!r}" for name, exc in failed
    )


def test_markdown_convert_none_is_empty() -> None:
    """
    Perform the test markdown convert none is empty operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown convert none is empty through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.markdown import Markdown

    assert Markdown().convert(None) == ""


def test_markdown_unicode_torture_with_extensions() -> None:
    """
    Perform the test markdown unicode torture with extensions operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown unicode torture with extensions through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    source = (
        "# T\u00eftle \U0001f9ea\n\n"
        "Body: caf\u00e9 | \u0395\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac | \u0939\u093f\u0928\u094d\u0926\u0940 | \u65e5\u672c\u8a9e | \u0639\u0631\u0628\u064a | e\u0301 [^n]\n\n"
        "| k | v |\n"
        "| - | - |\n"
        "| \U0001f642 | \u6f22\u5b57 |\n\n"
        "[^n]: Footnote \u03a9\n"
    )

    html = markdown.markdown(source, extensions=["tables", "footnotes", "toc", "headerid"])

    assert '<h1 id="title">T\u00eftle \U0001f9ea</h1>' in html
    assert "<table>" in html
    assert "\u6f22\u5b57" in html
    assert "footnote" in html.lower()


def test_markdown_from_file_replaces_invalid_utf8_sequences() -> None:
    """
    Perform the test markdown from file replaces invalid utf8 sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown from file replaces invalid utf8 sequences through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.markdown import markdownFromFile

    bad_utf8 = b"# Titl\xffe\n\nBody: caf\xc3\xa9 and bad \xe2(\xa1"
    output = io.BytesIO()

    markdownFromFile(input=io.BytesIO(bad_utf8), output=output, encoding="utf-8")

    rendered = output.getvalue().decode("utf-8")
    assert "<h1>Titl\ufffde</h1>" in rendered
    assert "caf\u00e9" in rendered
    assert "\ufffd" in rendered


def test_markdown_cli_run_dispatches_to_markdown_from_file(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test markdown cli run dispatches to markdown from file operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown cli run dispatches to markdown from file through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    main_mod = importlib.import_module("LiuXin_alpha.file_formats.markdown.__main__")

    options = {
        "input": "in.md",
        "output": "out.html",
        "safe_mode": False,
        "extensions": ["tables"],
        "encoding": "utf-8",
        "output_format": "html5",
        "lazy_ol": True,
    }
    called: dict[str, object] = {}

    monkeypatch.setattr(main_mod, "parse_options", lambda: (options, 20))
    monkeypatch.setattr(main_mod, "markdownFromFile", lambda **kwargs: called.update(kwargs))

    main_mod.run()

    assert called == options
