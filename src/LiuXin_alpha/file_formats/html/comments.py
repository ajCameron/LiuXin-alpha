"""
Convert, sanitize and render book comments.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise comments through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from __future__ import annotations

from html import escape


def comments_to_html(text: str | None) -> str:
    """
    Convert plain-text comments to a minimal HTML fragment.

    Example:
        Exercise comments to html through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not text:
        return ""
    raw = str(text).strip()
    if not raw:
        return ""
    if "<" in raw and ">" in raw:
        return raw

    paragraphs = [p.strip() for p in raw.split("\n\n") if p.strip()]
    if not paragraphs:
        return ""
    rendered = []
    for p in paragraphs:
        rendered.append("<p>%s</p>" % escape(p).replace("\n", "<br/>"))
    return "".join(rendered)
