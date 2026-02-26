from __future__ import annotations

from html import escape


def comments_to_html(text: str | None) -> str:
    """
    Convert plain-text comments to a minimal HTML fragment.

    If the input already looks like HTML, return it unchanged.
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
