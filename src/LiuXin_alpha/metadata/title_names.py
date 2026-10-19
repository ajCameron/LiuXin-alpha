"""Derive bibliographic sort values from titles without database access.

The module owns leading-article, opening-quote and language-selection policy.
Database drivers may register :func:`title_to_title_sort` as a SQL callback, but
do not define the bibliographic transformation.
"""

from __future__ import annotations

import re

from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.localization import canonicalize_lang, get_lang

_BUILTIN_ENGLISH_ARTICLES = frozenset((r"A\s+", r"The\s+", r"An\s+"))
_IGNORED_OPENING_CHARACTERS = "'\"" + "".join(
    chr(codepoint) for codepoint in (*range(0x2018, 0x201E), 0x2032, 0x2033)
)
_title_sort_patterns: dict[tuple[str, ...], re.Pattern[str]] = {}


def get_title_sort_pattern(lang: str | None = None) -> re.Pattern[str]:
    """Return the cached leading-article pattern for a language selection.

    An absent language uses the configured title-sort language and then the
    active locale. Invalid or missing configuration falls back first to the
    configured English articles and finally to ``A``, ``An`` and ``The``.
    Patterns are cached by the resolved article rules, so profile changes and
    live setting updates cannot reuse a different profile's pattern.

    Example:
        >>> get_title_sort_pattern("eng") is get_title_sort_pattern("eng")
        True

    :param lang: Optional language code or name used to choose article rules.
    :return: Case-insensitive pattern matching one leading article.
    """

    selected_language = lang
    if selected_language is None:
        selected_language = preferences.get("default_language_for_title_sort")
        if selected_language is None:
            selected_language = get_lang()
    try:
        selected_language = (
            canonicalize_lang(selected_language) if selected_language else None
        )
    except TypeError:
        selected_language = None

    article_preferences = preferences.get("per_language_title_sort_articles", {})
    try:
        articles = article_preferences.get(selected_language)
    except AttributeError:
        articles = None
    try:
        articles = (
            frozenset(articles) if articles else frozenset(article_preferences["eng"])
        )
    except Exception:
        articles = _BUILTIN_ENGLISH_ARTICLES

    cache_key = tuple(sorted(articles))
    cached = _title_sort_patterns.get(cache_key)
    if cached is not None:
        return cached
    expression = f"^({'|'.join(cache_key)})"
    try:
        pattern = re.compile(expression, re.IGNORECASE)
    except Exception:
        pattern = re.compile(r"^(A|The|An)\s+", re.IGNORECASE)
    _title_sort_patterns[cache_key] = pattern
    return pattern


def title_to_title_sort(
    title: object,
    order: str | None = None,
    lang: str | None = None,
) -> str:
    """Derive a bibliographic sort value from a display title.

    Falsy input produces empty text. Strictly alphabetic order only strips the
    value. Library order removes one opening quote and moves one recognized
    leading article to a comma-separated suffix.

    Example:
        >>> title_to_title_sort("  The Book  ")
        'Book, The'
        >>> title_to_title_sort('"The Book')
        'Book, The'
        >>> title_to_title_sort(None)
        ''

    :param title: Display title; truthy non-string values are converted to text.
    :param order: Explicit sorting mode, or None to read title_series_sorting.
    :param lang: Optional language used to choose leading-article rules.
    :return: Stripped alphabetic or article-moved sort value.
    """

    if not title:
        return ""
    if order is None:
        order = preferences.get("title_series_sorting", "library_order")
    value = str(title).strip()
    if order == "strictly_alphabetic":
        return value
    if value and value[0] in _IGNORED_OPENING_CHARACTERS:
        value = value[1:]
    match = get_title_sort_pattern(lang).search(value)
    if match:
        try:
            article = match.group(1)
        except IndexError:
            pass
        else:
            value = value[len(article) :] + ", " + article
            if value and value[0] in _IGNORED_OPENING_CHARACTERS:
                value = value[1:]
    return value.strip()


__all__ = ["get_title_sort_pattern", "title_to_title_sort"]
