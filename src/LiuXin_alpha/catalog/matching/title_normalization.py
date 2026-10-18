"""
Normalize titles for Catalog's legacy fuzzy-comparison policy.

This module owns the historical article, punctuation, separator and whitespace
rules used to decide whether two book titles are approximately equal.  The
rules are bibliographic matching policy: they neither query a database nor
prepare values for a database driver.
"""

from __future__ import annotations

import re

from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower

_fuzzy_title_patterns: tuple[tuple[re.Pattern[str], str], ...] | None = None


def _safe_title_sort_pattern() -> re.Pattern[str] | str:
    """
    Return the configured leading-article pattern or an English fallback.

    Example:
        >>> bool(_safe_title_sort_pattern())
        True


    :return: Configured compiled pattern, or a fallback pattern string when the
        metadata preference helper fails.
    """

    try:
        from LiuXin_alpha.metadata.title_names import get_title_sort_pattern

        return get_title_sort_pattern()
    except Exception:
        return r"^(a|an|the)\s+"


def fuzzy_title_patterns() -> tuple[tuple[re.Pattern[str], str], ...]:
    """
    Build and cache the ordered substitutions for fuzzy title matching.

    The retained behavior removes selected punctuation and one configured
    leading article, changes hyphens/dots/underscores to spaces, and collapses
    whitespace.  Preference changes do not invalidate the process cache.

    Example:
        >>> first = fuzzy_title_patterns()
        >>> first is fuzzy_title_patterns()
        True


    :return: Shared ordered tuple of compiled patterns and replacements.
    """

    global _fuzzy_title_patterns
    if _fuzzy_title_patterns is None:
        _fuzzy_title_patterns = tuple(
            (
                re.compile(pattern, re.IGNORECASE)
                if isinstance(pattern, str)
                else pattern,
                replacement,
            )
            for pattern, replacement in (
                (r'[\[\](){}<>\'";,:#]', ""),
                (_safe_title_sort_pattern(), ""),
                (r"[-._]", " "),
                (r"\s+", " "),
            )
        )
    return _fuzzy_title_patterns


def normalise_fuzzy_title(title: str) -> str:
    """
    Return the historical fuzzy-comparison key for a title.

    This is deliberately not a canonical title or persisted identity.  No
    final strip follows the substitutions, preserving the existing edge-space
    behavior.

    Example:
        >>> normalise_fuzzy_title("The Left-Hand of Darkness")
        'left hand of darkness'

    :param title: Title stripped and ICU-lowercased before substitutions.
    :return: Comparison key using the retained legacy rules.
    """

    normalized = icu_lower(title.strip())
    for pattern, replacement in fuzzy_title_patterns():
        normalized = pattern.sub(replacement, normalized)
    return normalized


def fuzzy_title(title: str) -> str:
    """
    Return the fuzzy title key under the retained legacy name.

    Example:
        >>> fuzzy_title("The Hobbit")
        'hobbit'

    :param title: Title to normalize for approximate comparison.
    :return: The same key as :func:`normalise_fuzzy_title`.
    """

    return normalise_fuzzy_title(title)


__all__ = ["fuzzy_title", "fuzzy_title_patterns", "normalise_fuzzy_title"]
