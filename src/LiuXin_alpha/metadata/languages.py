"""Normalize bibliographic language collections without database access."""

from __future__ import annotations

from collections.abc import Iterable

from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang

_IGNORED_CANONICAL_LANGUAGE_VALUES = frozenset(("und", "zxx", "mis", "mul"))


def canonicalize_languages(languages: Iterable[str]) -> tuple[str, ...]:
    """Canonicalize, filter, and de-duplicate languages in encounter order.

    This preserves the historical adapter policy: canonicalization requests language
    names rather than codes, then filters false results and literal special codes.
    Consequently a recognized ``und`` currently becomes ``Undetermined`` and remains
    present. Canonicalizer errors propagate.

    Example:
        >>> canonicalize_languages(("eng", "eng", "und"))
        ('English', 'Undetermined')

    :param languages: Language names or codes in source order.
    :return: Distinct accepted canonical language names in encounter order.
    """

    canonical_languages: list[str] = []
    for language in languages:
        canonical_language = canonicalize_lang(language)
        if (
            not canonical_language
            or canonical_language in canonical_languages
            or canonical_language in _IGNORED_CANONICAL_LANGUAGE_VALUES
        ):
            continue
        canonical_languages.append(canonical_language)
    return tuple(canonical_languages)


__all__ = ["canonicalize_languages"]
