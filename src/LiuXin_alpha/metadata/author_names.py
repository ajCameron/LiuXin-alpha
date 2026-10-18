"""
Derive bibliographic sort values from author names without database access.

The module owns the legacy prefix, suffix, copy-word and comma policy as a pure
metadata-value transformation.  Database drivers may adapt stored values before
calling it, but do not define the bibliographic rule.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.text import remove_bracketed_text

AuthorSortMethod = Literal["copy", "comma", "nocomma"]


def author_to_author_sort(
    author: str,
    method: AuthorSortMethod | None = "comma",
) -> str:
    """
    Derive a surname-first sort value under the configured author-name policy.

    Bracketed text is ignored for tokenization.  Short names, configured copy
    words, explicit copy mode, exhausted prefix/suffix tokens, and comma-mode
    names already containing a comma preserve the supplied spelling.  Passing
    ``None`` selects the configured method; the explicit default remains comma
    mode so callers cannot change behavior when preferences change accidentally.

    Example:
        >>> author_to_author_sort("Dr Arthur C Clarke Jr", method="nocomma")
        'Clarke Arthur C Jr'


    :param author: Author name to turn into a bibliographic sort value.
    :param method: Explicit copy/comma/nocomma policy, or None to read the
        author_sort_copy_method preference.
    :return: Derived sort value, the original name when policy requires it, or
        empty text for a falsy name.
    """

    if not author:
        return ""
    tokens = remove_bracketed_text(author).strip().split()
    if len(tokens) < 2:
        return author
    if method is None:
        try:
            method = preferences["author_sort_copy_method"]
        except KeyError:
            method = "comma"

    lowered_tokens = frozenset(token.lower() for token in tokens)
    try:
        copy_words = frozenset(
            word.lower() for word in preferences["author_name_copywords"]
        )
    except KeyError:
        copy_words = frozenset()
    if lowered_tokens.intersection(copy_words):
        method = "copy"
    if method == "copy":
        return author

    prefixes = {prefix.lower() for prefix in preferences["author_name_prefixes"]}
    prefixes |= {prefix + "." for prefix in prefixes}
    while tokens and tokens[0].lower() in prefixes:
        tokens = tokens[1:]
    if not tokens:
        return author

    suffixes = {suffix.lower() for suffix in preferences["author_name_suffixes"]}
    suffixes |= {suffix + "." for suffix in suffixes}
    retained_suffixes: list[str] = []
    while tokens and tokens[-1].lower() in suffixes:
        retained_suffixes.insert(0, tokens.pop())
    if not tokens:
        return author
    if method == "comma" and "," in "".join(tokens):
        return author

    sorted_tokens = tokens[-1:] + tokens[:-1]
    name_token_count = len(sorted_tokens)
    sorted_tokens.extend(retained_suffixes)
    if method != "nocomma" and name_token_count > 1:
        sorted_tokens[0] += ","
    return " ".join(sorted_tokens)


def authors_to_sort_string(
    authors: Iterable[str],
    *,
    method: AuthorSortMethod | None = "comma",
) -> str:
    """
    Derive and join author sort values in encounter order.

    Example:
        >>> authors_to_sort_string(["Arthur C Clarke", "Ada Lovelace"])
        'Clarke, Arthur C & Lovelace, Ada'


    :param authors: Author names to transform without filtering empty results.
    :param method: Policy forwarded to each author transformation; None reads
        the configured preference.
    :return: Sort values joined with a spaced ampersand.
    """

    return " & ".join(
        author_to_author_sort(author, method=method) for author in authors
    )


__all__ = ["AuthorSortMethod", "author_to_author_sort", "authors_to_sort_string"]
