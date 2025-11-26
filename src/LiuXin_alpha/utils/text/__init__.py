
"""
Text manipulation tools.
"""

from typing import Optional

import re


def url_slash_cleaner(url):
    """
    Removes redundant /'s from urls.
    """
    return re.sub(r"(?<!:)/{2,}", "/", url)


def human_readable(size, sep=" "):
    """Convert a size in bytes into a human-readable form."""
    divisor, suffix = 1, "B"
    for i, candidate in enumerate(("B", "KB", "MB", "GB", "TB", "PB", "EB")):
        if size < (1 << ((i + 1) * 10)):
            divisor, suffix = (1 << (i * 10)), candidate
            break
    size = str(float(size) / divisor)
    if size.find(".") > -1:
        size = size[: size.find(".") + 2]
    if size.endswith(".0"):
        size = size[:-2]
    return size + sep + suffix


def remove_bracketed_text(src, brackets: Optional[dict[str, str]] = None) -> str:
    """
    Remove bracketed text from a given string.

    :param src:
    :param brackets: Optional dict keyed with the left bracket, valued with the right.
                     Defaults to {"(": ")", "[": "]", "{": "}"}
    :return:
    """
    brackets = brackets if brackets is not None else {"(": ")", "[": "]", "{": "}"}

    from collections import Counter

    counts = Counter()
    buf = []
    src = str(src)
    rmap = dict([(v, k) for k, v in brackets.items()])
    for char in src:
        if char in brackets:
            counts[char] += 1
        elif char in rmap:
            idx = rmap[char]
            if counts[idx] > 0:
                counts[idx] -= 1
        elif sum(counts.values()) < 1:
            buf.append(char)
    return "".join(buf)
