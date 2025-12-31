# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``tokenizer`` extension.

The compiled extension is a speedup for CSS token streams (used by tinycss-like code).
We provide a single helper:
    as_css(tokens) -> str

Best-effort: accept a string/bytes (returned/decoded) or an iterable of tokens.
If tokens are tuples, we prefer the second element as the "value".
"""

from __future__ import annotations

from typing import Any, Iterable


def as_css(tokens: Any) -> str:
    if tokens is None:
        return ""
    if isinstance(tokens, bytes):
        return tokens.decode("utf-8", "replace")
    if isinstance(tokens, str):
        return tokens
    parts = []
    try:
        it = iter(tokens)
    except TypeError:
        return str(tokens)
    for t in it:
        if isinstance(t, str):
            parts.append(t)
        elif isinstance(t, bytes):
            parts.append(t.decode("utf-8", "replace"))
        elif isinstance(t, tuple) and len(t) >= 2:
            parts.append(str(t[1]))
        else:
            parts.append(str(t))
    return "".join(parts)
