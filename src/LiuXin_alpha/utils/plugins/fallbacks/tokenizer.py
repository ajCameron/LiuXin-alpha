# -*- coding: utf-8 -*-
"""
Tokenize HTML input through the HTML5 state machine and parse-error recovery rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tokenizer through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from typing import Any, Iterable


def as_css(tokens: Any) -> str:
    """
    Perform the as css utility operation under explicit compatibility rules.

    Example:
        Exercise as css through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param tokens: Value supplied for tokens under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
