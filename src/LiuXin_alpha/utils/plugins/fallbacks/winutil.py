# -*- coding: utf-8 -*-
"""
Provide winutil utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise winutil through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import time
from typing import Optional, Tuple


def strftime(fmt: str, t: Optional[Tuple[int, ...]] = None) -> str:
    """
    Perform the strftime utility operation under explicit compatibility rules.

    Example:
        Exercise strftime through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param fmt: Date, number or template format specification.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if t is None:
        return time.strftime(fmt)
    return time.strftime(fmt, t)  # type: ignore[arg-type]
