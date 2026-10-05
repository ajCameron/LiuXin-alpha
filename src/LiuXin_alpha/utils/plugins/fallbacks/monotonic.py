# -*- coding: utf-8 -*-
"""
Provide monotonic utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise monotonic through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import time


def monotonic() -> float:
    """
    Perform the monotonic utility operation under explicit compatibility rules.

    Example:
        Exercise monotonic through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return time.monotonic()
