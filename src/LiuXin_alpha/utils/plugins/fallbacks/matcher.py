# -*- coding: utf-8 -*-
"""
Provide matcher utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise matcher through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

import difflib
from typing import List, Sequence, Tuple


def ratio(a: str, b: str) -> float:
    """
    Perform the ratio utility operation under explicit compatibility rules.

    Example:
        Exercise ratio through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param a: Value supplied for a under the utility contract.
    :param b: Value supplied for b under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return difflib.SequenceMatcher(a=a, b=b).ratio()
