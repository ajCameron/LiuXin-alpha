# -*- coding: utf-8 -*-
"""
Provide winfonts utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise winfonts through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations


def font_families():
    """
    Perform the font families utility operation under explicit compatibility rules.

    Example:
        Exercise font families through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return []
