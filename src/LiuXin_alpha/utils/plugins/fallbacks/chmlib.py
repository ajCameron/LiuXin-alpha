# -*- coding: utf-8 -*-
"""
Provide chmlib utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise chmlib through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations


class CHMError(Exception):
    """
    Report the CHMError Calibre compatibility failure.

    Example:
        Exercise CHMError through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    pass
