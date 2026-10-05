# -*- coding: utf-8 -*-
"""
Provide wpd utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise wpd through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations


class WpdError(Exception):
    """
    Report the WpdError Calibre compatibility failure.

    Example:
        Exercise WpdError through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    pass
