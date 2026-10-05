# -*- coding: utf-8 -*-
"""
Provide libmtp utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise libmtp through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations


class LibMTPError(Exception):
    """
    Report the LibMTPError Calibre compatibility failure.

    Example:
        Exercise LibMTPError through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    pass
