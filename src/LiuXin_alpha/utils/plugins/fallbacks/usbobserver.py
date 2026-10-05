# -*- coding: utf-8 -*-
"""
Provide usbobserver utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise usbobserver through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations


def date_format() -> str:
    # A reasonable default date format.
    """
    Perform the date format utility operation under explicit compatibility rules.

    Example:
        Exercise date format through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "%Y-%m-%d"
