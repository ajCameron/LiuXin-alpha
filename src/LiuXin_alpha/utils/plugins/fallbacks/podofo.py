# -*- coding: utf-8 -*-
"""
Provide podofo utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise podofo through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class Error(Exception):
    """
    Report the Error Calibre compatibility failure.

    Example:
        Exercise Error through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    pass


@dataclass
class PDFDoc:
    """
    Provide the PDFDoc utility contract with explicit state and cleanup behavior.

    Example:
        Exercise PDFDoc through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    source: object

    def __post_init__(self) -> None:
        """
        Initialize and validate the PDFDoc state.

        Example:
            Exercise PDFDoc.  post init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: None; validated state is stored on the receiving object.
        """
        raise Error("podofo is not available; PDFDoc requires the compiled extension or an alternative backend")
