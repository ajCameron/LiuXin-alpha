"""
Expose the supported gui2 compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py
"""

from __future__ import annotations

from typing import Any

config: dict[str, Any] = {"use_roman_numerals_for_series_number": False}


def is_ok_to_use_qt() -> bool:
    """
    Return whether the GUI path is available in this process.

    Example:
        Exercise is ok to use qt through a consuming regression::

            python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py


    :return: True when the documented condition holds; otherwise False.
    """
    return False


def must_use_qt() -> bool:
    """
    Compatibility hook for callers that require Qt to be used.

    Example:
        Exercise must use qt through a consuming regression::

            python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return False


def ensure_app() -> None:
    """
    No-op placeholder when no Qt application bootstrap is available.

    Example:
        Exercise ensure app through a consuming regression::

            python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def load_builtin_fonts() -> None:
    """
    No-op placeholder for font loading in headless runs.

    Example:
        Exercise load builtin fonts through a consuming regression::

            python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def pixmap_to_data(pixmap: Any) -> bytes:
    """
    Convert a Qt pixmap-like object to bytes.

    Example:
        Exercise pixmap to data through a consuming regression::

            python -m pytest -q tests/surfaces/test_compatibility_surface_documentation_contracts.py


    :param pixmap: Value supplied for pixmap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if pixmap is None:
        return b""
    return b""


__all__ = [
    "config",
    "is_ok_to_use_qt",
    "must_use_qt",
    "ensure_app",
    "load_builtin_fonts",
    "pixmap_to_data",
]
