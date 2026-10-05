"""
Provide test fallback speedup utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test fallback speedup through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest


def test_speedup_parse_date_basic_and_with_timezone() -> None:
    """
    Perform the test speedup parse date basic and with timezone utility operation under explicit compatibility rules.

    Example:
        Exercise test speedup parse date basic and with timezone through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import speedup

    assert speedup.parse_date("2025-12-31 01:02:03") == (2025, 12, 31, 1, 2, 3, 0)
    assert speedup.parse_date("   2025-12-31 01:02:03+02:30") == (2025, 12, 31, 1, 2, 3, 9000)
    assert speedup.parse_date("2025-12-31 01:02:03-01:00") == (2025, 12, 31, 1, 2, 3, -3600)


def test_speedup_parse_date_invalid_returns_none() -> None:
    """
    Perform the test speedup parse date invalid returns none utility operation under explicit compatibility rules.

    Example:
        Exercise test speedup parse date invalid returns none through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import speedup

    assert speedup.parse_date("") is None
    assert speedup.parse_date("2025") is None
    assert speedup.parse_date("2025-xx-31 01:02:03") is None
    assert speedup.parse_date("2025-12-31 01:02:03+ab:cd") is None


def test_speedup_pdf_float_matches_rules() -> None:
    """
    Perform the test speedup pdf float matches rules utility operation under explicit compatibility rules.

    Example:
        Exercise test speedup pdf float matches rules through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import speedup

    assert speedup.pdf_float(0.0) == "0"
    assert speedup.pdf_float(1e-12) == "0"
    assert speedup.pdf_float(1.23456789)  # non-empty
    assert speedup.pdf_float(1.0) == "1"
    assert speedup.pdf_float(1.50) == "1.5"
    assert speedup.pdf_float(-2.0) == "-2"


def test_speedup_create_texture_ppm_shape_and_header() -> None:
    """
    Perform the test speedup create texture ppm shape and header utility operation under explicit compatibility rules.

    Example:
        Exercise test speedup create texture ppm shape and header through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import speedup

    # Keep it tiny; we only care that it is a valid-ish PPM blob.
    out = speedup.create_texture(8, 6, 10, 20, 30)
    assert out.startswith(b"P6\n")

    # Parse header: P6\n{w} {h}\n255\n
    header_end = out.find(b"\n255\n")
    assert header_end != -1
    header = out[: header_end + len(b"\n255\n")].decode("ascii")
    assert "8 6" in header
    payload = out[header_end + len(b"\n255\n") :]
    assert len(payload) == 8 * 6 * 3


def test_speedup_detach_redirects_stdio_to_devnull_in_subprocess() -> None:
    """
    Detach is intentionally invasive; validate it in a subprocess.

    Example:
        Exercise test speedup detach redirects stdio to devnull in subprocess through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_speedup.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    code = (
        "from LiuXin_alpha.utils.plugins.fallbacks.speedup import detach; "
        "import os, sys; "
        "detach(os.devnull); "
        "print('VISIBLE?'); sys.stdout.flush()"
    )
    cp = subprocess.run([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)

    # If detach worked, stdout should be empty.
    assert (cp.stdout or b"") == b""
