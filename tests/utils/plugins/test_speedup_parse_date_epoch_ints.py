# -*- coding: utf-8 -*-
"""
Provide test speedup parse date epoch ints utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test speedup parse date epoch ints through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""

from __future__ import annotations

from datetime import datetime

import pytest

from LiuXin_alpha.utils.libraries import calibre_date as cd


def test_c_parse_epoch_seconds_int_returns_datetime() -> None:
    """
    Perform the test c parse epoch seconds int returns datetime utility operation under explicit compatibility rules.

    Example:
        Exercise test c parse epoch seconds int returns datetime through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dt = cd.c_parse(1_700_000_000)  # ~ 2023-11
    assert isinstance(dt, datetime)
    assert dt.year >= 2000


def test_c_parse_epoch_milliseconds_int_returns_datetime() -> None:
    """
    Perform the test c parse epoch milliseconds int returns datetime utility operation under explicit compatibility rules.

    Example:
        Exercise test c parse epoch milliseconds int returns datetime through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dt = cd.c_parse(1_700_000_000_000)  # epoch ms
    assert isinstance(dt, datetime)
    assert dt.year >= 2000


def test_c_parse_small_int_is_treated_as_year() -> None:
    """
    Perform the test c parse small int is treated as year utility operation under explicit compatibility rules.

    Example:
        Exercise test c parse small int is treated as year through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dt = cd.c_parse(2001)
    assert isinstance(dt, datetime)
    assert dt.year == 2001


def test_speedup_string_path_still_works() -> None:
    # This exercises the speedup plugin API (C extension or pure-python fallback).
    """
    Perform the test speedup string path still works utility operation under explicit compatibility rules.

    Example:
        Exercise test speedup string path still works through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dt = cd.c_parse("2024-01-02 03:04:05+00:00")
    assert isinstance(dt, datetime)
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (2024, 1, 2, 3, 4, 5)


def test_epoch_int_still_parses_if_speedup_returns_none(monkeypatch) -> None:
    # Simulate a speedup layer that can't parse ints (returns None), which triggers the fallback heuristic.
    """
    Perform the test epoch int still parses if speedup returns none utility operation under explicit compatibility rules.

    Example:
        Exercise test epoch int still parses if speedup returns none through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(cd, "_c_speedup", lambda raw: None)
    dt = cd.c_parse(1_700_000_000)
    assert isinstance(dt, datetime)
    assert dt.year >= 2000
