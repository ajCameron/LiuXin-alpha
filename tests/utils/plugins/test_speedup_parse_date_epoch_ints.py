# -*- coding: utf-8 -*-
"""
Ensure calibre_date.c_parse() handles integer timestamps consistently across speedup plugin layers.

The speedup plugin's parse_date() API is primarily for ISO-ish date strings. When an int/float is supplied,
the speedup layer may return None, so calibre_date.c_parse() must interpret "large" ints as unix epoch
seconds/ms and only treat small ints as years (legacy calibre behavior for values like 2001).
"""

from __future__ import annotations

from datetime import datetime

import pytest

from LiuXin_alpha.utils.libraries import calibre_date as cd


def test_c_parse_epoch_seconds_int_returns_datetime() -> None:
    dt = cd.c_parse(1_700_000_000)  # ~ 2023-11
    assert isinstance(dt, datetime)
    assert dt.year >= 2000


def test_c_parse_epoch_milliseconds_int_returns_datetime() -> None:
    dt = cd.c_parse(1_700_000_000_000)  # epoch ms
    assert isinstance(dt, datetime)
    assert dt.year >= 2000


def test_c_parse_small_int_is_treated_as_year() -> None:
    dt = cd.c_parse(2001)
    assert isinstance(dt, datetime)
    assert dt.year == 2001


def test_speedup_string_path_still_works() -> None:
    # This exercises the speedup plugin API (C extension or pure-python fallback).
    dt = cd.c_parse("2024-01-02 03:04:05+00:00")
    assert isinstance(dt, datetime)
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (2024, 1, 2, 3, 4, 5)


def test_epoch_int_still_parses_if_speedup_returns_none(monkeypatch) -> None:
    # Simulate a speedup layer that can't parse ints (returns None), which triggers the fallback heuristic.
    monkeypatch.setattr(cd, "_c_speedup", lambda raw: None)
    dt = cd.c_parse(1_700_000_000)
    assert isinstance(dt, datetime)
    assert dt.year >= 2000
