"""
Provide test date stubbed utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test date stubbed through a consuming regression::

        python -m pytest -q tests/utils/test_date_stubbed.py
"""
from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest


def _install_liuxin_date_stubs() -> None:
    """
    Provide minimal stubs so `LiuXin_alpha.utils.date` can import.

    Example:
        Exercise  install liuxin date stubs through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    liuxin = types.ModuleType("LiuXin")
    utils = types.ModuleType("LiuXin.utils")

    # LiuXin.utils.calibre.strftime has the same signature as time.strftime, with `t=`
    calibre = types.ModuleType("LiuXin.utils.calibre")
    import time

    def strftime(fmt: str, t=None):
        """
        Perform the strftime utility operation under explicit compatibility rules.

        Example:
            Exercise  install liuxin date stubs.strftime through a consuming regression::

                python -m pytest -q tests/utils/test_date_stubbed.py


        :param fmt: Date, number or template format specification.
        :param t: Value supplied for t under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return time.strftime(fmt, t)

    calibre.strftime = strftime  # type: ignore[attr-defined]

    # localization table used for day/month names
    localization = types.ModuleType("LiuXin.utils.localization")
    localization.lcdata = {
        "abday": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
        "day": ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
        "abmon": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        "mon": [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ],
    }

    translator = types.ModuleType("LiuXin.utils.localization.translator")
    translator._ = lambda s: s  # type: ignore[attr-defined]
    translator.localize_manual = lambda s: s  # type: ignore[attr-defined]
    localization.localize_user_manual_link = lambda s: s  # type: ignore[attr-defined]

    # dateutil alias
    lx_libs = types.ModuleType("LiuXin.utils.libraries")
    lx_dateutil = types.ModuleType("LiuXin.utils.lx_libraries.dateutil")
    from dateutil import parser, tz

    lx_dateutil.parser = parser  # type: ignore[attr-defined]
    lx_dateutil.tz = tz  # type: ignore[attr-defined]

    constants = types.ModuleType("LiuXin.constants")
    constants.iswindows = False  # type: ignore[attr-defined]

    sys.modules.setdefault("LiuXin", liuxin)
    sys.modules.setdefault("LiuXin.utils", utils)
    sys.modules.setdefault("LiuXin.utils.calibre", calibre)
    sys.modules.setdefault("LiuXin.utils.localization", localization)
    sys.modules.setdefault("LiuXin.utils.localization.translator", translator)
    sys.modules.setdefault("LiuXin.utils.lx_libraries", lx_libs)
    sys.modules.setdefault("LiuXin.utils.lx_libraries.dateutil", lx_dateutil)
    sys.modules.setdefault("LiuXin.constants", constants)


def test_format_date_and_iso_helpers_importable() -> None:
    """
    Perform the test format date and iso helpers importable utility operation under explicit compatibility rules.

    Example:
        Exercise test format date and iso helpers importable through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_liuxin_date_stubs()

    import importlib
    import LiuXin_alpha.utils.date as d

    importlib.reload(d)

    dt = datetime(2024, 12, 31, 23, 59, 58, tzinfo=timezone.utc)
    s = d.format_date(dt, "dd MMM yyyy")
    assert "31" in s and "Dec" in s and "2024" in s

    assert d.isoformat_timestamp(0).startswith("1970")
    assert isinstance(d.timestampfromdt(dt), float)


def test_fix_only_date_handles_empty_and_partial_dates() -> None:
    """
    Perform the test fix only date handles empty and partial dates utility operation under explicit compatibility rules.

    Example:
        Exercise test fix only date handles empty and partial dates through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_liuxin_date_stubs()

    import importlib
    import LiuXin_alpha.utils.date as d

    importlib.reload(d)

    assert d.fix_only_date("") == ""
    assert d.fix_only_date("2020") == "2020-01-01"
    assert d.fix_only_date("2020-07") == "2020-07-01"


def test_isoformat_rejects_bad_date() -> None:
    """
    Perform the test isoformat rejects bad date utility operation under explicit compatibility rules.

    Example:
        Exercise test isoformat rejects bad date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_liuxin_date_stubs()

    import importlib
    import LiuXin_alpha.utils.date as d

    importlib.reload(d)

    with pytest.raises(Exception):
        # Bad input type
        d.isoformat("not-a-date")  # type: ignore[arg-type]
