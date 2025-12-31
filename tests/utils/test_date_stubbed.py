from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest


def _install_liuxin_date_stubs() -> None:
    """Provide minimal stubs so `LiuXin_alpha.utils.date` can import.

    The alpha tree still references some legacy `LiuXin.*` modules.
    These stubs let the unit tests exercise the pure date logic without
    needing the full legacy package.
    """
    liuxin = types.ModuleType("LiuXin")
    utils = types.ModuleType("LiuXin.utils")

    # LiuXin.utils.calibre.strftime has the same signature as time.strftime, with `t=`
    calibre = types.ModuleType("LiuXin.utils.calibre")
    import time

    def strftime(fmt: str, t=None):
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
    _install_liuxin_date_stubs()

    import importlib
    import LiuXin_alpha.utils.date as d

    importlib.reload(d)

    assert d.fix_only_date("") == ""
    assert d.fix_only_date("2020") == "2020-01-01"
    assert d.fix_only_date("2020-07") == "2020-07-01"


def test_isoformat_rejects_bad_date() -> None:
    _install_liuxin_date_stubs()

    import importlib
    import LiuXin_alpha.utils.date as d

    importlib.reload(d)

    with pytest.raises(Exception):
        # Bad input type
        d.isoformat("not-a-date")  # type: ignore[arg-type]