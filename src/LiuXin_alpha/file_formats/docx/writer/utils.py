#!/usr/bin/env python2
# vim:fileencoding=utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import re
from collections import namedtuple

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

try:
    from tinycss.color3 import parse_color_string
except ImportError:
    _Color = namedtuple("_Color", "red green blue alpha")

    _NAMED_COLORS = {
        "aliceblue": (240, 248, 255),
        "black": (0, 0, 0),
        "blue": (0, 0, 255),
        "green": (0, 128, 0),
        "lime": (0, 255, 0),
        "red": (255, 0, 0),
        "white": (255, 255, 255),
        "yellow": (255, 255, 0),
    }
    _RGB_RE = re.compile(r"^rgba?\((.*?)\)$", re.IGNORECASE)

    def _clamp_channel(x):
        return max(0, min(255, int(x)))

    def _parse_component(raw):
        raw = raw.strip()
        if raw.endswith("%"):
            return _clamp_channel(round(float(raw[:-1]) * 255.0 / 100.0))
        return _clamp_channel(float(raw))

    def _parse_alpha(raw):
        raw = raw.strip()
        if raw.endswith("%"):
            return max(0.0, min(1.0, float(raw[:-1]) / 100.0))
        val = float(raw)
        if val > 1:
            return max(0.0, min(1.0, val / 255.0))
        return max(0.0, min(1.0, val))

    def parse_color_string(value):
        if value is None:
            return None
        raw = str(value).strip().lower()
        if not raw:
            return None
        if raw in {"transparent", "none"}:
            return _Color(0.0, 0.0, 0.0, 0.0)
        if raw in _NAMED_COLORS:
            r, g, b = _NAMED_COLORS[raw]
            return _Color(r / 255.0, g / 255.0, b / 255.0, 1.0)
        if raw.startswith("#"):
            hx = raw[1:]
            if len(hx) == 3:
                hx = "".join(ch * 2 for ch in hx)
            if len(hx) != 6 or not re.match(r"^[0-9a-f]{6}$", hx):
                return None
            r = int(hx[0:2], 16)
            g = int(hx[2:4], 16)
            b = int(hx[4:6], 16)
            return _Color(r / 255.0, g / 255.0, b / 255.0, 1.0)
        m = _RGB_RE.match(raw)
        if not m:
            return None
        parts = [x.strip() for x in m.group(1).split(",")]
        if len(parts) not in {3, 4}:
            return None
        try:
            r, g, b = (_parse_component(x) for x in parts[:3])
            alpha = _parse_alpha(parts[3]) if len(parts) == 4 else 1.0
        except Exception:
            return None
        return _Color(r / 255.0, g / 255.0, b / 255.0, alpha)


def int_or_zero(raw):
    try:
        return int(raw)
    except (ValueError, TypeError, AttributeError):
        return 0


# convert_color() {{{
def convert_color(value):
    if not value:
        return
    if value.lower() == "currentcolor":
        return "auto"
    val = parse_color_string(value)
    if val is None:
        return
    if val.alpha < 0.01:
        return
    return "%02X%02X%02X" % (
        int(val.red * 255),
        int(val.green * 255),
        int(val.blue * 255),
    )


def test_convert_color():
    import unittest

    class TestColors(unittest.TestCase):
        def test_color_conversion(self):
            ae = self.assertEqual
            cc = convert_color
            ae(None, cc(None))
            ae(None, cc("transparent"))
            ae(None, cc("none"))
            ae(None, cc("#12j456"))
            ae("auto", cc("currentColor"))
            ae("F0F8FF", cc("AliceBlue"))
            ae("000000", cc("black"))
            ae("FF0000", cc("red"))
            ae("00FF00", cc("lime"))
            ae(cc("#001"), "000011")
            ae("12345D", cc("#12345d"))
            ae("FFFFFF", cc("rgb(255, 255, 255)"))
            ae("FF0000", cc("rgba(255, 0, 0, 23)"))

    tests = unittest.defaultTestLoader.loadTestsFromTestCase(TestColors)
    unittest.TextTestRunner(verbosity=4).run(tests)


# }}}
