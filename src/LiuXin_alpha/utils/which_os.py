
"""
Tools for determining the OS we're currently running on.
"""


from __future__ import print_function

import os
import sys

_plat = sys.platform.lower()
iswindows = "win32" in _plat or "win64" in _plat
isosx = "darwin" in _plat
isnewosx = isosx and getattr(sys, "new_app_bundle", False)
isfreebsd = "freebsd" in _plat
isnetbsd = "netbsd" in _plat
isdragonflybsd = "dragonfly" in _plat
isbsd = isfreebsd or isnetbsd or isdragonflybsd
islinux = not (iswindows or isosx or isbsd)
isfrozen = hasattr(sys, "frozen")
isunix = isosx or islinux
isportable = os.environ.get("CALIBRE_PORTABLE_BUILD", None) is not None
ispy3 = sys.version_info.major > 2
isxp = iswindows and sys.getwindowsversion().major < 6

# Deals with the fact that sys.getwindowsversion is not defined on all systems
try:
    pass
except AttributeError:
    isxp = False
is64bit = sys.maxsize > (1 << 32)
