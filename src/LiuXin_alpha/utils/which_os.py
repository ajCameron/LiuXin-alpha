
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
try:
    _windows_version = sys.getwindowsversion() if iswindows else None
except AttributeError:
    _windows_version = None
isxp = iswindows and _windows_version is not None and _windows_version.major < 6
is64bit = sys.maxsize > (1 << 32)
