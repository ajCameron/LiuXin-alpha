# -*- coding: utf-8 -*-
# Vendored from python-dateutil 2.9.0.post0 (https://github.com/dateutil/dateutil): dateutil/tz/__init__.py
# Licensed under the Apache License 2.0 or the BSD 3-Clause licence; see LICENSE in this folder.
from .tz import *
from .tz import __doc__

__all__ = ["tzutc", "tzoffset", "tzlocal", "tzfile", "tzrange",
           "tzstr", "tzical", "tzwin", "tzwinlocal", "gettz",
           "enfold", "datetime_ambiguous", "datetime_exists",
           "resolve_imaginary", "UTC", "DeprecatedTzFormatWarning"]


class DeprecatedTzFormatWarning(Warning):
    """Warning raised when time zones are parsed from deprecated formats."""
