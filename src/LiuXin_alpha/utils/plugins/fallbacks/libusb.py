# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``libusb`` extension.

Stub: in production, wire to pyusb or the compiled extension.
"""

from __future__ import annotations


class LibUSBError(Exception):
    pass
