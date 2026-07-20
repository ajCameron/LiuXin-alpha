# -*- coding: utf-8 -*-

# Probably used as part of the reader system
from __future__ import annotations

import typing as _typing
try:
    from PyQt5.Qt import QPrinter
except Exception:
    class _QPrinterFallback:
        Millimeter = 0
        Point = 1
        Inch = 2
        Pica = 3
        Didot = 4
        Cicero = 5
        DevicePixel = 6

        A0 = 10
        A1 = 11
        A2 = 12
        A3 = 13
        A4 = 14
        A5 = 15
        A6 = 16
        A7 = 17
        A8 = 18
        A9 = 19
        B0 = 20
        B1 = 21
        B2 = 22
        B3 = 23
        B4 = 24
        B5 = 25
        B6 = 26
        B7 = 27
        B8 = 28
        B9 = 29
        B10 = 30
        C5E = 31
        Comm10E = 32
        DLE = 33
        Executive = 34
        Folio = 35
        Ledger = 36
        Legal = 37
        Letter = 38
        Tabloid = 39

        Portrait = 40
        Landscape = 41

    QPrinter = _QPrinterFallback

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

UNITS = {
    "millimeter": QPrinter.Millimeter,
    "point": QPrinter.Point,
    "inch": QPrinter.Inch,
    "pica": QPrinter.Pica,
    "didot": QPrinter.Didot,
    "cicero": QPrinter.Cicero,
    "devicepixel": QPrinter.DevicePixel,
}


def unit(unit: _typing.Any) -> _typing.Any:
    return UNITS.get(unit, QPrinter.Inch)


PAPER_SIZES = {
    "a0": QPrinter.A0,  # 841 x 1189 mm
    "a1": QPrinter.A1,  # 594 x 841 mm
    "a2": QPrinter.A2,  # 420 x 594 mm
    "a3": QPrinter.A3,  # 297 x 420 mm
    "a4": QPrinter.A4,  # 210 x 297 mm, 8.26 x 11.69 inches
    "a5": QPrinter.A5,  # 148 x 210 mm
    "a6": QPrinter.A6,  # 105 x 148 mm
    "a7": QPrinter.A7,  # 74 x 105 mm
    "a8": QPrinter.A8,  # 52 x 74 mm
    "a9": QPrinter.A9,  # 37 x 52 mm
    "b0": QPrinter.B0,  # 1030 x 1456 mm
    "b1": QPrinter.B1,  # 728 x 1030 mm
    "b2": QPrinter.B2,  # 515 x 728 mm
    "b3": QPrinter.B3,  # 364 x 515 mm
    "b4": QPrinter.B4,  # 257 x 364 mm
    "b5": QPrinter.B5,  # 182 x 257 mm, 7.17 x 10.13 inches
    "b6": QPrinter.B6,  # 128 x 182 mm
    "b7": QPrinter.B7,  # 91 x 128 mm
    "b8": QPrinter.B8,  # 64 x 91 mm
    "b9": QPrinter.B9,  # 45 x 64 mm
    "b10": QPrinter.B10,  # 32 x 45 mm
    "c5e": QPrinter.C5E,  # 163 x 229 mm
    "comm10e": QPrinter.Comm10E,  # 105 x 241 mm, U.S. Common 10 Envelope
    "dle": QPrinter.DLE,  # 110 x 220 mm
    "executive": QPrinter.Executive,  # 7.5 x 10 inches, 191 x 254 mm
    "folio": QPrinter.Folio,  # 210 x 330 mm
    "ledger": QPrinter.Ledger,  # 432 x 279 mm
    "legal": QPrinter.Legal,  # 8.5 x 14 inches, 216 x 356 mm
    "letter": QPrinter.Letter,  # 8.5 x 11 inches, 216 x 279 mm
    "tabloid": QPrinter.Tabloid,  # 279 x 432 mm
    # 'custom':  QPrinter.Custom,  # Unknown, or a user defined size.
}


def paper_size(size: _typing.Any) -> _typing.Any:
    return PAPER_SIZES.get(size, QPrinter.Letter)


ORIENTATIONS = {
    "portrait": QPrinter.Portrait,
    "landscape": QPrinter.Landscape,
}


def orientation(orientation: _typing.Any) -> _typing.Any:
    return ORIENTATIONS.get(orientation, QPrinter.Portrait)


def size(size: _typing.Any) -> _typing.Any:
    try:
        return int(size)
    except:
        return 1
