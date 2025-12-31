# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``speedup`` extension.

This module is intended to be API-compatible with calibre/LiuXin's C extension:
- parse_date(raw) -> (year, month, day, hour, minute, second, tzsecs) | None
- pdf_float(f) -> str
- detach(devnull_path) -> None
- create_texture(...) -> bytes (PPM P6)
"""

from __future__ import annotations

import math
import os
import random
import sys
from typing import Optional, Tuple


O_CLOEXEC: int = getattr(os, "O_CLOEXEC", 0)


def parse_date(raw: object) -> Optional[Tuple[int, int, int, int, int, int, int]]:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        s = raw.decode("ascii", "ignore")
    else:
        s = str(raw)
    s = s.lstrip(" \t\n\r\f\v")
    if len(s) < 19:
        return None
    try:
        year_s = s[0:4]
        month_s = s[5:7]
        day_s = s[8:10]
        hour_s = s[11:13]
        minute_s = s[14:16]
        second_s = s[17:19]
        if not (year_s.isdigit() and month_s.isdigit() and day_s.isdigit()
                and hour_s.isdigit() and minute_s.isdigit() and second_s.isdigit()):
            return None
        year = int(year_s)
        month = int(month_s)
        day = int(day_s)
        hour = int(hour_s)
        minute = int(minute_s)
        second = int(second_s)

        tzsecs = 0
        tail = s[-6:]
        if tail and tail[0] in "+-":
            sign = 1 if tail[0] == "+" else -1
            tzh_s = tail[1:3]
            tzm_s = tail[4:6]
            if not (tzh_s.isdigit() and tzm_s.isdigit()):
                return None
            tzsecs = sign * (int(tzh_s) * 60 + int(tzm_s)) * 60
        return (year, month, day, hour, minute, second, tzsecs)
    except Exception:
        return None


def pdf_float(f: float) -> str:
    try:
        f = float(f)
    except Exception as e:
        raise TypeError("pdf_float expects a float-like value") from e

    a = abs(f)
    if not (a > 1.0e-7):
        return "0"

    precision = 6
    if a > 1.0:
        precision = 6 - int(math.log10(a))
        if precision < 0:
            precision = 0
        elif precision > 6:
            precision = 6

    s = format(f, f".{precision}f")
    if precision > 0:
        s = s.rstrip("0")
        if s.endswith(".") or s.endswith(","):
            s = s[:-1]
    if "," in s:
        s = s.replace(",", ".")
    return s


def detach(devnull: str) -> None:
    if not isinstance(devnull, str):
        devnull = str(devnull)

    fd_in = os.open(devnull, os.O_RDONLY)
    fd_out = os.open(devnull, os.O_WRONLY)
    try:
        os.dup2(fd_in, 0)
        os.dup2(fd_out, 1)
        os.dup2(fd_out, 2)
    finally:
        for fd in (fd_in, fd_out):
            try:
                os.close(fd)
            except OSError:
                pass

    try:
        sys.stdin = open(devnull, "r")
    except Exception:
        pass
    try:
        sys.stdout = open(devnull, "w")
    except Exception:
        pass
    try:
        sys.stderr = open(devnull, "w")
    except Exception:
        pass


def create_texture(
    width: int,
    height: int,
    red: int,
    green: int,
    blue: int,
    blend_red: int = 0,
    blend_green: int = 0,
    blend_blue: int = 0,
    blend_alpha: float = 0.1,
    density: float = 0.7,
    weight: int = 3,
    radius: float = 1.0,
) -> bytes:
    if weight % 2 != 1 or weight < 1:
        raise ValueError("The weight must be an odd positive number")
    if radius <= 0:
        raise ValueError("The radius must be positive")
    if width > 100000 or height > 10000:
        raise ValueError("The width or height is too large")
    if width < 1 or height < 1:
        raise ValueError("The width or height is too small")

    def _uc(x: int) -> int:
        return 0 if x < 0 else 255 if x > 255 else int(x)

    base_r, base_g, base_b = _uc(red), _uc(green), _uc(blue)
    br, bg, bb = _uc(blend_red), _uc(blend_green), _uc(blend_blue)
    blend_alpha = float(blend_alpha)
    density = float(density)

    center = weight // 2
    sqr = radius * radius
    factor = 1.0 / (2.0 * math.pi * sqr)
    denom = 2.0 * sqr

    kernel = [0.0] * (weight * weight)
    ksum = 0.0
    for r in range(weight):
        for c in range(weight):
            val = factor * math.exp(-(((r - center) ** 2 + (c - center) ** 2) / denom))
            kernel[r * weight + c] = val
            ksum += val
    inv = 1.0 / ksum
    kernel = [v * inv for v in kernel]

    size = width * height
    mask = [0.0] * size
    for i in range(size):
        if random.random() <= density:
            mask[i] = blend_alpha

    half = weight // 2
    src = mask[:]

    def clamp(v: int, lo: int, hi: int) -> int:
        return lo if v < lo else hi if v > hi else v

    for rr in range(height):
        row_off = rr * width
        for cc in range(width):
            acc = 0.0
            for i in range(-half, half + 1):
                r2 = clamp(rr + i, 0, height - 1)
                r2_off = r2 * width
                ki_off = (half + i) * weight
                for j in range(-half, half + 1):
                    c2 = clamp(cc + j, 0, width - 1)
                    acc += src[r2_off + c2] * kernel[ki_off + (half + j)]
            if acc < 0.0:
                acc = 0.0
            elif acc > 1.0:
                acc = 1.0
            mask[row_off + cc] = acc

    header = f"P6\n{int(width)} {int(height)}\n255\n".encode("ascii")
    out = bytearray(header)

    for m in mask:
        invm = 1.0 - m
        r = int(br * m) + int(base_r * invm)
        g = int(bg * m) + int(base_g * invm)
        b = int(bb * m) + int(base_b * invm)
        out.append(r & 0xFF)
        out.append(g & 0xFF)
        out.append(b & 0xFF)

    return bytes(out)
