"""Validate zero-based rating values without persistence or matching policy."""

from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation


def _rating_number(value: object) -> float:
    """Accept finite numeric input and numeric text, excluding booleans."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError("Rating must be a finite number or null.")
    try:
        number = float(value)
    except (ValueError, OverflowError):
        raise ValueError("Rating must be a finite number or null.") from None
    if not math.isfinite(number) or number < 0:
        raise ValueError("Rating must be finite and nonnegative.")
    return number


def _rating_scale(value: object) -> int:
    """Require a positive integral maximum without rounding fractional text."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError("Rating scale must be a positive integer or null.")
    try:
        scale = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("Rating scale must be a positive integer or null.") from None
    if (
        not scale.is_finite()
        or scale <= 0
        or scale > 2**63 - 1
        or scale != scale.to_integral_value()
    ):
        raise ValueError("Rating scale must be a positive integer or null.")
    return int(scale)


def normalise_rating_pair(
    value: object, out_of: object
) -> tuple[float | None, int | None]:
    """Validate a nullable rating and its optional zero-based scale.

    A missing value means unrated; a missing scale means the upper bound is not
    known. Numeric values must be finite and nonnegative. A supplied scale must
    fit a positive signed 64-bit whole number, and a supplied value cannot exceed it. Numeric
    text and Decimal inputs are accepted; booleans and empty strings are not.
    This does not rescale scores or derive a compatibility display field.

    Example:
        >>> normalise_rating_pair("4.25", "5.0")
        (4.25, 5)
        >>> normalise_rating_pair(None, 10)
        (None, 10)
    """
    rating = None if value is None else _rating_number(value)
    scale = None if out_of is None else _rating_scale(out_of)
    if rating is not None:
        # Check exact input bounds as well as the stored float: conversion can
        # round a tiny negative to zero or a just-too-large decimal down to the maximum.
        exact = Decimal(str(value))
        if exact < 0:
            raise ValueError("Rating must be finite and nonnegative.")
        if scale is not None and (exact > scale or rating > scale):
            raise ValueError("Rating cannot exceed its scale.")
    return rating, scale
