"""Shared value types for the Row-oriented metadata helper contracts.

``LinkPriority`` accepts a numeric order, ``"highest"`` for helper-assigned
precedence, or ``None`` for backend defaults. Date-like values are normalized
by the concrete helper before persistence.
"""

from __future__ import annotations

import datetime

from typing import Any, Mapping, TypeAlias

from LiuXin_alpha.databases.api import RowAPI

DateLike: TypeAlias = int | float | datetime.date | datetime.datetime | str
IsoDateLike: TypeAlias = datetime.date | datetime.datetime | str
LinkPriority: TypeAlias = int | float | str | None
RowMapping: TypeAlias = Mapping[str, Any]
RowOrMapping: TypeAlias = RowAPI | RowMapping
RowValue: TypeAlias = str | int | float | datetime.datetime
TextOrRow: TypeAlias = str | RowAPI

__all__ = [
    "DateLike",
    "IsoDateLike",
    "LinkPriority",
    "RowMapping",
    "RowOrMapping",
    "RowValue",
    "TextOrRow",
]
