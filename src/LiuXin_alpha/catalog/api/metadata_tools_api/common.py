"""
Define shared annotations for legacy row-oriented metadata contracts.

RowOrMapping permits database Rows or mappings; TextOrRow accepts text or
a Row. LinkPriority includes numbers, strings such as highest, and None; its
annotation does not validate allowed strings. DateLike permits numeric dates
while IsoDateLike does not. Concrete helpers decide normalization and storage
behavior; type aliases themselves enforce no runtime conversions.
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
