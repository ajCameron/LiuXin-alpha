"""Shared types for catalog metadata tool API contracts."""

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
