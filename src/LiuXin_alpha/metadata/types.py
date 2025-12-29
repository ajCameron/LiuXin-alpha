# src/LiuXin_alpha/metadata/containers/calibre_like_book_metadata/types.py
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import datetime
from os import PathLike
from typing import Any, Protocol, runtime_checkable, TypeAlias

# Common internal container pattern in your codebase:
# OrderedDict keyed by "display value", valued by database id or None.
DbId: TypeAlias = int
ValueToId: TypeAlias = OrderedDict[str, DbId | None]

CreatorsDump: TypeAlias = dict[str, ValueToId]
IdentifiersDump: TypeAlias = dict[str, ValueToId]

# What your set_identifiers docstring allows (string, set, OrderedDict).
IdentifierValues: TypeAlias = str | set[str] | Sequence[str] | ValueToId
IdentifiersInput: TypeAlias = Mapping[str, IdentifierValues]

# File / cover inputs: your code accepts paths, bytes-ish, or readable streams.
Pathish: TypeAlias = str | PathLike[str]

@runtime_checkable
class BinaryReadable(Protocol):
    def read(self, n: int = -1) -> bytes: ...

@runtime_checkable
class SupportsClose(Protocol):
    def close(self) -> Any: ...

FileData: TypeAlias = Pathish | bytes | BinaryReadable
CoverData: TypeAlias = Pathish | bytes | BinaryReadable

# A "calibre-like" metadata object as actually used in from_calibre()
@runtime_checkable
class CalibreMetadataLike(Protocol):
    title: str | None
    authors: Sequence[str] | None

    # Optional-ish attrs (duck-typed): calibre sometimes has one/both.
    author_sort: str | None
    creator_sort: str | None

    # Your code checks for pubdate or pub_date; calibre commonly uses datetime.
    pubdate: datetime | None
    pub_date: datetime | None

    application_id: str | None
    applicationid: str | None

    languages: Sequence[str] | None
    book_producer: str | None
    producer: str | None

    cover: Any  # calibre cover field varies; keep loose.

    def get_identifiers(self) -> Mapping[str, str] | Mapping[str, IdentifierValues]: ...
