
"""
API for the metadata classes.

These classes include all the metadata containers for all types of metadata
These include
 - document metadata
 - file metadata

It also includes the API for the plugins used to read and write metadata from files.
"""

from __future__ import annotations


import abc
import dataclasses

from typing import Optional


class MetadataContainerAPI(abc.ABC):
    """
    Fundamental API for the metadata containers.

    Probably SO fundamental that there's no actual stuff that can be hung on it.
    But it seems neater to have something at the root of the class hierarchy.
    """


class BookMetadataContainerAPI(MetadataContainerAPI):
    """
    Complete metadata for a "book" shaped object.

    Contains all the metadata for a book in various types of container.
    """
    def __init__(self, title: str) -> None:
        """
        Container for all the metadata of the book.

        :param title:
        """
        self.title = title


class FileAddStatusAPI:
    """
    Status of trying to add a file to a store.

    The status should depend on the store itself.
    """
    status: bool                        # - True if we did add, False otherwise
    error_str: Optional[str] = None     # - If something has gone wrong


class WorkContainerAPI:
    """
    API for the work containers.
    """




@dataclasses.dataclass
class FileMetadata:
    """
    Metadata for a single file.

    Contains information such as
     - store id
     - file size
     - file hash
     - file url
    """


@dataclasses.dataclass
class FileLineage:
    """
    Files can be derived from each other, so each file can have a lineage.

    (often this lineage will be one file deep).
    """




@dataclasses.dataclass
class FilesMetadata:
    """
    Metadata for a collection of files - usually a collection of file lineages.
    """




@dataclasses.dataclass
class LibraryMetadata:
    """
    Combined book and file metadata
    """



@dataclasses.dataclass
class RawMetadata:
    """
    For when you don't want to have to do heavy processing.
    """





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

