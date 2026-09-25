
"""
Define metadata input aliases, structural I/O/container protocols, and string-valued credit-role enums.

The runtime-checkable protocols describe required members; runtime checks do not
validate their type annotations. Numeric ID aliases refer to int without creating
distinct runtime types.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/test_relation_container_contracts.py
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import StrEnum
from os import PathLike
from typing import Any, Protocol, runtime_checkable, TypeAlias, Union, Literal


AgentTypes: TypeAlias = Union[
    Literal["human"],
    Literal["person"],
    Literal["organization"],
    Literal["organisation"],
    Literal["group"],
    Literal["pseudonym"],
]

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
    """
    Describe an object with a binary read method; this protocol owns no stream or buffer.

    Example:
        >>> from io import BytesIO
        >>> isinstance(BytesIO(b'data'), BinaryReadable)
        True
    """
    def read(self, n: int = -1) -> bytes:
        """
        Specify binary reading for protocol implementers without supplying a concrete read operation.

        Example:
            >>> from io import BytesIO
            >>> BytesIO(b'data').read(2)
            b'da'


        :param n: Requested read size; -1 conventionally requests all remaining bytes.
        :return: Bytes returned by the implementing object.
        """
        ...

@runtime_checkable
class SupportsClose(Protocol):
    """
    Describe an object exposing close, without defining resource ownership or cleanup semantics.

    Example:
        >>> from io import BytesIO
        >>> isinstance(BytesIO(), SupportsClose)
        True
    """
    def close(self) -> Any:
        """
        Specify a close operation whose resource effects are supplied by the implementing object.

        Example:
            >>> from io import BytesIO
            >>> stream = BytesIO()
            >>> stream.close()
            >>> stream.closed
            True


        :return: Implementation-defined result; the protocol imposes no concrete return
            value.
        """
        ...

FileData: TypeAlias = Pathish | bytes | BinaryReadable
CoverData: TypeAlias = Pathish | bytes | BinaryReadable

# A "calibre-like" metadata object as actually used in from_calibre()
@runtime_checkable
class CalibreMetadataLike(Protocol):
    """
    Describe the metadata attributes and identifier accessor expected from a Calibre-like object.

    Both spelling variants listed in the protocol participate in its structural
    contract, even where consuming code may tolerate one missing alias.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/containers/test_relation_container_contracts.py
    """
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

    def get_identifiers(self) -> Mapping[str, str] | Mapping[str, IdentifierValues]:
        """
        Specify access to identifier schemes and their accepted scalar or multi-value representations.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/containers/test_relation_container_contracts.py


        :return: Mapping of scheme names to strings, sets/sequences, or display-value-to-ID
            mappings; ownership is implementation-defined.
        """
        ...


AgentID = int
WorkID = int
ExpressionID = int
ManifestationID = int
ItemID = int
LanguageID = int


class CreditSource(StrEnum):
    """
    Name the origin of a metadata credit: user-set, imported, derived, or parsed.

    Example:
        >>> CreditSource.IMPORTED.value
        'imported'
    """
    USER_SET = "user_set"
    IMPORTED = "imported"
    DERIVED = "derived"
    PARSED = "parsed"


class WorkAgentRole(StrEnum):
    """
    Name supported agent roles attached to a work, including authorship, creation, contribution, and subject.

    Example:
        >>> WorkAgentRole.AUTHOR == 'author'
        True
    """
    AUTHOR = "author"
    COMPOSER = "composer"
    ARTIST = "artist"
    DIRECTOR = "director"
    CREATOR = "creator"
    CONTRIBUTOR = "contributor"
    SUBJECT = "subject"


class ExpressionAgentRole(StrEnum):
    """
    Name agent roles attached to an expression, such as translation, adaptation, editing, and performance.

    Example:
        >>> ExpressionAgentRole.TRANSLATOR.value
        'translator'
    """
    TRANSLATOR = "translator"
    ADAPTOR = "adaptor"
    EDITOR = "editor"
    COMMENTATOR = "commentator"
    NARRATOR = "narrator"
    PERFORMER = "performer"
    ILLUSTRATOR = "illustrator"
    CONTRIBUTOR = "contributor"


class ManifestationAgentRole(StrEnum):
    """
    Name agent roles attached to a manifestation’s publishing, manufacture, distribution, or design.

    Example:
        >>> ManifestationAgentRole.PUBLISHER.value
        'publisher'
    """
    PUBLISHER = "publisher"
    IMPRINTER = "imprinter"
    PRINTER = "printer"
    DISTRIBUTOR = "distributor"
    MANUFACTURER = "manufacturer"
    EDITORIAL_DIRECTOR = "editorial_director"
    DESIGNER = "designer"


class ItemAgentRole(StrEnum):
    """
    Name agent roles attached to an individual item’s ownership, annotation, binding, sale, or care.

    Example:
        >>> ItemAgentRole.OWNER.value
        'owner'
    """
    OWNER = "owner"
    DONOR = "donor"
    INSCRIBER = "inscriber"
    ANNOTATOR = "annotator"
    BINDER = "binder"
    BOOKSELLER = "bookseller"
    RESTORER = "restorer"
    CUSTODIAN = "custodian"
