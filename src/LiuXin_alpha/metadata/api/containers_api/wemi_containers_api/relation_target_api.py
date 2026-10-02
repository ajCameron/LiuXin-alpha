"""
Define recursive metadata payload types and structural relation targets.

Targets may be scalar values, mappings, serializable identities or row-like objects.
Id extraction follows an explicit field/attribute fallback and does not serialize
objects.

Example:
    >>> relation_target_id({'work_id': '3'}, 'work_id')
    3
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, runtime_checkable

MetadataScalar: TypeAlias = str | int | float | bool | None
MetadataValue: TypeAlias = (
    MetadataScalar
    | list["MetadataValue"]
    | tuple["MetadataValue", ...]
    | Mapping[str, "MetadataValue"]
)
MetadataRecord: TypeAlias = Mapping[str, MetadataValue]
MutableMetadataRecord: TypeAlias = dict[str, MetadataValue]
RelationLinkType: TypeAlias = str


@runtime_checkable
class SupportsMetadataMapping(Protocol):
    """
    Describe objects that can serialize themselves to metadata records.

    The runtime-checkable protocol establishes member presence without validating
    payload contents or copying behavior.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
        >>> work = WorkIdentity(work_id=3)
        >>> isinstance(work, SupportsMetadataMapping)
        True
    """

    def to_mapping(self) -> MetadataRecord:
        """
        Require a metadata-record representation of this object.

        Field shape and whether values are copied or shared follow the implementation.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import WorkIdentity
            >>> work = WorkIdentity(work_id=3)
            >>> work.to_mapping()['work_id']
            3


        :return: Mapping of string keys to metadata values.
        """


@runtime_checkable
class SupportsRowMapping(Protocol):
    """
    Describe the table, id and record surface of a database row.

    Implementations control record ownership and database access. Runtime structural
    checks do not validate the declared types.

    Example:
        >>> from types import SimpleNamespace
        >>> row = SimpleNamespace(table='works', row_id=3, row_dict={'work_id': 3})
        >>> isinstance(row, SupportsRowMapping)
        True
    """

    @property
    def table(self) -> str:
        """
        Require the source table name of this row-like object.

        Example:
            >>> from types import SimpleNamespace
            >>> row = SimpleNamespace(table='works', row_id=3, row_dict={'work_id': 3})
            >>> row.table
            'works'


        :return: Source table name.
        """

    @property
    def row_id(self) -> int | None:
        """
        Require the row id when one is available.

        Example:
            >>> from types import SimpleNamespace
            >>> row = SimpleNamespace(table='works', row_id=3, row_dict={'work_id': 3})
            >>> row.row_id
            3


        :return: Integer row id, or None.
        """

    @property
    def row_dict(self) -> MetadataRecord:
        """
        Require the row payload as a metadata record.

        The implementation determines whether this mapping is live or copied.

        Example:
            >>> from types import SimpleNamespace
            >>> row = SimpleNamespace(table='works', row_id=3, row_dict={'work_id': 3})
            >>> row.row_dict['work_id']
            3


        :return: Mapping of row columns to metadata values.
        """


RelationTarget: TypeAlias = (
    MetadataScalar
    | MetadataRecord
    | SupportsMetadataMapping
    | SupportsRowMapping
)


def relation_target_id(target: RelationTarget | None, id_column: str) -> int | None:
    """
    Extract the first present id candidate and attempt integer conversion.

    Mappings try id_column, id, then row_id. Other objects try the named attribute, that
    column in row_dict, row_id, then id; to_mapping is never called. None and empty text
    are absent, but zero and False are present. A chosen invalid candidate returns None
    for TypeError, ValueError or OverflowError without trying later fallbacks. Attribute
    access and other errors may propagate.

    Example:
        >>> relation_target_id({'work_id': '', 'id': '3'}, 'work_id')
        3
        >>> relation_target_id({'work_id': 'bad', 'id': '3'}, 'work_id') is None
        True
        >>> relation_target_id({'work_id': 0, 'id': 3}, 'work_id')
        0


    :param target: Mapping, row-like object, identity or other possible relation target.
    :param id_column: Preferred canonical id key or attribute name.
    :return: Converted integer id, or None for no usable candidate.
    """

    value = None
    if isinstance(target, Mapping):
        value = _first_present_mapping_value(target, id_column, "id", "row_id")
    else:
        value = getattr(target, id_column, None)
        row_dict = getattr(target, "row_dict", None)
        if value in (None, "") and isinstance(row_dict, Mapping):
            value = _first_present_mapping_value(row_dict, id_column)
        if value in (None, ""):
            value = getattr(target, "row_id", None)
        if value in (None, ""):
            value = getattr(target, "id", None)

    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _first_present_mapping_value(
    mapping: Mapping[str, MetadataValue],
    *keys: str,
) -> MetadataValue:
    """
    Return the first requested value that is neither None nor empty text.

    Zero, False and other falsey values remain present. No conversion or validation
    occurs.

    Example:
        >>> _first_present_mapping_value({'a': '', 'b': 0, 'c': 3}, 'a', 'b', 'c')
        0


    :param mapping: Metadata mapping read through get.
    :param keys: Keys to inspect in order.
    :return: First present value, or None if every requested key is absent.
    """
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


__all__ = [
    "MetadataScalar",
    "MetadataValue",
    "MetadataRecord",
    "MutableMetadataRecord",
    "relation_target_id",
    "RelationLinkType",
    "RelationTarget",
    "SupportsMetadataMapping",
    "SupportsRowMapping",
]
