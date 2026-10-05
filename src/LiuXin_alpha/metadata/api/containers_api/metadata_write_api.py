"""
Describe shared target, database, and result shapes for metadata persistence.

Recursive value aliases support scalar, list, tuple, and string-keyed mapping
records. Protocols specify interfaces only; the concrete WEMI writer implements
persistence and report construction.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_metadata_write_api.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, TypeAlias, TypedDict, runtime_checkable


MetadataWriteScalar: TypeAlias = str | int | float | bool | None
MetadataWriteValue: TypeAlias = (
    MetadataWriteScalar
    | list["MetadataWriteValue"]
    | tuple["MetadataWriteValue", ...]
    | Mapping[str, "MetadataWriteValue"]
)
MetadataWriteRecord: TypeAlias = Mapping[str, MetadataWriteValue]


class MetadataWriteReportMapping(TypedDict):
    """
    Describe the complete mapping form of a metadata write report.

    All declared keys are required by this TypedDict: target identity, checked fields,
    row/link changes, skips, errors, and the changed flag. Runtime dictionaries are not
    validated by the type declaration.

    Example:
        >>> 'changed' in MetadataWriteReportMapping.__required_keys__
        True
    """

    item_id: int | None
    target_level: str
    target_table: str | None
    target_id: int | None
    fields_checked: list[str]
    rows_added: list[MetadataWriteRecord]
    rows_updated: list[MetadataWriteRecord]
    rows_removed: list[MetadataWriteRecord]
    links_added: list[MetadataWriteRecord]
    links_removed: list[MetadataWriteRecord]
    skipped: list[str]
    errors: list[str]
    changed: bool


class MetadataWriteTargetRowAPI(Protocol):
    """
    Describe a row-like write target exposing its values as a mapping.

    Writers may also accept a plain MetadataWriteRecord via the target-row union; this
    protocol supplies no lookup or persistence implementation.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/api/test_metadata_write_api.py
    """

    @property
    def row_dict(self) -> MetadataWriteRecord:
        """
        Expose the row values used to identify a metadata write target.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_metadata_write_api.py


        :return: Record mapping supplied by the row implementation; copying is not required
            by this contract.
        """


MetadataWriteTargetRow: TypeAlias = MetadataWriteRecord | MetadataWriteTargetRowAPI


class MetadataWriteDatabaseAPI(Protocol):
    """
    Mark a database object accepted by metadata write adapters.

    This protocol currently declares no members. It does not validate backend
    capabilities, transactions, or ownership; the concrete writer determines the
    operations it needs.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/api/test_metadata_write_api.py
    """


@runtime_checkable
class MetadataWriteReportAPI(Protocol):
    """
    Describe a write result with target identity, mutations, skips, and errors.

    The protocol is runtime-checkable for member presence, without validating nested
    value types. Implementations compute changed and produce the public report mapping.

    Example:
        >>> from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadataWriteReport
        >>> report = LiuXinWEMIMetadataWriteReport(item_id=7, target_level='work')
        >>> isinstance(report, MetadataWriteReportAPI)
        True
    """

    item_id: int | None
    target_level: str
    target_table: str | None
    target_id: int | None
    fields_checked: list[str]
    rows_added: list[MetadataWriteRecord]
    rows_updated: list[MetadataWriteRecord]
    rows_removed: list[MetadataWriteRecord]
    links_added: list[MetadataWriteRecord]
    links_removed: list[MetadataWriteRecord]
    skipped: list[str]
    errors: list[str]

    @property
    def changed(self) -> bool:
        """
        Indicate whether the report contains row or link mutations.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_metadata_write_api.py


        :return: True when a recorded write changed rows or links; checks/skips alone do not
            count.
        """

    def to_mapping(self) -> MetadataWriteReportMapping:
        """
        Export the report fields in the public dictionary shape.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_metadata_write_api.py


        :return: MetadataWriteReportMapping containing target, changes, skips, errors, and
            changed.
        """


__all__ = [
    "MetadataWriteDatabaseAPI",
    "MetadataWriteRecord",
    "MetadataWriteReportMapping",
    "MetadataWriteReportAPI",
    "MetadataWriteScalar",
    "MetadataWriteTargetRow",
    "MetadataWriteTargetRowAPI",
    "MetadataWriteValue",
]
