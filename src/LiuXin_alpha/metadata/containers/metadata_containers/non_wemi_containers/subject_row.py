"""
Provide the concrete subjects row value used by metadata callers.

The SubjectRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = SubjectRow(subject='Computer science')
    >>> row.subject
    'Computer science'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SubjectRow(MetadataTableRow):
    """
    Store a subject heading with optional parent, position and string tree id.

    Subject text, sort/hash and full hierarchy fields are independent supplied values.
    Use SubjectTreeRelation for explicit local relation validation.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = SubjectRow.from_mapping({'subject_id': 7, 'subject': 'Computer science'})
        >>> row.primary_id, row.to_mapping()['subject']
        (7, 'Computer science')
    """
    TABLE_NAME: ClassVar[str] = "subjects"
    ID_COLUMN: ClassVar[str] = "subject_id"

    subject_id: int | None = None
    subject: str | None = None
    subject_phash: str | None = None
    subject_sort: str | None = None
    subject_parent_id: int | None = None
    subject_parent_position: int | None = None
    subject_tree_id: str | None = None
    subject_full: str | None = None
    subject_created_timestamp_ep_k: int | None = None
    subject_modified_timestamp_ep_k: int | None = None
    subject_source_created_datestamp_ep_k: int | None = None
    subject_source_modified_datestamp_ep_k: int | None = None
    subject_scratch: str | None = None


__all__ = ["SubjectRow"]
