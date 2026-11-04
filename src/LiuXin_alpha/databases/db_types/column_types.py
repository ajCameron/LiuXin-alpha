
"""Describe recursively structured column options without binding them to tables.

Option values contain scalar values, tuples or nested string-keyed mappings.
The aliases describe typing only; serialization and validation belong to their
consumers in column_metadata and the shared value-policy modules.
"""

from typing import Mapping


type ColumnOptionValue = (
    str
    | int
    | float
    | bool
    | None
    | tuple[ColumnOptionValue, ...]
    | Mapping[str, ColumnOptionValue]
)
type ColumnOptions = Mapping[str, ColumnOptionValue]
