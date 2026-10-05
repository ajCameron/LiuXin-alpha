"""
Check column-name collisions across a supplied schema-name mapping.

The helper reports collisions rather than validating SQL, table existence or types.
"""

# Verifies that the database is fine - checks for things which might cause problems

from copy import deepcopy

from typing import Optional


# Todo: Implement as a check before the driver is allowed to create a new main table
# Todo: Will currently fail, due to custom columns having bad column names
def check_for_duplicate_column_names(
        tables_and_columns: dict[str, dict[str, list[str]]],
        additional_column_names: Optional[list[str]] = None) -> bool:
    """
    Report whether a schema column repeats or collides with additional reserved names.

    Each table value is iterated directly (dictionary values therefore contribute keys).
    Additional iterables are deduplicated first; a string contributes characters, and
    None seeds a None sentinel. Repeats solely within the additional input are not reported.

    Example:
        >>> check_for_duplicate_column_names({"a": ["id"], "b": ["id"]})
        True


    :param tables_and_columns: Table-to-iterable-of-column-names mapping, despite the narrower annotation.
    :param additional_column_names: Optional reserved names or scalar seed; copied before use.
    :return: True on the first collision; False when all schema column names are new.
    """
    additional_column_names = deepcopy(additional_column_names)

    # Iterates over every column name in the database, adding them to a set and checking that the set's size increases
    # by exactly one each time
    rows_set = set()
    if hasattr(additional_column_names, "__iter__"):
        additional_column_names = set([name for name in additional_column_names])
        rows_set = rows_set.union(additional_column_names)
    else:
        rows_set.add(additional_column_names)

    for table in tables_and_columns:
        current_columns = tables_and_columns[table]
        for column in current_columns:
            if column not in rows_set:
                rows_set.add(column)
            else:
                return True
    return False
