"""
Check column-name collisions across a supplied schema-name mapping.

The helper reports collisions rather than validating SQL, table existence or types.
"""

# Verifies that the database is fine - checks for things which might cause problems

from collections.abc import Iterable, Mapping


# Todo: Implement as a check before the driver is allowed to create a new main table
# Todo: Will currently fail, due to custom columns having bad column names
def check_for_duplicate_column_names(
    tables_and_columns: Mapping[str, Iterable[str]],
    additional_column_names: Iterable[str] | None = None,
) -> bool:
    """
    Report whether a schema column repeats or collides with additional reserved names.

    Each table value is iterated directly (dictionary values therefore contribute keys).
    Additional iterables are deduplicated first; a string contributes characters, and
    None seeds a None sentinel. Repeats solely within the additional input are not
    reported.

    Example:
        >>> check_for_duplicate_column_names({"a": ["id"], "b": ["id"]})
        True


    :param tables_and_columns: Table-to-iterable-of-column-names mapping.
    :param additional_column_names: Optional reserved names; the iterable is consumed once.
    :return: True on the first collision; False when all schema column names are new.
    """
    # Iterates over every column name in the database, adding them to a set and checking that the set's size increases
    # by exactly one each time
    rows_set: set[str | None] = (
        set(additional_column_names) if additional_column_names is not None else {None}
    )

    for table in tables_and_columns:
        current_columns = tables_and_columns[table]
        for column in current_columns:
            if column not in rows_set:
                rows_set.add(column)
            else:
                return True
    return False
