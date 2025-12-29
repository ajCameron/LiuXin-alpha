# Verifies that the database is fine - checks for things which might cause problems

from copy import deepcopy


# Todo: Implement as a check before the driver is allowed to create a new main table
# Todo: Will currently fail, due to custom columns having bad column names
def check_for_duplicate_column_names(tables_and_columns, additional_column_names=None):
    """
    Takes an iterable of additional columns names (or a string). Checks through the database to make sure that none of
    the given names conflict with any names currently present in the database.
    :param tables_and_columns: A map keyed with the name of the table and valued with all the names of the columns in
                               that table.
    :param additional_column_names: An iterable containing additional column names to exclude
    :return:
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
