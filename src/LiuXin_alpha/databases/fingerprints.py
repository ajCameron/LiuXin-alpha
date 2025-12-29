# Methods to cope with fingerprint assets (files and folders).
# Bit of a mess, frankly.

from copy import deepcopy


def generate_book_fingerprint(db, book_row):
    """
    The union of all the things the book is linked to - with all the things the title is linked to.
    :param db:
    :param book_row:
    :return:
    """
    book_title = book_row["book_title"]

    fingerprint = generate_title_fingerprint(db=db, title_row=book_title)

    # Include all other main tables in the fingerprint
    main_tables = deepcopy(db.main_tables)
    main_tables.remove("books")
    main_tables.remove("titles")
    for table in main_tables:

        base_print = deepcopy(table) + "_{}"
        linked_rows = db.get_interlinked_rows(target_row=book_row, secondary_table=table)
        for row in linked_rows:
            fingerprint.add(base_print.format(row.row_id))

    return fingerprint


def generate_title_fingerprint(db, title_row):
    """
    Generates a fingerprint for the given title_row.
    :param db: The database in which to work
    :param title_row: The books title in the titles table
    :return:
    """
    fingerprint = set()

    # generate the title fingerprint and add it
    fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=title_row))

    # Match the title as primary row
    for p_title_row in db.get_intralinked_rows(primary_row=title_row, secondary_row=None):
        fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=p_title_row))

    # Match the title as secondary rows
    for s_title_row in db.get_intralinked_rows(primary_row=None, secondary_row=title_row):
        fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=s_title_row))

    return fingerprint


def generate_one_title_fingerprint(db, title_row):
    """
    Generates a fingerprint based off a single title.
    :param db:
    :param title_row:
    :return:
    """
    fp = set()

    main_tables = deepcopy(db.main_tables)
    main_tables.remove("titles")

    for table in main_tables:

        base_print = deepcopy(table) + "_{}"
        linked_rows = db.get_interlinked_rows(target_row=title_row, secondary_table=table)
        for row in linked_rows:
            fp.add(base_print.format(row.row_id))

    return fp
