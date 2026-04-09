
"""
Methods to generated hashes and gists from the database.

Used to detected if metadata sets for an object has changed.
(Conceptually a good idea - not sure how practical).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from copy import deepcopy

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI
    from LiuXin_alpha.databases.api.row import RowAPI


# Todo: There has to be better ways to do this
def _row_value(row: Any, key: str, default: Optional[Any] = None) -> Any:
    """
    Agnostic value getter from a row.

    :param row:
    :param key:
    :param default:
    :return:
    """
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        if key in row:
            return row[key]
        return default
    except Exception:
        return default


def generate_book_fingerprint(db: "DatabaseAPI", book_row: "RowAPI") -> set[str]:
    """
    The union of all the things the book is linked to - with all the things the title is linked to.

    :param db:
    :param book_row:
    :return:
    """
    title_id = _row_value(book_row, "book_title", None)
    if title_id is None:

        # In FRBR-era schemas, books can be keyed directly by book_id/title_id.
        title_id = _row_value(book_row, "book_id", None)

    title_row = db.get_row_from_id("titles", title_id) if title_id is not None else None
    if title_row is not None:
        fingerprint = generate_title_fingerprint(db=db, title_row=title_row)
    else:
        fingerprint = set()

    # Include all other main tables in the fingerprint
    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("books")
    main_tables.discard("titles")
    for table in main_tables:
        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("books", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=book_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fingerprint.add(base_print.format(row.row_id))

    return fingerprint


def generate_title_fingerprint(db: "DatabaseAPI", title_row: "RowAPI") -> set[str]:
    """
    Generates a fingerprint for the given title_row.

    :param db: The database in which to work
    :param title_row: The books title in the titles table
    :return:
    """
    fingerprint = set()

    # generate the title fingerprint and add it
    fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=title_row))

    # Only query title intralinks when the schema exposes that relation.
    if db.driver_wrapper.check_for_intralink_table("titles"):
        # Match the title as primary row
        try:
            for p_title_row in db.get_intralinked_rows(primary_row=title_row, secondary_row=None):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=p_title_row))
        except Exception:
            pass

        # Match the title as secondary rows
        try:
            for s_title_row in db.get_intralinked_rows(primary_row=None, secondary_row=title_row):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=s_title_row))
        except Exception:
            pass

    return fingerprint


def generate_one_title_fingerprint(db: "DatabaseAPI", title_row: "RowAPI") -> set[str]:
    """
    Generates a fingerprint based off a single title.

    :param db:
    :param title_row:
    :return:
    """
    fp = set()

    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("titles")

    for table in main_tables:
        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("titles", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=title_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fp.add(base_print.format(row.row_id))

    return fp
