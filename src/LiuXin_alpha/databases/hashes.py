
"""
Build legacy relationship fingerprints from linked table names and row IDs.

Fingerprints are sets of table_id strings, not content hashes. They ignore row text, link ordering and link properties, and skip many unavailable relation queries. Equal fingerprints therefore do not establish metadata equality. The helpers retain legacy books/titles naming for compatibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from copy import deepcopy

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


# Todo: There has to be better ways to do this
def _row_value(row: Any, key: str, default: Optional[Any] = None) -> Any:
    """
    Read a mapping-like row value with a fallback for unsupported access.

    Dictionaries use get directly. Other rows are probed with membership before subscription; any exception in that probe is swallowed. A dict subclass whose get raises is not covered by that handler.

    Example:
        >>> _row_value({"book_id": 7}, "book_id")
        7
        >>> _row_value(object(), "book_id", "missing")
        'missing'


    :param row: Dictionary or row supporting membership and item lookup.
    :param key: Column name to look up.
    :param default: Fallback when the column is absent or non-dict access fails.
    :return: Stored value, including None, or default.
    """
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        if key in row:
            return row[key]
        return default
    except Exception:
        return default


# Todo: In general, these are not relevant anymore - as we're working on WEMI principles.
def generate_book_fingerprint(db: "DatabaseAPI", book_row: "RowAPI") -> set[str]:
    """
    Union a book’s legacy title fingerprint with its other direct links.

    Resolve the title in titles using book_title, falling back to book_id only when book_title is None. If no title resolves, start empty. Then inspect book links to main tables except books and titles. Missing link tables and relation-query exceptions are skipped. Title lookup, row-ID formatting and errors during lazy linked-row iteration can still propagate. Neither the book ID nor its text is included by itself.

    Example:
        fingerprint = generate_book_fingerprint(db, book_row)
        related_ids = sorted(fingerprint)


    :param db: Database exposing main_tables, driver_wrapper relation discovery and row/link queries.
    :param book_row: Book row with book_title or, when that value is None, a book_id fallback.
    :return: Set of table_id strings from the resolved title group and direct book relations.
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
    Union direct-link fingerprints for a title and its immediate intralink neighbours.

    Include the supplied title first. If the schema exposes title intralinks, inspect both outgoing and incoming neighbours, without recursively walking further titles. Each directional neighbour loop suppresses exceptions, retaining contributions added before failure. The initial title fingerprint and intralink-table check can still fail. A neighbour title ID itself is not automatically included.

    Example:
        fingerprint = generate_title_fingerprint(db, title_row)
        unchanged_links = fingerprint == previous_fingerprint


    :param db: Database exposing title relation discovery and link queries.
    :param title_row: Title row whose direct relations and immediate neighbours are inspected.
    :return: Set of table_id strings accumulated across the title and its immediate neighbours.
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
    Collect IDs linked directly to one title across the other main tables.

    Exclude titles, but include books when that relation exists. Skip missing link tables and exceptions while discovering or requesting each relation. Iteration and row_id access occur outside that handler. The title’s own ID, metadata values, priorities and link types are not fingerprint components.

    Example:
        fingerprint = generate_one_title_fingerprint(db, title_row)
        linked_agents = sorted(value for value in fingerprint if value.startswith("agents_"))


    :param db: Database whose main_tables and driver_wrapper describe available relations.
    :param title_row: Title row passed as the primary row to get_interlinked_rows.
    :return: Set of table_id strings, deduplicated across retrieved linked rows.
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
